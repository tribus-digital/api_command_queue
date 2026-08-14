import 'package:api_command_queue/api_command_queue.dart';
import 'package:clock/clock.dart';
import 'package:fake_async/fake_async.dart';
import 'package:test/test.dart';

import 'test_utils.dart';

final class TestQueue extends ApiCommandQueue<DummyData,
    ApiCommandRequest<DummyData>, DummyData, DummyCommand> {
  TestQueue()
      : super(
          commandFromJson: DummyCommand.fromJson,
          concurrencyLimit: 1,
          retryPolicy: const ExponentialBackoffRetryPolicy(
            maxAttempts: 10,
            initialDelay: Duration(seconds: 4),
            backoffFactor: 2.0,
            maxDelay: Duration(seconds: 30),
            maxAge: Duration(hours: 1),
          ),
        );
}

/// Counts flushes so a queue registered under several command types can show
/// whether it was walked more than once.
final class CountingQueue extends ApiCommandQueue<DummyData,
    ApiCommandRequest<DummyData>, DummyData, DummyCommand> {
  CountingQueue() : super(commandFromJson: DummyCommand.fromJson);

  int flushCount = 0;

  @override
  Future<void> flush() {
    flushCount += 1;
    return super.flush();
  }
}

int _attempts(TestQueue queue, String id) =>
    queue.state.pending[id]?.attemptCount ?? -1;

/// Backoff is scheduled rather than slept, so a failed command sits until
/// something triggers the next flush. Left to a consumer that only flushes on
/// connectivity changes and user activity, a device sitting idle never retries
/// at all.
void main() {
  test('a failed command is retried without anything else triggering a flush', () {
    fakeAsync((async) {
      final queue = TestQueue();
      final orchestrator = ApiCommandOrchestrator(
        commandQueues: {DummyCommand: queue},
        autoFlushWhenDue: true,
      );

      orchestrator.enqueue(
        DummyCommand.createPending(id: 'a', value: 1, willSucceed: false),
      );

      async.flushMicrotasks();
      expect(_attempts(queue, 'a'), 1, reason: 'enqueue processes it once');

      /// nothing else happens - no connectivity change, nobody touching the app
      async.elapse(const Duration(seconds: 5));
      expect(_attempts(queue, 'a'), 2);

      async.elapse(const Duration(seconds: 9));
      expect(_attempts(queue, 'a'), 3);

      orchestrator.close();
    });
  });

  test('it stops once nothing is queued', () {
    fakeAsync((async) {
      final queue = TestQueue();
      final orchestrator = ApiCommandOrchestrator(
        commandQueues: {DummyCommand: queue},
        autoFlushWhenDue: true,
      );

      orchestrator.enqueue(DummyCommand.createPending(id: 'a', value: 1));
      async.flushMicrotasks();

      expect(queue.state.pending, isEmpty);
      expect(orchestrator.nextDueAt, isNull);

      /// a timer left running here would flush an empty queue forever
      async.elapse(const Duration(minutes: 5));

      orchestrator.close();
      expect(async.pendingTimers, isEmpty);
    });
  });

  test('it is off unless asked for', () {
    fakeAsync((async) {
      final queue = TestQueue();
      final orchestrator = ApiCommandOrchestrator(
        commandQueues: {DummyCommand: queue},
      );

      orchestrator.enqueue(
        DummyCommand.createPending(id: 'a', value: 1, willSucceed: false),
      );
      async.flushMicrotasks();
      expect(_attempts(queue, 'a'), 1);

      async.elapse(const Duration(minutes: 5));

      expect(
        _attempts(queue, 'a'),
        1,
        reason: 'consumers driving their own flushes should not get a second thing doing it',
      );

      orchestrator.close();
    });
  });

  test('a paused orchestrator does not flush itself', () {
    fakeAsync((async) {
      final queue = TestQueue();
      final orchestrator = ApiCommandOrchestrator(
        commandQueues: {DummyCommand: queue},
        autoFlushWhenDue: true,
      );

      orchestrator.enqueue(
        DummyCommand.createPending(id: 'a', value: 1, willSucceed: false),
      );
      async.flushMicrotasks();

      orchestrator.setProcessingEnabled(false);
      async.elapse(const Duration(minutes: 5));

      expect(
        _attempts(queue, 'a'),
        1,
        reason: 'offline or signed out, retrying is pointless and would spin',
      );

      orchestrator.close();
    });
  });

  test('closing cancels the timer', () {
    fakeAsync((async) {
      final queue = TestQueue();
      final orchestrator = ApiCommandOrchestrator(
        commandQueues: {DummyCommand: queue},
        autoFlushWhenDue: true,
      );

      orchestrator.enqueue(
        DummyCommand.createPending(id: 'a', value: 1, willSucceed: false),
      );
      async.flushMicrotasks();

      orchestrator.close();
      async.elapse(const Duration(minutes: 1));

      expect(async.pendingTimers, isEmpty);
    });
  });

  /// Found on a device: a write in flight for three seconds had the timer
  /// firing on its one second floor the whole time, each firing walking every
  /// queue to discover there was nothing it could do.
  test('a command being executed is not reported as due', () {
    fakeAsync((async) {
      final queue = TestQueue();
      final orchestrator = ApiCommandOrchestrator(
        commandQueues: {DummyCommand: queue},
        autoFlushWhenDue: true,
      );

      orchestrator.enqueue(
        DummyCommand.createPending(
          id: 'a',
          value: 1,
          executeDelay: const Duration(seconds: 3),
        ),
      );
      async.flushMicrotasks();

      expect(queue.inFlightCount, 1);
      expect(
        orchestrator.nextDueAt,
        isNull,
        reason: 'it is being sent right now - there is nothing to schedule for',
      );

      async.elapse(const Duration(seconds: 3));
      async.flushMicrotasks();

      expect(queue.state.pending, isEmpty);
      expect(orchestrator.nextDueAt, isNull);

      orchestrator.close();
    });
  });

  test('each queue is flushed once however many command types it takes', () {
    fakeAsync((async) {
      final queue = CountingQueue();
      final orchestrator = ApiCommandOrchestrator(
        /// the usual shape: a create and a patch sharing one queue
        commandQueues: {DummyCommand: queue, DummyCommand2: queue},
      );

      orchestrator.flushAll();
      async.flushMicrotasks();

      expect(queue.flushCount, 1);

      orchestrator.close();
    });
  });

  /// Found on a device: a startup flush and a sync flush overlapped, and every
  /// queue past the point they met was walked twice.
  group('overlapping flushes', () {
    test('a second call joins the running flush rather than walking again', () async {
      final queue = CountingQueue();
      final orchestrator = ApiCommandOrchestrator(
        commandQueues: {DummyCommand: queue},
      );

      queue.addCommand(
        DummyCommand.createPending(
          id: 'a',
          value: 1,
          executeDelay: const Duration(milliseconds: 50),
        ),
      );

      await Future.wait([orchestrator.flushAll(), orchestrator.flushAll()]);

      expect(
        queue.flushCount,
        lessThanOrEqualTo(2),
        reason: 'two callers should not mean two full walks per queue',
      );

      await orchestrator.close();
    });

    test('joining a flush with nothing new does not cause a second walk', () async {
      final queue = CountingQueue();
      final orchestrator = ApiCommandOrchestrator(
        commandQueues: {DummyCommand: queue},
      );

      queue.addCommand(
        DummyCommand.createPending(
          id: 'a',
          value: 1,
          executeDelay: const Duration(milliseconds: 50),
        ),
      );

      final first = orchestrator.flushAll();
      await Future<void>.delayed(const Duration(milliseconds: 10));
      final second = orchestrator.flushAll();

      await Future.wait([first, second]);

      expect(
        queue.flushCount,
        1,
        reason: 'the joiner had nothing new, so re-walking every queue is noise',
      );

      await orchestrator.close();
    });

    test('reports itself busy for the whole walk, not just while sending', () async {
      final queue = CountingQueue();
      final orchestrator = ApiCommandOrchestrator(
        commandQueues: {DummyCommand: queue},
      );

      final seen = <QueueFlushStatus>[];
      orchestrator.stream.listen(seen.add);

      queue.addCommand(
        DummyCommand.createPending(
          id: 'a',
          value: 1,
          executeDelay: const Duration(milliseconds: 20),
        ),
      );

      await orchestrator.flushAll();

      /// the controller is broadcast, so the closing emission reaches listeners
      /// a microtask after the flush returns
      await Future<void>.delayed(Duration.zero);

      expect(
        seen.where((status) => status == QueueFlushStatus.idle).length,
        1,
        reason: 'a lull between commands is not the end of the flush, got $seen',
      );

      await orchestrator.close();
    });

    test('work queued during a flush still gets a pass', () async {
      final queue = CountingQueue();
      final orchestrator = ApiCommandOrchestrator(
        commandQueues: {DummyCommand: queue},
        processingEnabled: false,
      );

      queue.addCommand(
        DummyCommand.createPending(
          id: 'a',
          value: 1,
          executeDelay: const Duration(milliseconds: 50),
        ),
      );
      orchestrator.resumeAll();

      final first = orchestrator.flushAll();

      /// arrives after the walk has started
      await Future<void>.delayed(const Duration(milliseconds: 10));
      queue.addCommand(DummyCommand.createPending(id: 'b', value: 2));
      final second = orchestrator.flushAll();

      await Future.wait([first, second]);

      expect(
        queue.state.pending,
        isEmpty,
        reason: 'joining a running flush must not mean the later work is skipped',
      );

      await orchestrator.close();
    });
  });

  test('the aggregate state is only announced when it changes', () {
    /// queues emit on every command they touch and most leave the aggregate
    /// where it was, so a listener was being woken dozens of times per flush
    fakeAsync((async) {
      final queue = TestQueue();
      final orchestrator = ApiCommandOrchestrator(
        commandQueues: {DummyCommand: queue, DummyCommand2: queue},
        autoFlushWhenDue: true,
      );

      final seen = <QueueFlushStatus>[];
      orchestrator.stream.listen(seen.add);

      for (var index = 0; index < 5; index += 1) {
        orchestrator.enqueue(DummyCommand.createPending(id: '$index', value: index));
      }
      async.elapse(const Duration(seconds: 1));

      expect(
        seen,
        everyElement(isA<QueueFlushStatus>()),
        reason: 'sanity - the stream is wired up',
      );
      expect(
        seen.length,
        lessThanOrEqualTo(4),
        reason: 'five commands should not mean dozens of identical emissions, got $seen',
      );

      orchestrator.close();
    });
  });

  test('the timer never fires faster than the floor', () {
    /// a due command that a flush cannot clear would otherwise reschedule
    /// instantly and spin
    fakeAsync((async) {
      final queue = TestQueue();
      final orchestrator = ApiCommandOrchestrator(
        commandQueues: {DummyCommand: queue},
        autoFlushWhenDue: true,
      );

      final overdue = DummyCommand.createPending(
        id: 'a',
        value: 1,
        willSucceed: false,
      ).copyWith(
        attemptCount: 1,
        lastUpdated: clock.now().subtract(const Duration(hours: 1)),
      );

      queue.addCommand(overdue);
      orchestrator.enqueue(DummyCommand.createPending(id: 'b', value: 2));
      async.flushMicrotasks();

      final before = _attempts(queue, 'a');
      async.elapse(const Duration(milliseconds: 500));

      expect(_attempts(queue, 'a'), before);

      orchestrator.close();
    });
  });
}
