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
