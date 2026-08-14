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
