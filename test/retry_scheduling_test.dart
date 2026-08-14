import 'package:api_command_queue/api_command_queue.dart';
import 'package:clock/clock.dart';
import 'package:fake_async/fake_async.dart';
import 'package:test/test.dart';

import 'test_utils.dart';

final class TestQueue extends ApiCommandQueue<DummyData,
    ApiCommandRequest<DummyData>, DummyData, DummyCommand> {
  TestQueue({required super.retryPolicy})
      : super(
          commandFromJson: DummyCommand.fromJson,
          concurrencyLimit: 1,
        );
}

const _policy = ExponentialBackoffRetryPolicy(
  maxAttempts: 5,
  initialDelay: Duration(seconds: 1),
  backoffFactor: 2.0,
  maxDelay: Duration(seconds: 30),
  maxAge: Duration(hours: 1),
);

TestQueue _queue() => TestQueue(retryPolicy: _policy);

int _attempts(TestQueue queue, String id) =>
    queue.state.pending[id]?.attemptCount ?? -1;

/// Runs a flush and settles everything that does not need time to pass.
///
/// Returns whether it finished without the clock moving - which is the point of
/// the whole exercise. A queue that sleeps its backoff inline cannot.
bool _flushSettles(FakeAsync async, TestQueue queue) {
  var done = false;
  queue.flush().then((_) => done = true);
  async.flushMicrotasks();
  return done;
}

/// Backoff is a delay applied *between* flushes, not something a flush waits
/// out. Sleeping it inline meant one failing command held its queue for the
/// length of its whole retry ladder - minutes - and, where queues flush in
/// sequence, held every queue behind it too.
void main() {
  test('a flush does not wait out a failing command\'s backoff', () {
    fakeAsync((async) {
      final queue = _queue();
      queue.addCommand(
        DummyCommand.createPending(id: 'a', value: 1, willSucceed: false),
      );

      expect(
        _flushSettles(async, queue),
        isTrue,
        reason: 'the flush should return once the attempt fails, not sleep on the retry',
      );
      expect(_attempts(queue, 'a'), 1);
    });
  });

  test('a command is not retried before its backoff has elapsed', () {
    fakeAsync((async) {
      final queue = _queue();
      queue.addCommand(
        DummyCommand.createPending(id: 'a', value: 1, willSucceed: false),
      );

      _flushSettles(async, queue);
      expect(_attempts(queue, 'a'), 1);

      /// flushing again straight away must not burn an attempt
      _flushSettles(async, queue);
      expect(_attempts(queue, 'a'), 1);

      async.elapse(const Duration(milliseconds: 999));
      _flushSettles(async, queue);
      expect(_attempts(queue, 'a'), 1, reason: 'still inside the first second');

      async.elapse(const Duration(milliseconds: 1));
      _flushSettles(async, queue);
      expect(_attempts(queue, 'a'), 2, reason: 'the first second has passed');
    });
  });

  test('the delay between attempts grows with the policy', () {
    fakeAsync((async) {
      final queue = _queue();
      queue.addCommand(
        DummyCommand.createPending(id: 'a', value: 1, willSucceed: false),
      );

      _flushSettles(async, queue);
      expect(_attempts(queue, 'a'), 1);

      /// 1s, then 2s, then 4s
      for (final wait in const [1, 2, 4]) {
        async.elapse(Duration(seconds: wait - 1));
        _flushSettles(async, queue);
        final before = _attempts(queue, 'a');

        async.elapse(const Duration(seconds: 1));
        _flushSettles(async, queue);

        expect(
          _attempts(queue, 'a'),
          before + 1,
          reason: 'expected one more attempt after ${wait}s',
        );
      }
    });
  });

  test('a command restored with its backoff already elapsed runs immediately', () {
    /// the cross-session case: a command that failed four times, persisted, and
    /// came back after the app restarted. The wait it was owed passed while the
    /// app was closed, so it should not be made to sit through it again
    fakeAsync((async) {
      final queue = _queue();
      final stale = DummyCommand.createPending(id: 'a', value: 1, willSucceed: false).copyWith(
        attemptCount: 2,
        lastUpdated: clock.now().subtract(const Duration(hours: 1)),
      );

      queue.addCommand(stale);

      expect(_flushSettles(async, queue), isTrue);
      expect(
        _attempts(queue, 'a'),
        3,
        reason: 'it was owed a 2s wait that elapsed while the app was closed',
      );
    });
  });

  test('a successful command is never held back', () {
    fakeAsync((async) {
      final queue = _queue();
      queue.addCommand(
        DummyCommand.createPending(id: 'a', value: 1, willSucceed: true),
      );

      expect(_flushSettles(async, queue), isTrue);
      expect(queue.state.pending, isEmpty);
    });
  });

  group('nextDueAt', () {
    test('is null when nothing is queued', () {
      final queue = _queue();

      expect(queue.nextDueAt, isNull);
    });

    test('is now for a command that has never been attempted', () {
      fakeAsync((async) {
        final queue = _queue();
        queue.addCommand(DummyCommand.createPending(id: 'a', value: 1));

        expect(queue.nextDueAt, clock.now());
      });
    });

    test('is when the earliest waiting command comes due', () {
      fakeAsync((async) {
        final queue = _queue();
        queue.addCommand(
          DummyCommand.createPending(id: 'a', value: 1, willSucceed: false),
        );

        final failedAt = clock.now();
        _flushSettles(async, queue);

        expect(
          queue.nextDueAt,
          failedAt.add(const Duration(seconds: 1)),
          reason: 'a caller can schedule its next flush instead of polling',
        );
      });
    });
  });
}
