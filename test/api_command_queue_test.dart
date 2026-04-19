import 'dart:async';

import 'package:api_command_queue/api_command_queue.dart';
import 'package:fake_async/fake_async.dart';
import 'package:test/test.dart';

import 'test_utils.dart';

final class TestQueue extends ApiCommandQueue<DummyData,
    ApiCommandRequest<DummyData>, DummyData, DummyCommand> {
  TestQueue({
    required super.commandFromJson,
    super.defaultDebounce,
    super.retryPolicy,
    super.concurrencyLimit,
    super.failedCapacity,
  });
}

void main() {
  late TestQueue queue;

  setUp(() {
    queue = TestQueue(
      commandFromJson: DummyCommand.fromJson,
      retryPolicy: const ExponentialBackoffRetryPolicy(
        maxAttempts: 1,
        initialDelay: Duration.zero,
        backoffFactor: 1.0,
        maxDelay: Duration.zero,
        maxAge: Duration(hours: 1),
      ),
    );
  });

  group('the basics', () {
    test('addCommand adds to pending', () {
      final command =
          DummyCommand.createPending(id: 'foo', value: 10, willSucceed: true);

      queue.addCommand(command);

      expect(queue.state.pending.containsKey('foo'), isTrue);
    });

    test('single strategy replaces existing', () {
      final first = DummyCommand.createPending(
        id: 'a',
        value: 1,
        strategy: CommandReplaceStrategy.single,
      );
      final second = DummyCommand.createPending(
        id: 'b',
        value: 2,
        strategy: CommandReplaceStrategy.single,
      );

      queue.addCommand(first);
      expect(queue.state.pending.length, equals(1));

      queue.addCommand(second);

      expect(queue.state.pending.length, equals(1));
      expect(queue.state.pending.containsKey('b'), isTrue);
    });

    test('success removes from pending on flush', () async {
      final command =
          DummyCommand.createPending(id: 'ok', value: 5, willSucceed: true);

      queue.addCommand(command, processNow: true);
      await Future<void>.delayed(Duration.zero);

      expect(queue.state.pending, isEmpty);
    });

    test('failure moves to failed after maxAttempts', () async {
      final command =
          DummyCommand.createPending(id: 'fail', value: 7, willSucceed: false);

      queue.addCommand(command, processNow: true);
      await Future<void>.delayed(const Duration(milliseconds: 50));

      expect(queue.state.pending, isEmpty);
      expect(queue.state.failed.containsKey('fail'), isTrue);
      expect(
        queue.state.failed['fail']!.status,
        equals(ApiCommandStatus.error),
      );
      expect(queue.state.failed['fail']!.attemptCount, equals(1));
    });

    test('toJson and fromJson preserve state', () {
      final command = DummyCommand.createPending(id: 'x', value: 1);
      queue.addCommand(command);

      final restored = queue.fromJson(queue.toJson(queue.state));

      expect(restored.pending.keys, equals(queue.state.pending.keys));
    });

    test('results emits on success and final failure', () async {
      final queue = TestQueue(
        commandFromJson: DummyCommand.fromJson,
        retryPolicy: const ExponentialBackoffRetryPolicy(
          maxAttempts: 1,
          initialDelay: Duration.zero,
          backoffFactor: 1.0,
          maxDelay: Duration.zero,
          maxAge: Duration(hours: 1),
        ),
        concurrencyLimit: 1,
      );

      final events = <ApiCommandResult<DummyCommand, DummyData>>[];
      final subscription = queue.results.listen(events.add);

      queue.addCommand(
          DummyCommand.createPending(id: 'ok', value: 1, willSucceed: true),
          processNow: true);
      queue.addCommand(
          DummyCommand.createPending(id: 'fail', value: 2, willSucceed: false),
          processNow: true);

      await Future<void>.delayed(const Duration(milliseconds: 50));

      expect(events.map((event) => event.command.uuid),
          containsAll(['ok', 'fail']));
      expect(events.firstWhere((event) => event.command.uuid == 'ok').success,
          isTrue);
      expect(events.firstWhere((event) => event.command.uuid == 'fail').success,
          isFalse);
      await subscription.cancel();
    });
  });

  group('addCommand debouncing (single strategy)', () {
    late TestQueue queue;
    const debounceInterval = Duration(milliseconds: 100);

    setUp(() {
      queue = TestQueue(
        commandFromJson: DummyCommand.fromJson,
        retryPolicy: const ExponentialBackoffRetryPolicy(
          maxAttempts: 1,
          initialDelay: Duration.zero,
          backoffFactor: 1.0,
          maxDelay: Duration.zero,
          maxAge: Duration(hours: 1),
        ),
        defaultDebounce: debounceInterval,
      );
    });

    test('coalesces multiple rapid calls into the last command', () {
      fakeAsync((async) {
        final first = DummyCommand.createPending(
          id: 'first',
          value: 1,
          strategy: CommandReplaceStrategy.single,
        );
        final second = DummyCommand.createPending(
          id: 'second',
          value: 2,
          strategy: CommandReplaceStrategy.single,
        );

        queue.addCommand(first);
        expect(queue.state.pending, isEmpty);

        async.elapse(const Duration(milliseconds: 50));
        queue.addCommand(second);

        async.elapse(const Duration(milliseconds: 49));
        expect(queue.state.pending, isEmpty);

        async.elapse(const Duration(milliseconds: 1));
        expect(queue.state.pending, isEmpty);

        async.elapse(const Duration(milliseconds: 50));
        expect(queue.state.pending.length, equals(1));
        expect(queue.state.pending.containsKey('second'), isTrue);
      });
    });

    test('per call debounce zero interval enqueues immediately', () {
      fakeAsync((async) {
        final command = DummyCommand.createPending(
          id: 'immediate',
          value: 42,
          strategy: CommandReplaceStrategy.single,
        );

        queue.addCommand(command, debounce: Duration.zero);

        expect(queue.state.pending.containsKey('immediate'), isTrue);
      });
    });

    test('multiple strategy commands bypass debounce', () {
      fakeAsync((async) {
        final command = DummyCommand.createPending(
          id: 'multi',
          value: 7,
          strategy: CommandReplaceStrategy.multiple,
        );

        queue.addCommand(command);

        expect(queue.state.pending.containsKey('multi'), isTrue);
      });
    });

    test('separate types get separate timers', () {
      fakeAsync((async) {
        final a1 = DummyCommand.createPending(
          id: 'A1',
          value: 1,
          strategy: CommandReplaceStrategy.single,
        );
        final b1 = DummyCommand2.createPending(
          id: 'B1',
          value: 2,
          strategy: CommandReplaceStrategy.single,
        );

        queue.addCommand(a1);

        async.elapse(const Duration(milliseconds: 50));
        queue.addCommand(b1);

        async.elapse(const Duration(milliseconds: 50));
        expect(queue.state.pending.length, equals(1));
        expect(queue.state.pending.containsKey('A1'), isTrue);

        async.elapse(const Duration(milliseconds: 50));
        expect(queue.state.pending.length, equals(2));
        expect(queue.state.pending.containsKey('B1'), isTrue);
      });
    });

    test('restoreState clears pending debounce timers', () {
      fakeAsync((async) {
        queue.addCommand(
          DummyCommand.createPending(
            id: 'debounced',
            value: 1,
            strategy: CommandReplaceStrategy.single,
          ),
        );

        queue.restoreState(
          SyncState<DummyData, ApiCommandRequest<DummyData>, DummyData,
              DummyCommand>(
            pending: {
              'restored': DummyCommand.createPending(id: 'restored', value: 9),
            },
            failed: const {},
          ),
        );

        async.elapse(const Duration(milliseconds: 200));

        expect(queue.state.pending.keys, equals({'restored'}));
      });
    });
  });
}
