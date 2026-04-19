import 'package:api_command_queue/api_command_queue.dart';
import 'package:test/test.dart';

import 'test_utils.dart';

void main() {
  group('SyncState', () {
    late DummyCommand c1;
    late DummyCommand c2;

    setUp(() {
      c1 = DummyCommand.createPending(id: 'one', value: 1);
      c2 = DummyCommand.createPending(id: 'two', value: 2);
    });

    test('copyWith replaces pending and failed maps', () {
      final state = SyncState<DummyData, ApiCommandRequest<DummyData>,
          DummyData, DummyCommand>(
        pending: {'one': c1},
        failed: const {},
      );

      final next = state.copyWith(failed: {'two': c2});

      expect(next.pending, equals(state.pending));
      expect(next.failed, equals({'two': c2}));
    });

    test('toJson round trip', () {
      final state = SyncState<DummyData, ApiCommandRequest<DummyData>,
          DummyData, DummyCommand>(
        pending: {'one': c1},
        failed: {'two': c2},
      );

      final restored = SyncState<DummyData, ApiCommandRequest<DummyData>,
          DummyData, DummyCommand>.fromJson(
        state.toJson((cmd) => cmd.toJson()),
        DummyCommand.fromJson,
      );

      expect(restored.pending.keys, equals({'one'}));
      expect(restored.failed.keys, equals({'two'}));
      expect(restored.pending['one']!.uuid, equals(c1.uuid));
      expect(restored.failed['two']!.uuid, equals(c2.uuid));
    });

    test('pending and failed maps are unmodifiable', () {
      final state = SyncState<DummyData, ApiCommandRequest<DummyData>,
          DummyData, DummyCommand>(
        pending: {'one': c1},
        failed: {'two': c2},
      );

      expect(() => state.pending.remove('one'), throwsUnsupportedError);
      expect(() => state.failed['three'] = c1, throwsUnsupportedError);
    });
  });
}
