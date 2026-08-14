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

    /// A consumer persisting this state does not get to decide what a throw in
    /// here costs it. `hydrated_bloc` catches the error, falls back to an empty
    /// queue and - by default - writes that back over the stored copy, so
    /// anything this decoder refuses to read takes every queued command with it.
    group('decoding state it cannot fully read', () {
      SyncState<DummyData, ApiCommandRequest<DummyData>, DummyData, DummyCommand>
          restore(Map<String, dynamic> json) {
        return SyncState.fromJson(json, DummyCommand.fromJson);
      }

      Map<String, dynamic> stateWith({
        Object? flushStatus,
        Object? pending,
        Object? failed,
      }) {
        final state = SyncState<DummyData, ApiCommandRequest<DummyData>,
            DummyData, DummyCommand>(
          pending: {'one': c1},
          failed: {'two': c2},
        ).toJson((cmd) => cmd.toJson());

        if (flushStatus != null) state['flushStatus'] = flushStatus;
        if (pending != null) state['pending'] = pending;
        if (failed != null) state['failed'] = failed;

        return state;
      }

      test('an unrecognised flushStatus does not cost the queue', () {
        /// a renamed status, a downgrade, a corrupt byte - none of which say
        /// anything about whether the commands are readable
        final restored = restore(stateWith(flushStatus: 'somethingElse'));

        expect(restored.flushStatus, QueueFlushStatus.idle);
        expect(restored.pending.keys, {'one'});
        expect(restored.failed.keys, {'two'});
      });

      test('a flushStatus of the wrong type does not cost the queue', () {
        final restored = restore(stateWith(flushStatus: 7));

        expect(restored.flushStatus, QueueFlushStatus.idle);
        expect(restored.pending.keys, {'one'});
      });

      test('a missing flushStatus reads as idle', () {
        final state = stateWith()..remove('flushStatus');

        expect(restore(state).flushStatus, QueueFlushStatus.idle);
      });

      test('an unreadable bucket does not cost the other one', () {
        final restored = restore(stateWith(failed: 'not a map'));

        expect(restored.pending.keys, {'one'});
        expect(restored.failed, isEmpty);
      });

      test('missing buckets decode as empty', () {
        final restored = restore({'flushStatus': 'idle'});

        expect(restored.pending, isEmpty);
        expect(restored.failed, isEmpty);
      });

      test('buckets that came back loosely typed still decode', () {
        /// storage backends hand maps back as Map<dynamic, dynamic> often
        /// enough that a hard cast here is a liability
        final state = stateWith();
        final pending = <dynamic, dynamic>{...state['pending'] as Map};

        final restored = restore({...state, 'pending': pending});

        expect(restored.pending.keys, {'one'});
      });
    });
  });
}
