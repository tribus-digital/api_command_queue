import 'package:api_command_queue/api_command_queue.dart';
import 'package:test/test.dart';

import 'test_utils.dart';

void main() {
  group('ApiCommandRequest', () {
    test('copyWith updates data and keeps parameters', () {
      final a = ApiCommandRequest(
        ApiCommandRequestMethod.post,
        DummyData(1),
        {'foo': 'bar'},
      );
      final c = a.copyWith(data: DummyData(2));

      expect(a.data.value, equals(1));
      expect(c.data.value, equals(2));
      expect(c.parameters, equals({'foo': 'bar'}));
    });

    test('parameters are exposed as an unmodifiable map', () {
      final request = ApiCommandRequest(
        ApiCommandRequestMethod.post,
        DummyData(1),
        {'foo': 'bar'},
      );

      expect(
        () => request.parameters['next'] = 'value',
        throwsUnsupportedError,
      );
    });

    test('getNamedParameter throws a clear error for missing parameters', () {
      final request = ApiCommandRequest(
        ApiCommandRequestMethod.post,
        DummyData(1),
      );

      expect(
        () => request.getNamedParameter<String>('foo'),
        throwsA(
          predicate(
            (error) =>
                error is StateError &&
                error.toString().contains('Missing request parameter "foo"'),
          ),
        ),
      );
    });

    test('getNamedParameter throws a clear error for wrong types', () {
      final request = ApiCommandRequest(
        ApiCommandRequestMethod.post,
        DummyData(1),
        {'foo': 'bar'},
      );

      expect(
        () => request.getNamedParameter<int>('foo'),
        throwsA(
          predicate(
            (error) =>
                error is StateError &&
                error.toString().contains('expected int'),
          ),
        ),
      );
    });

    test('toJson round trip', () {
      final original = ApiCommandRequest(
        ApiCommandRequestMethod.post,
        DummyData(42),
        {'x': 123},
      );

      final restored = ApiCommandRequest.fromJson<DummyData>(
        original.toJson((value) => value.toJson()),
        DummyData.fromJson,
      );

      expect(restored.method, equals(original.method));
      expect(restored.data, equals(original.data));
      expect(restored.parameters, equals(original.parameters));
    });

    test('toJson rejects non-json-safe parameter values', () {
      final request = ApiCommandRequest(
        ApiCommandRequestMethod.post,
        DummyData(42),
        {'createdAt': DateTime.now()},
      );

      expect(
        () => request.toJson((value) => value.toJson()),
        throwsArgumentError,
      );
    });
  });
}
