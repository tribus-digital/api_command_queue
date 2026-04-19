import 'package:api_command_queue/api_command_queue.dart';
import 'package:test/test.dart';

void main() {
  group('ApiCommandResponse', () {
    test('success getter is correct for 2xx', () {
      final response = ApiCommandResponse<String>('ok', false, status: 200);

      expect(response.data, equals('ok'));
      expect(response.fromCache, isFalse);
      expect(response.status, equals(200));
      expect(response.error, isNull);
      expect(response.success, isTrue);
    });

    test('2xx null payload responses are still successful', () {
      expect(
        ApiCommandResponse<String>(null, false, status: 204).success,
        isTrue,
      );
      expect(
        ApiCommandResponse<String>(null, false, status: 201).success,
        isTrue,
      );
    });

    test('failure when status >= 400 or an error is present', () {
      expect(
        ApiCommandResponse<String>('data', false, status: 404).success,
        isFalse,
      );
      expect(
        ApiCommandResponse<String>(null, false, status: 204, error: 'err')
            .success,
        isFalse,
      );
      expect(
        ApiCommandResponse<String>(null, false, status: 500, error: 'err')
            .success,
        isFalse,
      );
    });

    test('toJson round trip', () {
      final original = ApiCommandResponse<Map<String, dynamic>>(
        {'x': 1},
        true,
        status: 201,
      );

      final restored = ApiCommandResponse.fromJson<Map<String, dynamic>>(
        original.toJson((map) => map),
        (json) => Map<String, dynamic>.from(json as Map),
      );

      expect(restored.data, equals({'x': 1}));
      expect(restored.fromCache, isTrue);
      expect(restored.status, equals(201));
      expect(restored.error, isNull);
      expect(restored.timestamp, isA<int>());
      expect(restored.success, isTrue);
    });

    test('toJson round trip preserves null successful payloads', () {
      final original = ApiCommandResponse<Map<String, dynamic>>(
        null,
        false,
        status: 204,
      );

      final restored = ApiCommandResponse.fromJson<Map<String, dynamic>>(
        original.toJson((map) => map),
        (json) => Map<String, dynamic>.from(json as Map),
      );

      expect(restored.data, isNull);
      expect(restored.status, equals(204));
      expect(restored.success, isTrue);
    });
  });
}
