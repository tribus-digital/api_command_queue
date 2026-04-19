import 'package:api_command_queue/api_command_queue.dart';
import 'package:test/test.dart';

void main() {
  group('ExponentialBackoffRetryPolicy', () {
    test('has correct defaults', () {
      final policy = ExponentialBackoffRetryPolicy();

      expect(policy.maxAttempts, equals(10));
      expect(policy.initialDelay, equals(const Duration(seconds: 1)));
      expect(policy.backoffFactor, equals(2.0));
      expect(policy.maxDelay, equals(const Duration(minutes: 1)));
      expect(policy.maxAge, equals(const Duration(hours: 24)));
    });

    test('delayForAttempt grows exponentially', () {
      final policy = ExponentialBackoffRetryPolicy(
        initialDelay: const Duration(seconds: 1),
        backoffFactor: 2.0,
        maxDelay: const Duration(seconds: 10),
      );

      expect(policy.delayForAttempt(1), equals(const Duration(seconds: 1)));
      expect(policy.delayForAttempt(2), equals(const Duration(seconds: 2)));
      expect(policy.delayForAttempt(3), equals(const Duration(seconds: 4)));
      expect(policy.delayForAttempt(4), equals(const Duration(seconds: 8)));
    });

    test('delayForAttempt caps at maxDelay', () {
      final policy = ExponentialBackoffRetryPolicy(
        initialDelay: const Duration(seconds: 10),
        backoffFactor: 2.0,
        maxDelay: const Duration(seconds: 15),
      );

      expect(policy.delayForAttempt(2), equals(const Duration(seconds: 15)));
      expect(policy.delayForAttempt(100), equals(const Duration(seconds: 15)));
    });
  });
}
