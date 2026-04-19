import 'dart:math';

/// Defines how a queue retries failed commands.
abstract interface class RetryPolicy {
  /// Maximum number of attempts before a command is dead-lettered.
  int get maxAttempts;

  /// Maximum age for a failed command before it is culled.
  Duration get maxAge;

  /// Delay applied before the given retry attempt.
  Duration delayForAttempt(int attempt);
}

/// Exponential backoff retry policy with caps for attempts, delay, and age.
final class ExponentialBackoffRetryPolicy implements RetryPolicy {
  @override
  final int maxAttempts;
  final Duration initialDelay;
  final double backoffFactor;
  final Duration maxDelay;
  @override
  final Duration maxAge;

  const ExponentialBackoffRetryPolicy({
    this.maxAttempts = 10,
    this.initialDelay = const Duration(seconds: 1),
    this.backoffFactor = 2.0,
    this.maxDelay = const Duration(minutes: 1),
    this.maxAge = const Duration(hours: 24),
  });

  @override
  Duration delayForAttempt(int attempt) {
    final multiplier = pow(backoffFactor, attempt - 1) as double;
    final computed = initialDelay * multiplier;
    return computed > maxDelay ? maxDelay : computed;
  }
}
