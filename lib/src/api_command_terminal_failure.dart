import 'package:meta/meta.dart';

import 'api_command_response.dart';

/// Returns true when a failed response should permanently stop retrying.
typedef ApiCommandTerminalFailurePredicate<Result> = bool Function(
  ApiCommandResponse<Result?> response,
);

/// Reusable terminal-failure rule.
///
/// A rule can match exact status codes, status predicates, response body
/// predicates, error predicates, or full response predicates. Successful
/// responses never match, even if one of the configured predicates would return
/// true.
@immutable
final class ApiCommandTerminalFailureRule<Result> {
  /// Exact failed response status codes that should stop retries.
  final Set<int> statusCodes;

  /// Predicate for failed response status codes.
  final bool Function(int status)? statusMatches;

  /// Predicate for failed response payloads.
  final bool Function(Result? data)? dataMatches;

  /// Predicate for failed response errors.
  final bool Function(String? error)? errorMatches;

  /// Predicate with access to the full failed response.
  final ApiCommandTerminalFailurePredicate<Result>? responseMatches;

  /// Creates a reusable terminal-failure rule.
  const ApiCommandTerminalFailureRule({
    this.statusCodes = const <int>{},
    this.statusMatches,
    this.dataMatches,
    this.errorMatches,
    this.responseMatches,
  });

  /// Returns true when [response] is failed and matches any configured rule.
  bool matches(ApiCommandResponse<Result?> response) {
    if (response.success) {
      return false;
    }

    return statusCodes.contains(response.status) ||
        (statusMatches?.call(response.status) ?? false) ||
        (dataMatches?.call(response.data) ?? false) ||
        (errorMatches?.call(response.error) ?? false) ||
        (responseMatches?.call(response) ?? false);
  }
}
