import 'package:meta/meta.dart';
import 'package:uuid/uuid.dart';

import 'api_command_request.dart';
import 'api_command_response.dart';

/// Convenience alias for an untyped command.
typedef AnyApiCommand
    = ApiCommand<dynamic, ApiCommandRequest<dynamic>, dynamic, dynamic>;

/// Lifecycle status for an individual command.
enum ApiCommandStatus { idle, loading, success, error }

/// How a queue should handle newly enqueued commands of the same runtime type.
enum CommandReplaceStrategy { single, multiple }

/// Result emitted by a queue when a command completes successfully or exhausts
/// its retry policy.
final class ApiCommandResult<Command, Result> {
  /// Command associated with the completion event.
  final Command command;

  /// Final response recorded for the command.
  final ApiCommandResponse<Result?> response;

  /// Whether the final response is considered successful.
  final bool success;

  /// Creates a queue result event.
  ApiCommandResult(this.command, this.response) : success = response.success;
}

/// Aggregate flush status for a queue or orchestrator.
enum QueueFlushStatus { idle, inProgress }

/// Convenience predicates for [QueueFlushStatus].
extension QueueFlushStatusExtension on QueueFlushStatus {
  /// Whether no flush is currently running.
  bool get isIdle => this == QueueFlushStatus.idle;

  /// Whether a flush is currently running.
  bool get isInProgress => this == QueueFlushStatus.inProgress;
}

/// Base class for a retryable command persisted inside a queue.
///
/// Commands own serialization, execution, merge semantics, and optional
/// optimistic hooks. Application code is expected to provide concrete `fromJson`
/// factories and any domain-specific behavior around queue results.
abstract class ApiCommand<Payload, Request extends ApiCommandRequest<Payload>,
    Result, Self> {
  static final Uuid _uuid = Uuid();

  /// Generates a random command id.
  static String generateId() => _uuid.v4();

  /// Stable identifier used as the queue key and serialized as `id`.
  final String uuid;

  /// Serialized request payload and parameters for the command.
  final Request request;

  /// Strategy used when a queue receives another command of the same runtime
  /// type.
  final CommandReplaceStrategy strategy;

  /// Current lifecycle status of the command.
  final ApiCommandStatus status;

  /// Timestamp for the most recent update to the command record.
  final DateTime lastUpdated;

  /// Number of execution attempts recorded for the command.
  final int attemptCount;

  /// Timestamp of the first observed failure, if any.
  final DateTime? firstFailureAt;

  /// Most recent API response recorded for the command.
  final ApiCommandResponse<Result?>? apiResponse;

  @protected
  const ApiCommand({
    required this.uuid,
    required this.request,
    required this.strategy,
    required this.status,
    required this.attemptCount,
    required this.firstFailureAt,
    required this.lastUpdated,
    this.apiResponse,
  });

  /// Executes the command and returns its API response.
  Future<ApiCommandResponse<Result?>?> execute();

  /// Returns the optimistic value that application code should apply when the
  /// command is enqueued.
  Result? offlineResult() => null;

  /// Returns the compensating value that application code should apply if the
  /// command ultimately fails.
  Result? offlineRollback() => null;

  /// Merges a successful API result back into local optimistic state.
  Result offlineMerge(Result apiResult) => apiResult;

  /// Combines a new payload into the current command payload.
  Payload mergePayload(Payload update);

  /// Creates an updated copy of the command for queue state transitions.
  Self copyWith({
    Request? request,
    CommandReplaceStrategy? strategy,
    ApiCommandStatus? status,
    DateTime? lastUpdated,
    int? attemptCount,
    DateTime? firstFailureAt,
    ApiCommandResponse<Result?>? apiResponse,
  });

  /// Serializes the request payload portion of [request].
  @protected
  Object? requestDataToJson(Payload requestData);

  /// Serializes the response payload portion of [apiResponse].
  ///
  /// Consumers should return `null` when the response carries no payload.
  @protected
  Object? responseDataToJson(Result? responseData);

  /// Serializes the command using a stable JSON shape for hydrated state.
  Map<String, dynamic> toJson() => {
        'id': uuid,
        'strategy': strategy.name,
        'status': status.name,
        'lastUpdated': lastUpdated.toIso8601String(),
        'attemptCount': attemptCount,
        'firstFailureAt': firstFailureAt?.toIso8601String(),
        'request': request.toJson(requestDataToJson),
        'apiResponse': apiResponse?.toJson(responseDataToJson),
      };
}
