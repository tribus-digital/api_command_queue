import 'dart:collection';

import 'api_command_base.dart';
import 'api_command_request.dart';

/// In-memory queue state containing pending and dead-lettered commands.
final class SyncState<Payload, Request extends ApiCommandRequest<Payload>,
    Result, Command extends ApiCommand<Payload, Request, Result, Object?>> {
  /// Commands waiting to be processed or retried.
  final Map<String, Command> pending;

  /// Commands that exceeded retry or age limits.
  final Map<String, Command> failed;

  /// Current flush status for the queue.
  final QueueFlushStatus flushStatus;

  SyncState({
    required Map<String, Command> pending,
    required Map<String, Command> failed,
    this.flushStatus = QueueFlushStatus.idle,
  })  : pending = UnmodifiableMapView(Map<String, Command>.of(pending)),
        failed = UnmodifiableMapView(Map<String, Command>.of(failed));

  SyncState<Payload, Request, Result, Command> copyWith({
    Map<String, Command>? pending,
    Map<String, Command>? failed,
    QueueFlushStatus? flushStatus,
  }) {
    return SyncState<Payload, Request, Result, Command>(
      pending: pending ?? this.pending,
      failed: failed ?? this.failed,
      flushStatus: flushStatus ?? this.flushStatus,
    );
  }

  /// Serializes the state using a consumer-provided command encoder.
  Map<String, dynamic> toJson(
    Map<String, dynamic> Function(Command cmd) cmdToJson,
  ) {
    return {
      'pending': pending.map((key, cmd) => MapEntry(key, cmdToJson(cmd))),
      'failed': failed.map((key, cmd) => MapEntry(key, cmdToJson(cmd))),
      'flushStatus': flushStatus.name,
    };
  }

  /// Restores queue state from serialized JSON data.
  factory SyncState.fromJson(
    Map<String, dynamic> json,
    Command Function(Map<String, dynamic>) cmdFromJson,
  ) {
    final pendingJson = json['pending'] as Map<String, dynamic>? ?? {};
    final failedJson = json['failed'] as Map<String, dynamic>? ?? {};
    return SyncState<Payload, Request, Result, Command>(
      pending: pendingJson.map(
        (key, value) => MapEntry(
          key,
          cmdFromJson((value as Map).cast<String, dynamic>()),
        ),
      ),
      failed: failedJson.map(
        (key, value) => MapEntry(
          key,
          cmdFromJson((value as Map).cast<String, dynamic>()),
        ),
      ),
      flushStatus: json['flushStatus'] != null
          ? QueueFlushStatus.values.firstWhere(
              (status) => status.name == json['flushStatus'] as String,
            )
          : QueueFlushStatus.idle,
    );
  }
}
