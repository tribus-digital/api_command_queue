import 'dart:collection';

import 'api_command_base.dart';
import 'api_command_request.dart';
import 'logging.dart';

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
    final pendingJson = _commandsIn(json, 'pending');
    final failedJson = _commandsIn(json, 'failed');

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
      flushStatus: _flushStatusIn(json),
    );
  }

  /// Reads one of the command buckets, tolerating a shape it cannot use.
  ///
  /// Throwing here would cost the other bucket as well, and a consumer
  /// persisting this state gets no say in that - `hydrated_bloc` catches the
  /// error and, by default, writes its empty fallback back over the stored
  /// copy. Losing one unreadable bucket is better than losing both, and unlike
  /// throwing it says so.
  static Map<String, dynamic> _commandsIn(
    Map<String, dynamic> json,
    String bucket,
  ) {
    final raw = json[bucket];
    if (raw == null) return const {};

    if (raw is Map) return raw.cast<String, dynamic>();

    logError(
      'SyncState.fromJson: ignoring "$bucket" - expected a map, '
      'got ${raw.runtimeType}',
    );

    return const {};
  }

  /// Reads the flush status, falling back to [QueueFlushStatus.idle].
  ///
  /// This records whether a flush happened to be running when the state was
  /// written, which is worth nothing once the process it belonged to is gone.
  /// It used to be looked up with a bare `firstWhere`, so a value this build
  /// did not recognise - a renamed status, a downgrade, a corrupt byte - threw,
  /// and took every queued command with it.
  static QueueFlushStatus _flushStatusIn(Map<String, dynamic> json) {
    final raw = json['flushStatus'];
    if (raw == null) return QueueFlushStatus.idle;

    for (final status in QueueFlushStatus.values) {
      if (status.name == raw) return status;
    }

    logError(
      'SyncState.fromJson: unrecognised flushStatus "$raw" - '
      'treating the queue as idle',
    );

    return QueueFlushStatus.idle;
  }
}
