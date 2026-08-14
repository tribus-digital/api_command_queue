import 'dart:async';

import 'api_command_base.dart';
import 'api_command_queue.dart';
import 'api_command_request.dart';
import 'logging.dart';
import 'state_streamable.dart';

/// Coordinates one or more command queues and exposes aggregate flush state.
///
/// The orchestrator is intentionally framework-agnostic. Applications can use
/// it directly or wrap it in a domain-specific adapter that reacts to auth,
/// connectivity, or feature-level state.
class ApiCommandOrchestrator implements StateStreamable<QueueFlushStatus> {
  ApiCommandOrchestrator({
    required this.commandQueues,
    this.flushConcurrency,
    this.queueOrder,
    bool processingEnabled = true,
  }) : _processingEnabled = processingEnabled {
    for (final entry in commandQueues.entries) {
      _queueStateSubscriptions[entry.key] =
          entry.value.stream.listen(_onQueueStateChanged);
    }

    if (!_processingEnabled) {
      pauseAll();
    }
  }

  final Map<Type, StreamSubscription<dynamic>> _queueStateSubscriptions = {};
  final StreamController<QueueFlushStatus> _stateController =
      StreamController<QueueFlushStatus>.broadcast();

  /// Registered queues keyed by the command runtime type they accept.
  final Map<Type, AnyApiCommandQueueHandle> commandQueues;

  /// Maximum number of queues to flush in parallel during [flushAll].
  final int? flushConcurrency;

  /// Optional preferred queue order for [flushAll].
  final List<Type>? queueOrder;

  QueueFlushStatus _state = QueueFlushStatus.idle;
  bool _processingEnabled;
  bool _isClosed = false;

  @override
  QueueFlushStatus get state => _state;

  @override
  Stream<QueueFlushStatus> get stream => _stateController.stream;

  @override
  bool get isClosed => _isClosed;

  /// Whether new enqueues should immediately trigger queue processing.
  bool get processingEnabled => _processingEnabled;

  void _emitState(QueueFlushStatus nextState) {
    _state = nextState;
    if (!_stateController.isClosed) {
      _stateController.add(nextState);
    }
  }

  void _ensureOpen() {
    if (_isClosed) {
      throw StateError('$runtimeType is already closed.');
    }
  }

  void _onQueueStateChanged(dynamic _) {
    int activeCommands = 0;
    int activeQueues = 0;

    for (final queue in commandQueues.values) {
      activeCommands += queue.inFlightCount;
      activeQueues += queue.state.flushStatus.isInProgress ? 1 : 0;
    }

    logDebug('Active commands: $activeCommands, active queues: $activeQueues');

    if (activeQueues > 0 && activeCommands > 0) {
      _emitState(QueueFlushStatus.inProgress);
    } else {
      _emitState(QueueFlushStatus.idle);
    }
  }

  List<AnyApiCommandQueueHandle> get _orderedQueues {
    final all = commandQueues;
    if (queueOrder == null || queueOrder!.isEmpty) {
      return all.values.toList();
    }

    final result = <AnyApiCommandQueueHandle>[];
    final used = <Type>{};

    for (final type in queueOrder!) {
      if (!all.containsKey(type)) {
        throw ArgumentError('No queue registered for type $type');
      }
      result.add(all[type]!);
      used.add(type);
    }

    for (final entry in all.entries) {
      if (!used.contains(entry.key)) {
        result.add(entry.value);
      }
    }
    return result;
  }

  Result? enqueue<
      Payload,
      Result,
      Command extends ApiCommand<Payload, ApiCommandRequest<Payload>, Result,
          Object?>>(
    Command command, {
    Duration? debounce,
  }) {
    _ensureOpen();
    final queue = commandQueues[command.runtimeType] ??
        (throw ArgumentError('No queue registered for ${command.runtimeType}'));

    queue.addCommand(
      command,
      processNow: _processingEnabled,
      debounce: debounce,
    );

    return command.offlineResult();
  }

  /// The earliest time any registered queue has work due, or null when nothing
  /// is waiting.
  ///
  /// [flushAll] only processes what is currently due, so a consumer that wants
  /// retries to happen without waiting for the next user action can schedule
  /// its next flush against this.
  DateTime? get nextDueAt {
    DateTime? earliest;

    for (final queue in commandQueues.values) {
      final due = queue.nextDueAt;
      if (due == null) continue;
      if (earliest == null || due.isBefore(earliest)) {
        earliest = due;
      }
    }

    return earliest;
  }

  /// Flushes all registered queues, optionally limiting queue-level parallelism.
  Future<void> flushAll() async {
    _ensureOpen();
    _emitState(QueueFlushStatus.inProgress);

    final queues = _orderedQueues;
    if (flushConcurrency == null || flushConcurrency! <= 0) {
      logDebug(
        '[Orchestrator] flushing all ${queues.length} queues in parallel '
        '(unlimited)',
      );
      await Future.wait(queues.map((queue) => queue.flush()));
    } else {
      logDebug(
        '[Orchestrator] flushAll() - ${queues.length} queues, running '
        '$flushConcurrency at once',
      );
      for (var index = 0; index < queues.length; index += flushConcurrency!) {
        final batch = queues.skip(index).take(flushConcurrency!);
        logDebug(
          '[Orchestrator] flushing batch '
          '${index ~/ flushConcurrency! + 1}/'
          '${(queues.length / flushConcurrency!).ceil()}',
        );
        await Future.wait(batch.map((queue) => queue.flush()));
      }
    }

    _emitState(QueueFlushStatus.idle);
  }

  /// Pauses processing for all queues without removing pending commands.
  void pauseAll() {
    _ensureOpen();
    _processingEnabled = false;
    logDebug('[Orchestrator] pausing all queues');
    for (final queue in commandQueues.values) {
      queue.pause();
    }
  }

  /// Resumes processing for all queues.
  void resumeAll({bool flushNow = false}) {
    _ensureOpen();
    _processingEnabled = true;
    logDebug('[Orchestrator] resuming all queues');
    for (final queue in commandQueues.values) {
      queue.resume(flushNow: flushNow);
    }
  }

  /// Enables or disables processing and triggers a flush when re-enabled.
  void setProcessingEnabled(bool enabled) {
    _ensureOpen();
    if (enabled == _processingEnabled) {
      return;
    }

    if (enabled) {
      resumeAll(flushNow: false);
      unawaited(flushAll());
    } else {
      pauseAll();
    }
  }

  @override
  Future<void> close() async {
    if (_isClosed) {
      return;
    }
    _isClosed = true;
    await Future.wait(
      _queueStateSubscriptions.values
          .map((subscription) => subscription.cancel()),
    );
    _queueStateSubscriptions.clear();
    await Future.wait(commandQueues.values.map((queue) => queue.close()));
    await _stateController.close();
  }
}
