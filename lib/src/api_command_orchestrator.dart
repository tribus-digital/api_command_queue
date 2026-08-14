import 'dart:async';

import 'package:clock/clock.dart';

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
    this.autoFlushWhenDue = false,
  }) : _processingEnabled = processingEnabled {
    for (final entry in commandQueues.entries) {
      _queueStateSubscriptions[entry.key] =
          entry.value.stream.listen(_onQueueStateChanged);
    }

    if (!_processingEnabled) {
      pauseAll();
    }
  }

  /// Whether the orchestrator flushes itself when a command's backoff comes
  /// due.
  ///
  /// A flush only processes commands that are due, so without this a failed
  /// command waits for something else to trigger the next flush - a
  /// connectivity change, an app resume, a user action. On a device sitting
  /// idle, nothing does, and the retry never happens.
  ///
  /// With this on, the orchestrator holds a single timer set to the earliest
  /// [nextDueAt] across its queues and flushes when it fires. Off by default:
  /// consumers that already drive flushing on their own schedule do not need a
  /// second thing doing it.
  final bool autoFlushWhenDue;

  /// Floor on the auto-flush timer.
  ///
  /// A command that is due but that a flush does not clear - a paused queue,
  /// say - leaves [nextDueAt] in the past, and rescheduling straight away would
  /// spin. This turns that into a slow poll instead.
  static const _minimumAutoFlushDelay = Duration(seconds: 1);

  Timer? _dueTimer;

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
  bool _isFlushing = false;

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

  /// Sets the auto-flush timer to whenever the next command comes due.
  ///
  /// Called after anything that can change what is queued or when it is due.
  void _scheduleNextFlush() {
    _dueTimer?.cancel();
    _dueTimer = null;

    if (!autoFlushWhenDue || _isClosed || !_processingEnabled) {
      return;
    }

    /// a flush already running will pick up anything due, and reschedules when
    /// it finishes - timing another one alongside it just walks every queue
    /// again for nothing
    if (_isFlushing) {
      return;
    }

    final due = nextDueAt;
    if (due == null) {
      return;
    }

    final wait = due.difference(clock.now());
    _dueTimer = Timer(
      wait < _minimumAutoFlushDelay ? _minimumAutoFlushDelay : wait,
      () {
        _dueTimer = null;
        if (_isClosed) return;

        logDebug('[Orchestrator] auto-flushing, a command is due');
        unawaited(flushAll());
      },
    );
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

    /// when a command becomes due changes as queues work: one that fails is due
    /// its backoff, one that finishes may leave nothing due at all, and a queue
    /// processing an enqueue on its own never goes near [flushAll]. Scheduling
    /// from here rather than from the few places that call it means the timer
    /// tracks the queues instead of guessing at them.
    _scheduleNextFlush();
  }

  /// The queues to flush, in order, each appearing once.
  ///
  /// A queue is usually registered under more than one command type - a create
  /// and a patch that share it - so walking [commandQueues] or [queueOrder]
  /// naively visits the same queue repeatedly. The repeats are no-ops, since a
  /// flush already running returns its in-progress future, but they make the
  /// walk several times longer than it needs to be and the ordering log
  /// impossible to read.
  List<AnyApiCommandQueueHandle> get _orderedQueues {
    final all = commandQueues;
    final result = <AnyApiCommandQueueHandle>[];
    final seen = Set<AnyApiCommandQueueHandle>.identity();

    void add(AnyApiCommandQueueHandle queue) {
      if (seen.add(queue)) result.add(queue);
    }

    if (queueOrder != null) {
      for (final type in queueOrder!) {
        final queue = all[type];
        if (queue == null) {
          throw ArgumentError('No queue registered for type $type');
        }
        add(queue);
      }
    }

    for (final queue in all.values) {
      add(queue);
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

    _scheduleNextFlush();

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
    _isFlushing = true;
    _emitState(QueueFlushStatus.inProgress);

    try {
      await _flushOrderedQueues();
    } finally {
      _isFlushing = false;
      _emitState(QueueFlushStatus.idle);
      _scheduleNextFlush();
    }
  }

  Future<void> _flushOrderedQueues() async {
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

      /// nothing will be processed until this is turned back on, and that call
      /// flushes anyway
      _dueTimer?.cancel();
      _dueTimer = null;
    }
  }

  @override
  Future<void> close() async {
    if (_isClosed) {
      return;
    }
    _isClosed = true;
    _dueTimer?.cancel();
    _dueTimer = null;
    await Future.wait(
      _queueStateSubscriptions.values
          .map((subscription) => subscription.cancel()),
    );
    _queueStateSubscriptions.clear();
    await Future.wait(commandQueues.values.map((queue) => queue.close()));
    await _stateController.close();
  }
}
