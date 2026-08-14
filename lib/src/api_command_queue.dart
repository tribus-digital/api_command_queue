import 'dart:async';

import 'package:clock/clock.dart';

import 'api_command_base.dart';
import 'api_command_request.dart';
import 'api_command_response.dart';
import 'api_command_terminal_failure.dart';
import 'logging.dart';
import 'retry_policy.dart';
import 'state_streamable.dart';
import 'sync_state.dart';

/// Interface exposed by command queues to orchestrators and adapters.
abstract interface class ApiCommandQueueHandle<
        Payload,
        Request extends ApiCommandRequest<Payload>,
        Result,
        Command extends ApiCommand<Payload, Request, Result, Object?>>
    implements StateStreamable<SyncState<Payload, Request, Result, Command>> {
  /// Number of commands currently executing.
  int get inFlightCount;

  /// Whether queue processing is currently paused.
  bool get isPaused;

  /// Whether a flush loop is currently active.
  bool get isFlushing;

  /// Emits final success and terminal failure events for processed commands.
  Stream<ApiCommandResult<Command, Result>> get results;

  /// The earliest time a pending command is due, or null if none are waiting.
  ///
  /// A flush only processes what is currently due, so a consumer that wants
  /// retries to happen without waiting for the next user action can schedule
  /// against this rather than poll for one.
  DateTime? get nextDueAt;

  /// Adds a command to the pending queue.
  void addCommand(
    covariant Command command, {
    bool processNow = false,
    Duration? debounce,
  });

  /// Flushes pending commands until the queue is empty or paused.
  Future<void> flush();

  /// Prevents the queue from processing additional commands.
  void pause();

  /// Allows the queue to process commands again.
  void resume({bool flushNow = false});
}

/// Convenience alias for an untyped queue handle.
typedef AnyApiCommandQueueHandle = ApiCommandQueueHandle<dynamic,
    ApiCommandRequest<dynamic>, dynamic, AnyApiCommand>;

/// A stateful queue that persists retryable commands and executes them with a
/// configurable retry policy.
///
/// Concrete queues provide a `commandFromJson` callback so serialized state can
/// restore command objects without reflection.
abstract base class ApiCommandQueue<
        Payload,
        Request extends ApiCommandRequest<Payload>,
        Result,
        Command extends ApiCommand<Payload, Request, Result, Command>>
    implements ApiCommandQueueHandle<Payload, Request, Result, Command> {
  ApiCommandQueue({
    required Command Function(Map<String, dynamic>) commandFromJson,
    RetryPolicy? retryPolicy,
    this.terminalFailurePredicate,
    this.failedCapacity = 100,
    this.concurrencyLimit,
    this.defaultDebounce = Duration.zero,
  })  : _retryPolicy = retryPolicy ?? const ExponentialBackoffRetryPolicy(),
        _commandFromJson = commandFromJson,
        _state = SyncState<Payload, Request, Result, Command>(
          pending: const {},
          failed: const {},
        );

  final RetryPolicy _retryPolicy;
  final Command Function(Map<String, dynamic>) _commandFromJson;

  /// Queue-wide non-retryable failure predicate.
  ///
  /// This is evaluated after a command returns or throws a failed response, and
  /// before retry policy exhaustion is checked. If this returns true, the
  /// command is moved to the failed/dead-letter collection immediately.
  final ApiCommandTerminalFailurePredicate<Result>? terminalFailurePredicate;

  final int? concurrencyLimit;
  final int failedCapacity;

  final Map<String, Command> _inFlight = {};
  final Set<Future<void>> _activeCommands = <Future<void>>{};
  final Map<Type, Timer> _debounceTimers = {};
  final Map<Type, _DebouncedCommandEntry<Command>> _debouncePending = {};

  final StreamController<SyncState<Payload, Request, Result, Command>>
      _stateController = StreamController<
          SyncState<Payload, Request, Result, Command>>.broadcast();

  final StreamController<ApiCommandResult<Command, Result>> _resultsController =
      StreamController<ApiCommandResult<Command, Result>>.broadcast();

  SyncState<Payload, Request, Result, Command> _state;
  bool _paused = false;
  bool _isFlushing = false;
  bool _isClosed = false;
  Completer<void>? _flushCompleter;

  /// Default debounce interval for single-replacement commands.
  final Duration? defaultDebounce;

  @override
  SyncState<Payload, Request, Result, Command> get state => _state;

  @override
  Stream<SyncState<Payload, Request, Result, Command>> get stream =>
      _stateController.stream;

  @override
  bool get isClosed => _isClosed;

  @override
  int get inFlightCount => _inFlight.length;

  @override
  bool get isPaused => _paused;

  @override
  bool get isFlushing => _isFlushing;

  @override
  Stream<ApiCommandResult<Command, Result>> get results =>
      _resultsController.stream;

  /// Restores queue state from serialized JSON data.
  SyncState<Payload, Request, Result, Command> fromJson(
    Map<String, dynamic> json,
  ) {
    return SyncState.fromJson(json, _commandFromJson);
  }

  /// Serializes queue state into JSON.
  Map<String, dynamic> toJson(
    SyncState<Payload, Request, Result, Command> state,
  ) {
    return state.toJson((cmd) => cmd.toJson());
  }

  /// Replaces the in-memory queue state.
  void restoreState(SyncState<Payload, Request, Result, Command> newState) {
    _ensureOpen();
    _clearDebouncedCommands();
    _inFlight.clear();
    _emitState(newState);
  }

  void _emitState(SyncState<Payload, Request, Result, Command> nextState) {
    _state = nextState;
    if (!_isClosed && !_stateController.isClosed) {
      _stateController.add(nextState);
    }
  }

  void _ensureOpen() {
    if (_isClosed) {
      throw StateError('$runtimeType is already closed.');
    }
  }

  @override
  void pause() {
    _ensureOpen();
    _paused = true;
  }

  @override
  void resume({bool flushNow = false}) {
    _ensureOpen();
    _paused = false;
    if (flushNow) {
      unawaited(flush());
    }
  }

  @override
  void addCommand(
    covariant Command command, {
    bool processNow = false,
    Duration? debounce,
  }) {
    _ensureOpen();
    final interval = debounce ?? defaultDebounce;
    final isSingle = command.strategy == CommandReplaceStrategy.single;
    final key = command.runtimeType;

    if (isSingle && interval != null && interval > Duration.zero) {
      _debouncePending[key] = _DebouncedCommandEntry(
        command: command,
        processNow: processNow,
      );
      _debounceTimers[key]?.cancel();
      _debounceTimers[key] = Timer(interval, () => _flushDebouncedCommand(key));
      return;
    }

    _clearDebouncedCommand(key);

    _enqueueImmediate(command, processNow: processNow);
  }

  void _flushDebouncedCommand(Type key) {
    _debounceTimers.remove(key);
    final entry = _debouncePending.remove(key);
    if (entry == null || _isClosed) {
      return;
    }

    logDebug('[$runtimeType] enqueueing debounced command $key');
    _enqueueImmediate(entry.command, processNow: entry.processNow);
  }

  void _enqueueImmediate(Command command, {bool processNow = false}) {
    logDebug('[$runtimeType] enqueueing command ${command.uuid}');
    final pending = Map<String, Command>.of(state.pending);

    if (command.strategy == CommandReplaceStrategy.single) {
      final existingKey = pending.entries
          .firstWhere(
            (entry) => entry.value.runtimeType == command.runtimeType,
            orElse: () => MapEntry('', command),
          )
          .key;
      if (existingKey.isNotEmpty) {
        logDebug(
          '[$runtimeType] replacing existing ${command.runtimeType} '
          '(id=$existingKey)',
        );
        pending.remove(existingKey);
      }
    }

    pending[command.uuid] = command;
    _emitState(state.copyWith(pending: pending));

    if (processNow) {
      unawaited(flush());
    }
  }

  @override
  Future<void> flush() async {
    _ensureOpen();
    final inProgress = _flushCompleter;
    if (inProgress != null) {
      logDebug('[$runtimeType] flush() already in progress');
      return inProgress.future;
    }

    final completer = Completer<void>();
    _flushCompleter = completer;
    _isFlushing = true;
    logDebug(
      '[$runtimeType] flush() start (limit=$concurrencyLimit, paused=$_paused)',
    );
    _emitState(state.copyWith(flushStatus: QueueFlushStatus.inProgress));

    try {
      while (!_paused && !_isClosed) {
        _scheduleAvailableCommands();

        if (_activeCommands.isEmpty) {
          if (_runnableCommands().isEmpty) {
            break;
          }
          continue;
        }

        await Future.any(_activeCommands.toList(growable: false));
      }

      if (_activeCommands.isNotEmpty) {
        await Future.wait(_activeCommands.toList(growable: false));
      }
    } finally {
      _isFlushing = false;
      _flushCompleter = null;
      logDebug('[$runtimeType] flush() end');
      _emitState(state.copyWith(flushStatus: QueueFlushStatus.idle));
      if (!completer.isCompleted) {
        completer.complete();
      }
    }
  }

  /// When [command] may next be attempted.
  ///
  /// One that has never failed is due immediately. One that has is due its
  /// backoff after the attempt that failed, which [ApiCommand.lastUpdated]
  /// records.
  DateTime nextAttemptAt(Command command) {
    if (command.attemptCount == 0) {
      return command.lastUpdated;
    }

    return command.lastUpdated.add(
      _retryPolicy.delayForAttempt(command.attemptCount),
    );
  }

  @override
  DateTime? get nextDueAt {
    DateTime? earliest;

    for (final command in state.pending.values) {
      final due = nextAttemptAt(command);
      if (earliest == null || due.isBefore(earliest)) {
        earliest = due;
      }
    }

    return earliest;
  }

  List<Command> _runnableCommands() {
    final now = clock.now();

    return state.pending.values
        .where(
          (command) =>
              !_inFlight.containsKey(command.uuid) &&
              command.status != ApiCommandStatus.loading &&
              !now.isBefore(nextAttemptAt(command)),
        )
        .toList(growable: false);
  }

  void _scheduleAvailableCommands() {
    final runnable = _runnableCommands();
    if (runnable.isEmpty) {
      return;
    }

    final limit = concurrencyLimit;
    final maxToSchedule = limit == null
        ? runnable.length
        : (limit - _activeCommands.length).clamp(0, runnable.length);
    if (maxToSchedule <= 0) {
      return;
    }

    for (final command in runnable.take(maxToSchedule)) {
      final future = _processCommand(command);
      _activeCommands.add(future);
      future.whenComplete(() {
        _activeCommands.remove(future);
      });
    }
  }

  Future<void> _processCommand(Command command) async {
    final stored = state.pending[command.uuid];
    if (stored == null) {
      return;
    }

    final now = clock.now();
    final tries = stored.attemptCount;
    final firstFail = stored.firstFailureAt;

    if (_shouldDeadLetter(stored, now)) {
      logDebug('[$runtimeType] dead-lettering command ${command.uuid}');
      final deadLetter = stored.copyWith(
        status: ApiCommandStatus.error,
        firstFailureAt: firstFail ?? now,
        lastUpdated: now,
      );
      _moveToFailed(deadLetter);
      _emitResult(
        deadLetter,
        deadLetter.apiResponse ??
            ApiCommandResponse<Result?>(
              null,
              false,
              status: 500,
              error: 'Command exceeded retry policy.',
            ),
      );
      return;
    }

    if (_inFlight.containsKey(command.uuid)) {
      return;
    }
    _inFlight[command.uuid] = stored;

    var currentCommand = stored;
    try {
      if (tries > 0) {
        logDebug('[$runtimeType] retrying ${command.uuid}, attempt ${tries + 1}');
      }

      final latest = state.pending[command.uuid];
      if (latest == null || _isClosed) {
        return;
      }

      final loading = latest.copyWith(
        status: ApiCommandStatus.loading,
        lastUpdated: clock.now(),
      );
      currentCommand = loading;
      _replacePending(loading);

      final response = await loading.execute();

      if (response?.success == true) {
        logDebug('[$runtimeType] command ${loading.uuid} succeeded');
        _removePending(loading.uuid);

        final done = loading.copyWith(
          status: ApiCommandStatus.success,
          apiResponse: response,
          lastUpdated: clock.now(),
        );
        _emitResult(done, response!);
      } else {
        final failTime = firstFail ?? clock.now();
        final failedResponse = response ??
            ApiCommandResponse<Result?>(
              null,
              false,
              status: 500,
              error: 'Command returned no response.',
            );
        final failed = loading.copyWith(
          status: ApiCommandStatus.error,
          apiResponse: failedResponse,
          attemptCount: tries + 1,
          firstFailureAt: failTime,
          lastUpdated: clock.now(),
        );

        if (_isTerminalFailure(failed, failedResponse)) {
          logDebug(
            '[$runtimeType] terminal failure for command ${failed.uuid} '
            '(status=${failedResponse.status}, error=${failedResponse.error})',
          );
          _moveToFailed(failed);
          _emitResult(failed, failedResponse);
        } else if (_shouldDeadLetter(failed, clock.now())) {
          _moveToFailed(failed);
          _emitResult(failed, failedResponse);
        } else {
          _replacePending(failed);
        }
      }
    } catch (error, stackTrace) {
      logDebug('[$runtimeType] exception running ${command.uuid}: $error');
      final failTime = firstFail ?? now;
      final synthetic = ApiCommandResponse<Result?>(
        null,
        false,
        status: 500,
        error: error.toString(),
      );
      final failed = currentCommand.copyWith(
        status: ApiCommandStatus.error,
        apiResponse: synthetic,
        attemptCount: tries + 1,
        firstFailureAt: failTime,
        lastUpdated: clock.now(),
      );

      onError(error, stackTrace);

      if (_isTerminalFailure(failed, synthetic)) {
        logDebug(
          '[$runtimeType] terminal exception failure for command '
          '${failed.uuid} (error=${synthetic.error})',
        );
        _moveToFailed(failed);
        _emitResult(failed, synthetic);
      } else if (_shouldDeadLetter(failed, clock.now())) {
        _moveToFailed(failed);
        _emitResult(failed, synthetic);
      } else {
        _replacePending(failed);
      }
    } finally {
      _inFlight.remove(command.uuid);
    }
  }

  bool _isTerminalFailure(
    Command command,
    ApiCommandResponse<Result?> response,
  ) {
    if (response.success) {
      return false;
    }

    return (terminalFailurePredicate?.call(response) ?? false) ||
        command.isTerminalFailure(response);
  }

  bool _shouldDeadLetter(Command command, DateTime now) {
    return command.attemptCount >= _retryPolicy.maxAttempts ||
        (command.firstFailureAt != null &&
            now.difference(command.firstFailureAt!) > _retryPolicy.maxAge);
  }

  void _replacePending(Command command) {
    final pending = Map<String, Command>.of(state.pending)
      ..[command.uuid] = command;
    _emitState(state.copyWith(pending: pending));
  }

  void _removePending(String commandId) {
    final pending = Map<String, Command>.of(state.pending)..remove(commandId);
    _emitState(state.copyWith(pending: pending));
  }

  void _moveToFailed(Command command) {
    final pending = Map<String, Command>.of(state.pending);
    final failed = Map<String, Command>.of(state.failed);
    pending.remove(command.uuid);

    final now = clock.now();
    failed.removeWhere(
      (_, queued) =>
          queued.firstFailureAt != null &&
          now.difference(queued.firstFailureAt!) > _retryPolicy.maxAge,
    );

    if (failed.length >= failedCapacity) {
      final sorted = failed.entries.toList()
        ..sort(
          (a, b) =>
              _failureSortTime(a.value).compareTo(_failureSortTime(b.value)),
        );
      for (final entry in sorted.take(failed.length - failedCapacity + 1)) {
        failed.remove(entry.key);
      }
    }

    failed[command.uuid] = command;
    _emitState(state.copyWith(pending: pending, failed: failed));
    logDebug('[$runtimeType] command ${command.uuid} moved to dead-letter');
  }

  DateTime _failureSortTime(Command command) {
    return command.firstFailureAt ?? command.lastUpdated;
  }

  void _emitResult(Command command, ApiCommandResponse<Result?> response) {
    if (_isClosed || _resultsController.isClosed) {
      return;
    }
    _resultsController
        .add(ApiCommandResult<Command, Result>(command, response));
  }

  void _clearDebouncedCommand(Type key) {
    _debounceTimers.remove(key)?.cancel();
    _debouncePending.remove(key);
  }

  void _clearDebouncedCommands() {
    for (final timer in _debounceTimers.values) {
      timer.cancel();
    }
    _debounceTimers.clear();
    _debouncePending.clear();
  }

  /// Hook for subclasses that want custom error handling.
  void onError(Object error, StackTrace stackTrace) {
    logError(
      '[$runtimeType] uncaught error',
      error: error,
      stackTrace: stackTrace,
    );
  }

  @override
  Future<void> close() async {
    if (_isClosed) {
      return;
    }
    _paused = true;
    _isClosed = true;
    _clearDebouncedCommands();
    if (_activeCommands.isNotEmpty) {
      await Future.wait(_activeCommands.toList(growable: false));
    }
    await _resultsController.close();
    await _stateController.close();
  }
}

final class _DebouncedCommandEntry<Command> {
  const _DebouncedCommandEntry({
    required this.command,
    required this.processNow,
  });

  final Command command;
  final bool processNow;
}
