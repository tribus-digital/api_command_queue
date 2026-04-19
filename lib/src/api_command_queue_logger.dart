/// Signature for debug log events emitted by the package.
typedef ApiCommandQueueDebugLogger = void Function(String message);

/// Signature for error log events emitted by the package.
typedef ApiCommandQueueErrorLogger = void Function(
  String message, {
  Object? error,
  StackTrace? stackTrace,
});

void _noopDebug(String message) {}

void _noopError(
  String message, {
  Object? error,
  StackTrace? stackTrace,
}) {}

/// Logger callbacks used by `api_command_queue`.
///
/// Logging is disabled by default. Applications may replace the logger with
/// custom debug and error callbacks for diagnostics.
final class ApiCommandQueueLogger {
  /// Creates a logger configuration.
  const ApiCommandQueueLogger({
    this.debug = _noopDebug,
    this.error = _noopError,
  });

  /// Callback used for debug-level queue events.
  final ApiCommandQueueDebugLogger debug;

  /// Callback used for error-level queue events.
  final ApiCommandQueueErrorLogger error;
}

ApiCommandQueueLogger _apiCommandQueueLogger = const ApiCommandQueueLogger();

/// The active logger configuration for the package.
ApiCommandQueueLogger get apiCommandQueueLogger => _apiCommandQueueLogger;

/// Replaces the active logger callbacks.
///
/// Any callback left unspecified becomes a no-op.
void configureApiCommandQueueLogger({
  ApiCommandQueueDebugLogger? debug,
  ApiCommandQueueErrorLogger? error,
}) {
  _apiCommandQueueLogger = ApiCommandQueueLogger(
    debug: debug ?? _noopDebug,
    error: error ?? _noopError,
  );
}

/// Restores the default no-op logger.
void resetApiCommandQueueLogger() {
  _apiCommandQueueLogger = const ApiCommandQueueLogger();
}
