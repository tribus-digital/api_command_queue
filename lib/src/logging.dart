import 'api_command_queue_logger.dart';

void logDebug(String message) {
  apiCommandQueueLogger.debug(message);
}

void logError(
  String message, {
  Object? error,
  StackTrace? stackTrace,
}) {
  apiCommandQueueLogger.error(
    message,
    error: error,
    stackTrace: stackTrace,
  );
}
