import 'json_codec.dart';

/// Serialized API response data associated with a queued command.
///
/// The queue persists this model as part of serialized command state, so the
/// JSON shape should remain stable across package upgrades.
final class ApiCommandResponse<T> {
  /// Response payload restored by a consumer-provided decoder.
  final T? data;

  /// Whether the response was sourced from cache.
  final bool fromCache;

  /// HTTP-like status code associated with the response.
  final int status;

  /// Optional transport or domain error recorded for the response.
  final String? error;

  /// Milliseconds since epoch when the response wrapper was created.
  final int timestamp;

  /// Returns `true` for successful `2xx` responses with no attached error.
  ///
  /// This intentionally treats `204 No Content` and similar `2xx` responses
  /// with `null` data as successful queue executions.
  bool get success => status >= 200 && status < 300 && error == null;

  /// Creates a response using the current time as the persisted timestamp.
  ApiCommandResponse(
    T? data,
    bool fromCache, {
    int status = -1,
    String? error,
  }) : this._(
          data: data,
          fromCache: fromCache,
          status: status,
          error: error,
          timestamp: DateTime.now().millisecondsSinceEpoch,
        );

  const ApiCommandResponse._({
    required this.data,
    required this.fromCache,
    required this.status,
    required this.error,
    required this.timestamp,
  });

  /// Serializes the response using a consumer-provided payload encoder.
  Map<String, dynamic> toJson(JsonEncoder<T?> encodeData) {
    return {
      'data': data == null ? null : encodeData(data),
      'fromCache': fromCache,
      'status': status,
      'error': error,
      'timestamp': timestamp,
    };
  }

  /// Convenience helper for code that prefers a static predicate.
  static bool isSuccessful<T>(ApiCommandResponse<T> response) =>
      response.success;

  /// Restores a response from JSON state.
  ///
  /// Consumers must provide a [decodeData] callback for non-null payloads.
  static ApiCommandResponse<D> fromJson<D>(
    Map<String, dynamic> json,
    JsonDecoder<D> decodeData,
  ) {
    final rawData = json['data'];
    return ApiCommandResponse._(
      data: rawData == null ? null : decodeData(rawData),
      fromCache: (json['fromCache'] as bool?) ?? false,
      status: (json['status'] as int?) ?? -1,
      error: json['error'] as String?,
      timestamp: (json['timestamp'] as int?) ?? 0,
    );
  }
}
