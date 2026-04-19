/// Encodes a typed value into a JSON-safe representation.
typedef JsonEncoder<T> = Object? Function(T value);

/// Decodes a typed value from a JSON representation.
typedef JsonDecoder<T> = T Function(Object? json);

/// Adapts a `Map<String, dynamic>` decoder to a generic [JsonDecoder].
JsonDecoder<T> jsonMapDecoder<T>(
  T Function(Map<String, dynamic> json) decoder,
) {
  return (json) => decoder((json as Map).cast<String, dynamic>());
}

/// Adapts a map encoder to a generic [JsonEncoder].
JsonEncoder<T> jsonMapEncoder<T>(
  Map<String, dynamic> Function(T value) encoder,
) {
  return encoder;
}
