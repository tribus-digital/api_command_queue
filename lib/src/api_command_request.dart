import 'json_codec.dart';

/// Supported HTTP-like verbs for queued requests.
enum ApiCommandRequestMethod { post, put, patch, delete }

/// Serialized request data stored on a queued command.
final class ApiCommandRequest<Payload> {
  /// Request payload.
  final Payload data;

  /// Optional named parameters such as route ids or query values.
  final Map<String, Object?> parameters;

  /// Request verb associated with the payload.
  final ApiCommandRequestMethod method;

  ApiCommandRequest(
    this.method,
    this.data, [
    Map<String, Object?> parameters = const {},
  ]) : parameters = Map.unmodifiable(Map<String, Object?>.from(parameters));

  T getNamedParameter<T>(String name) {
    if (!parameters.containsKey(name)) {
      throw StateError('Missing request parameter "$name".');
    }

    final parameter = parameters[name];
    if (parameter is! T) {
      throw StateError(
        'Request parameter "$name" has type '
        '${parameter.runtimeType}, expected $T.',
      );
    }
    return parameter;
  }

  T? getNullableNamedParameter<T>(String name) {
    if (!parameters.containsKey(name)) {
      return null;
    }

    final parameter = parameters[name];
    if (parameter == null) {
      return null;
    }
    if (parameter is! T) {
      throw StateError(
        'Request parameter "$name" has type '
        '${parameter.runtimeType}, expected $T.',
      );
    }
    return parameter as T;
  }

  ApiCommandRequest<Payload> copyWith({
    Payload? data,
    Map<String, Object?>? parameters,
  }) {
    return ApiCommandRequest<Payload>(
      method,
      data ?? this.data,
      parameters ?? this.parameters,
    );
  }

  /// Serializes the request payload and parameters.
  Map<String, dynamic> toJson(JsonEncoder<Payload> encodePayload) => {
        'method': method.name.toLowerCase(),
        'data': encodePayload(data),
        'parameters': _validateJsonObject(parameters, 'parameters'),
      };

  /// Restores a request from JSON state.
  static ApiCommandRequest<T> fromJson<T>(
    Map<String, dynamic> json,
    JsonDecoder<T> decodePayload,
  ) {
    return ApiCommandRequest<T>(
      ApiCommandRequestMethod.values.firstWhere(
        (value) => value.name.toLowerCase() == json['method'],
      ),
      decodePayload(json['data']),
      json['parameters'] == null
          ? const {}
          : (json['parameters'] as Map).cast<String, Object?>(),
    );
  }
}

Map<String, Object?> _validateJsonObject(
  Map<String, Object?> source,
  String path,
) {
  final validated = <String, Object?>{};
  for (final entry in source.entries) {
    validated[entry.key] =
        _validateJsonValue(entry.value, '$path.${entry.key}');
  }
  return Map<String, Object?>.unmodifiable(validated);
}

Object? _validateJsonValue(Object? value, String path) {
  switch (value) {
    case null:
    case String():
    case num():
    case bool():
      return value;
    case List<Object?>():
      return List<Object?>.unmodifiable([
        for (var index = 0; index < value.length; index += 1)
          _validateJsonValue(value[index], '$path[$index]'),
      ]);
    case Map<Object?, Object?>():
      final validated = <String, Object?>{};
      for (final entry in value.entries) {
        final key = entry.key;
        if (key is! String) {
          throw ArgumentError.value(
            key,
            path,
            'Request parameters must use string map keys.',
          );
        }
        validated[key] = _validateJsonValue(entry.value, '$path.$key');
      }
      return Map<String, Object?>.unmodifiable(validated);
    default:
      throw ArgumentError.value(
        value,
        path,
        'Request parameters must be JSON-safe values.',
      );
  }
}
