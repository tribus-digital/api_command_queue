import 'package:api_command_queue/api_command_queue.dart';
import 'package:test/test.dart';

import 'test_utils.dart';

final class LoggingCommand extends ApiCommand<DummyData,
    ApiCommandRequest<DummyData>, DummyData, LoggingCommand> {
  const LoggingCommand._({
    required super.uuid,
    required super.request,
    required super.strategy,
    required super.status,
    required super.attemptCount,
    required super.firstFailureAt,
    required super.lastUpdated,
    super.apiResponse,
    required this.throwOnExecute,
  });

  final bool throwOnExecute;

  factory LoggingCommand.create({
    required String id,
    required bool throwOnExecute,
  }) {
    return LoggingCommand._(
      uuid: id,
      request: ApiCommandRequest(
        ApiCommandRequestMethod.post,
        const DummyData(1),
        const {},
      ),
      strategy: CommandReplaceStrategy.multiple,
      status: ApiCommandStatus.idle,
      attemptCount: 0,
      firstFailureAt: null,
      lastUpdated: DateTime.now(),
      throwOnExecute: throwOnExecute,
    );
  }

  @override
  Future<ApiCommandResponse<DummyData>?> execute() async {
    if (throwOnExecute) {
      throw StateError('boom');
    }
    return ApiCommandResponse<DummyData>(request.data, false, status: 200);
  }

  @override
  LoggingCommand copyWith({
    ApiCommandRequest<DummyData>? request,
    CommandReplaceStrategy? strategy,
    ApiCommandStatus? status,
    DateTime? lastUpdated,
    int? attemptCount,
    DateTime? firstFailureAt,
    ApiCommandResponse<DummyData?>? apiResponse,
  }) {
    return LoggingCommand._(
      uuid: uuid,
      request: request ?? this.request,
      strategy: strategy ?? this.strategy,
      status: status ?? this.status,
      attemptCount: attemptCount ?? this.attemptCount,
      firstFailureAt: firstFailureAt ?? this.firstFailureAt,
      lastUpdated: lastUpdated ?? this.lastUpdated,
      apiResponse: apiResponse ?? this.apiResponse,
      throwOnExecute: throwOnExecute,
    );
  }

  @override
  DummyData mergePayload(DummyData update) => update;

  @override
  Object? requestDataToJson(DummyData requestData) {
    return requestData.toJson();
  }

  @override
  Object? responseDataToJson(DummyData? responseData) {
    return responseData?.toJson();
  }

  static LoggingCommand fromJson(Map<String, dynamic> json) {
    return LoggingCommand._(
      uuid: json['id'] as String,
      request: ApiCommandRequest.fromJson(
        (json['request'] as Map).cast<String, dynamic>(),
        DummyData.fromJson,
      ),
      strategy: CommandReplaceStrategy.values.firstWhere(
        (value) => value.name == json['strategy'] as String,
      ),
      status: ApiCommandStatus.values.firstWhere(
        (value) => value.name == json['status'] as String,
      ),
      attemptCount: json['attemptCount'] as int,
      firstFailureAt: json['firstFailureAt'] == null
          ? null
          : DateTime.parse(json['firstFailureAt'] as String),
      lastUpdated: DateTime.parse(json['lastUpdated'] as String),
      apiResponse: json['apiResponse'] == null
          ? null
          : ApiCommandResponse.fromJson(
              (json['apiResponse'] as Map).cast<String, dynamic>(),
              DummyData.fromJson,
            ),
      throwOnExecute: false,
    );
  }
}

final class LoggingQueue extends ApiCommandQueue<DummyData,
    ApiCommandRequest<DummyData>, DummyData, LoggingCommand> {
  LoggingQueue()
      : super(
          commandFromJson: LoggingCommand.fromJson,
          retryPolicy: const ExponentialBackoffRetryPolicy(
            maxAttempts: 1,
            initialDelay: Duration.zero,
            backoffFactor: 1,
            maxDelay: Duration.zero,
          ),
        );
}

void main() {
  setUp(() {
    resetApiCommandQueueLogger();
  });

  tearDown(resetApiCommandQueueLogger);

  test('default logger is silent', () async {
    final queue = LoggingQueue();

    queue.addCommand(
      LoggingCommand.create(id: 'ok', throwOnExecute: false),
      processNow: true,
    );

    final result = await queue.results.first;
    expect(result.success, isTrue);
  });

  test('custom logger receives debug and error events', () async {
    final debugMessages = <String>[];
    final errorMessages = <String>[];
    final loggedErrors = <Object?>[];

    configureApiCommandQueueLogger(
      debug: debugMessages.add,
      error: (message, {error, stackTrace}) {
        errorMessages.add(message);
        loggedErrors.add(error);
      },
    );

    final queue = LoggingQueue();
    queue.addCommand(
      LoggingCommand.create(id: 'err', throwOnExecute: true),
      processNow: true,
    );

    final result = await queue.results.first;

    expect(result.success, isFalse);
    expect(debugMessages, isNotEmpty);
    expect(errorMessages, isNotEmpty);
    expect(loggedErrors.single, isA<StateError>());
  });
}
