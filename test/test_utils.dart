import 'package:api_command_queue/api_command_queue.dart';
import 'package:clock/clock.dart';

class DummyData {
  final int value;

  const DummyData(this.value);

  Map<String, dynamic> toJson() => {'value': value};

  static DummyData fromJson(Object? json) {
    final map = (json as Map).cast<String, dynamic>();
    return DummyData(map['value'] as int);
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) || other is DummyData && other.value == value;

  @override
  int get hashCode => value.hashCode;
}

class DummyCommand2 extends DummyCommand {
  const DummyCommand2._({
    required super.uuid,
    required super.request,
    required super.strategy,
    required super.status,
    required super.attemptCount,
    required super.lastUpdated,
    super.willSucceed,
  }) : super._();

  factory DummyCommand2.createPending({
    required String id,
    required int value,
    CommandReplaceStrategy strategy = CommandReplaceStrategy.multiple,
    bool willSucceed = true,
  }) {
    return DummyCommand2._(
      uuid: id,
      request: ApiCommandRequest(
        ApiCommandRequestMethod.post,
        DummyData(value),
        const {},
      ),
      strategy: strategy,
      status: ApiCommandStatus.idle,
      attemptCount: 0,
      lastUpdated: clock.now(),
      willSucceed: willSucceed,
    );
  }
}

class DummyCommand extends ApiCommand<DummyData, ApiCommandRequest<DummyData>,
    DummyData, DummyCommand> {
  final bool willSucceed;
  final Duration executeDelay;
  final ApiCommandResponse<DummyData>? failureResponse;
  final ApiCommandTerminalFailurePredicate<DummyData>? terminalFailurePredicate;

  const DummyCommand._({
    required super.uuid,
    required super.request,
    required super.strategy,
    required super.status,
    required super.attemptCount,
    required super.lastUpdated,
    super.firstFailureAt,
    super.apiResponse,
    this.willSucceed = true,
    this.executeDelay = Duration.zero,
    this.failureResponse,
    this.terminalFailurePredicate,
  });

  factory DummyCommand.createPending({
    required String id,
    required int value,
    CommandReplaceStrategy strategy = CommandReplaceStrategy.multiple,
    bool willSucceed = true,
    Duration executeDelay = Duration.zero,
    ApiCommandResponse<DummyData>? failureResponse,
    ApiCommandTerminalFailurePredicate<DummyData>? terminalFailurePredicate,
  }) {
    return DummyCommand._(
      uuid: id,
      request: ApiCommandRequest(
        ApiCommandRequestMethod.post,
        DummyData(value),
        const {},
      ),
      strategy: strategy,
      status: ApiCommandStatus.idle,
      attemptCount: 0,
      lastUpdated: clock.now(),
      willSucceed: willSucceed,
      executeDelay: executeDelay,
      failureResponse: failureResponse,
      terminalFailurePredicate: terminalFailurePredicate,
    );
  }

  @override
  Future<ApiCommandResponse<DummyData>> execute() async {
    /// lets a test ask what the queue reports while a command is actually in
    /// flight, rather than only before and after
    if (executeDelay > Duration.zero) {
      await Future<void>.delayed(executeDelay);
    }

    if (willSucceed) {
      return ApiCommandResponse<DummyData>(request.data, false, status: 200);
    }

    return failureResponse ??
        ApiCommandResponse<DummyData>(
          null,
          false,
          status: 500,
          error: 'forced failure',
        );
  }

  @override
  bool isTerminalFailure(ApiCommandResponse<DummyData?> response) {
    return terminalFailurePredicate?.call(response) ?? false;
  }

  @override
  DummyCommand copyWith({
    ApiCommandRequest<DummyData>? request,
    CommandReplaceStrategy? strategy,
    ApiCommandStatus? status,
    DateTime? lastUpdated,
    int? attemptCount,
    DateTime? firstFailureAt,
    ApiCommandResponse<DummyData?>? apiResponse,
  }) {
    return DummyCommand._(
      uuid: uuid,
      request: request ?? this.request,
      strategy: strategy ?? this.strategy,
      status: status ?? this.status,
      attemptCount: attemptCount ?? this.attemptCount,
      lastUpdated: lastUpdated ?? this.lastUpdated,
      firstFailureAt: firstFailureAt ?? this.firstFailureAt,
      apiResponse: apiResponse ?? this.apiResponse,
      willSucceed: willSucceed,
      executeDelay: executeDelay,
      failureResponse: failureResponse,
      terminalFailurePredicate: terminalFailurePredicate,
    );
  }

  @override
  Object? requestDataToJson(DummyData requestData) => requestData.toJson();

  @override
  Object? responseDataToJson(DummyData? responseData) => responseData?.toJson();

  static DummyCommand fromJson(Map<String, dynamic> json) {
    final request = ApiCommandRequest.fromJson<DummyData>(
      (json['request'] as Map).cast<String, dynamic>(),
      DummyData.fromJson,
    );
    final apiResponseJson = json['apiResponse'] as Map<String, dynamic>?;
    final apiResponse = apiResponseJson == null
        ? null
        : ApiCommandResponse.fromJson<DummyData>(
            apiResponseJson,
            DummyData.fromJson,
          );

    return DummyCommand._(
      uuid: json['id'] as String,
      request: request,
      strategy: CommandReplaceStrategy.values.firstWhere(
        (value) => value.name == json['strategy'] as String,
      ),
      status: ApiCommandStatus.values.firstWhere(
        (value) => value.name == json['status'] as String,
      ),
      lastUpdated: DateTime.parse(json['lastUpdated'] as String),
      attemptCount: json['attemptCount'] as int,
      firstFailureAt: json['firstFailureAt'] != null
          ? DateTime.parse(json['firstFailureAt'] as String)
          : null,
      apiResponse: apiResponse,
    );
  }

  @override
  DummyData mergePayload(DummyData update) => update;
}
