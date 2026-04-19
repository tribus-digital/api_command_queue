import 'dart:async';

import 'package:api_command_queue/api_command_queue.dart';

Future<void> main() async {
  await runExample();
}

Future<void> runExample() async {
  final queue = ExampleQueue();
  final orchestrator = ApiCommandOrchestrator(
    commandQueues: {ExampleCommand: queue},
    processingEnabled: false,
  );

  final optimistic =
      orchestrator.enqueue(ExampleCommand.create('cmd-1', 'Publish package'));
  print('Optimistic title: ${optimistic?.title}');

  orchestrator.setProcessingEnabled(true);

  final result = await queue.results.first.timeout(const Duration(seconds: 1));
  print(
    'Completed ${result.command.uuid} with status '
    '${result.response.status} success=${result.success}',
  );

  await orchestrator.close();
}

final class ExamplePayload {
  const ExamplePayload(this.title);

  final String title;

  Map<String, dynamic> toJson() => {'title': title};

  static ExamplePayload fromJson(Object? json) {
    final map = (json as Map).cast<String, dynamic>();
    return ExamplePayload(map['title'] as String);
  }
}

final class ExampleCommand extends ApiCommand<ExamplePayload,
    ApiCommandRequest<ExamplePayload>, ExamplePayload, ExampleCommand> {
  const ExampleCommand._({
    required super.uuid,
    required super.request,
    required super.strategy,
    required super.status,
    required super.attemptCount,
    required super.firstFailureAt,
    required super.lastUpdated,
    super.apiResponse,
  });

  factory ExampleCommand.create(String id, String title) {
    return ExampleCommand._(
      uuid: id,
      request: ApiCommandRequest(
        ApiCommandRequestMethod.post,
        ExamplePayload(title),
      ),
      strategy: CommandReplaceStrategy.multiple,
      status: ApiCommandStatus.idle,
      attemptCount: 0,
      firstFailureAt: null,
      lastUpdated: DateTime.now(),
    );
  }

  @override
  Future<ApiCommandResponse<ExamplePayload>?> execute() async {
    await Future<void>.delayed(const Duration(milliseconds: 10));
    return ApiCommandResponse<ExamplePayload>(request.data, false, status: 201);
  }

  @override
  ExamplePayload? offlineResult() => request.data;

  @override
  ExamplePayload mergePayload(ExamplePayload update) => update;

  @override
  ExampleCommand copyWith({
    ApiCommandRequest<ExamplePayload>? request,
    CommandReplaceStrategy? strategy,
    ApiCommandStatus? status,
    DateTime? lastUpdated,
    int? attemptCount,
    DateTime? firstFailureAt,
    ApiCommandResponse<ExamplePayload?>? apiResponse,
  }) {
    return ExampleCommand._(
      uuid: uuid,
      request: request ?? this.request,
      strategy: strategy ?? this.strategy,
      status: status ?? this.status,
      attemptCount: attemptCount ?? this.attemptCount,
      firstFailureAt: firstFailureAt ?? this.firstFailureAt,
      lastUpdated: lastUpdated ?? this.lastUpdated,
      apiResponse: apiResponse ?? this.apiResponse,
    );
  }

  @override
  Object? requestDataToJson(ExamplePayload requestData) => requestData.toJson();

  @override
  Object? responseDataToJson(ExamplePayload? responseData) =>
      responseData?.toJson();

  static ExampleCommand fromJson(Map<String, dynamic> json) {
    return ExampleCommand._(
      uuid: json['id'] as String,
      request: ApiCommandRequest.fromJson(
        (json['request'] as Map).cast<String, dynamic>(),
        ExamplePayload.fromJson,
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
              ExamplePayload.fromJson,
            ),
    );
  }
}

final class ExampleQueue extends ApiCommandQueue<ExamplePayload,
    ApiCommandRequest<ExamplePayload>, ExamplePayload, ExampleCommand> {
  ExampleQueue()
      : super(
          commandFromJson: ExampleCommand.fromJson,
          retryPolicy: const ExponentialBackoffRetryPolicy(
            maxAttempts: 1,
            initialDelay: Duration.zero,
            backoffFactor: 1,
            maxDelay: Duration.zero,
          ),
        );
}
