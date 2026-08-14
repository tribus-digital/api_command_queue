# api_command_queue

`api_command_queue` is a pure Dart package for queueing retryable API commands with explicit serialization, trailing-edge debounce for single-replacement commands, and a framework-agnostic orchestrator.

It is a good fit when you want:

- retry and backoff for failed API work
- early exit for non-retryable response failures
- optional queue-level concurrency limits
- app-controlled pause/resume behavior
- optimistic hooks that stay in application code
- explicit JSON codecs without reflection

It does not include:

- HTTP clients or repository implementations
- built-in persistence or hydration
- auth or connectivity observers
- framework-specific state management wrappers

## Install

```yaml
dependencies:
  api_command_queue: ^0.2.0
```

## Minimal Example

```dart
import 'package:api_command_queue/api_command_queue.dart';

final class TodoPayload {
  const TodoPayload(this.title);

  final String title;

  Map<String, dynamic> toJson() => {'title': title};

  static TodoPayload fromJson(Object? json) {
    final map = (json as Map).cast<String, dynamic>();
    return TodoPayload(map['title'] as String);
  }
}

final class CreateTodoCommand extends ApiCommand<TodoPayload,
    ApiCommandRequest<TodoPayload>, TodoPayload, CreateTodoCommand> {
  const CreateTodoCommand({
    required super.uuid,
    required super.request,
    required super.strategy,
    required super.status,
    required super.attemptCount,
    required super.firstFailureAt,
    required super.lastUpdated,
    super.apiResponse,
  });

  factory CreateTodoCommand.create(String title) {
    return CreateTodoCommand(
      uuid: ApiCommand.generateId(),
      request: ApiCommandRequest(
        ApiCommandRequestMethod.post,
        TodoPayload(title),
      ),
      strategy: CommandReplaceStrategy.multiple,
      status: ApiCommandStatus.idle,
      attemptCount: 0,
      firstFailureAt: null,
      lastUpdated: DateTime.now(),
    );
  }

  @override
  Future<ApiCommandResponse<TodoPayload>?> execute() async {
    return ApiCommandResponse(request.data, false, status: 201);
  }

  @override
  TodoPayload? offlineResult() => request.data;

  @override
  TodoPayload mergePayload(TodoPayload update) => update;

  @override
  CreateTodoCommand copyWith({
    ApiCommandRequest<TodoPayload>? request,
    CommandReplaceStrategy? strategy,
    ApiCommandStatus? status,
    DateTime? lastUpdated,
    int? attemptCount,
    DateTime? firstFailureAt,
    ApiCommandResponse<TodoPayload?>? apiResponse,
  }) {
    return CreateTodoCommand(
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
  Object? requestDataToJson(TodoPayload requestData) => requestData.toJson();

  @override
  Object? responseDataToJson(TodoPayload? responseData) =>
      responseData?.toJson();

  static CreateTodoCommand fromJson(Map<String, dynamic> json) {
    return CreateTodoCommand(
      uuid: json['id'] as String,
      request: ApiCommandRequest.fromJson(
        (json['request'] as Map).cast<String, dynamic>(),
        TodoPayload.fromJson,
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
              TodoPayload.fromJson,
            ),
    );
  }
}

final class TodoQueue extends ApiCommandQueue<TodoPayload,
    ApiCommandRequest<TodoPayload>, TodoPayload, CreateTodoCommand> {
  TodoQueue() : super(commandFromJson: CreateTodoCommand.fromJson);
}

Future<void> main() async {
  final queue = TodoQueue();
  final orchestrator = ApiCommandOrchestrator(
    commandQueues: {CreateTodoCommand: queue},
  );

  final optimistic = orchestrator.enqueue(CreateTodoCommand.create('Ship docs'));
  print('Optimistic title: ${optimistic?.title}');

  final result = await queue.results.first;
  print('Completed ${result.command.uuid} success=${result.success}');

  await orchestrator.close();
}
```

The runnable package example lives at [`example/main.dart`](example/main.dart).

## Serialization Contract

- `ApiCommandRequest.toJson(encodePayload)`
- `ApiCommandRequest.fromJson(..., decodePayload)`
- `ApiCommandResponse.toJson(encodeData)`
- `ApiCommandResponse.fromJson(..., decodeData)`

`ApiCommand` subclasses provide request and response payload encoders through `requestDataToJson(...)` and `responseDataToJson(...)`.

The package does not use reflection. Applications own payload decoding, command restoration, and any persistence layer built on top of the queue state.

## Request Parameters

`ApiCommandRequest.parameters` is an immutable map. Values must be JSON-safe when serialized:

- `null`
- `bool`
- `num`
- `String`
- `List`
- `Map<String, Object?>`

Required named parameters use `getNamedParameter<T>()`, which throws a `StateError` for missing or wrong-typed values.

## Optimistic Hooks

`ApiCommand` keeps optimistic behavior in app code:

- `offlineResult()` returns the immediate optimistic value after enqueue
- `offlineRollback()` returns the compensating value when the command ultimately fails
- `offlineMerge(apiResult)` lets the app merge a successful API payload back into local state

The package stores and executes commands. It does not mutate your application state for you.

## Processing Control

`ApiCommandOrchestrator` is framework-agnostic. External code can decide when queues should run:

- `pauseAll()`
- `resumeAll()`
- `setProcessingEnabled(bool enabled)`
- `flushAll()`

Single-replacement commands support true trailing-edge debounce. Every new enqueue resets the timer, and only the latest command is enqueued when the interval expires.

## Retry Scheduling

`flush()` processes the commands that are currently due and returns. It does not
wait out a failed command's backoff — one that fails is left pending and becomes
eligible again once its delay has passed, so a single failing command cannot hold
a flush, or a queue, open for the length of its retry ladder.

The consequence is that flushes have to happen often enough to pick commands up
as they come due. Connectivity changes, app resume, and user-triggered syncs are
usually enough. For consumers that would rather schedule explicitly:

- `nextAttemptAt(command)` — when a given command is next eligible
- `nextDueAt` — the earliest due time across the queue, or null if nothing is
  pending

Nothing retries while no flush is running. If the consumer's own triggers are
not enough — a device sitting idle with a failed command will not retry at all —
`ApiCommandOrchestrator(autoFlushWhenDue: true)` keeps a single timer set to the
earliest `nextDueAt` and flushes when it fires. It is cancelled while processing
is disabled and on close.

## Terminal Failures

Terminal failures let a queue stop retrying responses that are known not to
recover, such as validation errors, permission failures, duplicate writes, or
domain-level rejection states. Matching commands move straight to the failed
dead-letter collection and emit a final result after the current attempt.

Use a queue-wide predicate when the rule applies to every command in the queue:

```dart
final class TodoQueue extends ApiCommandQueue<TodoPayload,
    ApiCommandRequest<TodoPayload>, TodoPayload, CreateTodoCommand> {
  TodoQueue()
      : super(
          commandFromJson: CreateTodoCommand.fromJson,
          terminalFailurePredicate:
              const ApiCommandTerminalFailureRule<TodoPayload>(
            statusCodes: {400, 401, 403, 409, 422},
          ).matches,
        );
}
```

Use `ApiCommand.isTerminalFailure(...)` for command-specific rules:

```dart
final _validationRule = ApiCommandTerminalFailureRule<TodoPayload>(
  statusCodes: const {422},
  dataMatches: (data) => data?.title == 'validation_rejected',
);

@override
bool isTerminalFailure(ApiCommandResponse<TodoPayload?> response) {
  return _validationRule.matches(response);
}
```

`ApiCommandTerminalFailureRule` can match exact status codes, status predicates,
response payload predicates, error predicates, or the full response. Successful
`2xx` responses never match terminal-failure rules.

## Logging

Logging is disabled by default. If you want queue diagnostics, configure a logger:

```dart
configureApiCommandQueueLogger(
  debug: (message) => print(message),
  error: (message, {error, stackTrace}) {
    print('$message: $error');
  },
);
```
