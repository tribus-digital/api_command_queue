import 'dart:async';

import 'package:api_command_queue/api_command_queue.dart';
import 'package:fake_async/fake_async.dart';
import 'package:test/test.dart';

import '../example/main.dart' as example;
import 'test_utils.dart';

final class DebouncedQueue extends ApiCommandQueue<DummyData,
    ApiCommandRequest<DummyData>, DummyData, DummyCommand> {
  DebouncedQueue()
      : super(
          commandFromJson: DummyCommand.fromJson,
          defaultDebounce: const Duration(milliseconds: 100),
        );
}

enum _ControlledOutcome { success, failure, exception }

final class _ControlledCommand extends ApiCommand<DummyData,
    ApiCommandRequest<DummyData>, DummyData, _ControlledCommand> {
  const _ControlledCommand._({
    required super.uuid,
    required super.request,
    required super.strategy,
    required super.status,
    required super.attemptCount,
    required super.firstFailureAt,
    required super.lastUpdated,
    super.apiResponse,
    required this.outcome,
    required this.barrier,
  });

  final _ControlledOutcome outcome;
  final Completer<void> barrier;

  factory _ControlledCommand.create({
    required String id,
    required _ControlledOutcome outcome,
    required Completer<void> barrier,
  }) {
    return _ControlledCommand._(
      uuid: id,
      request: ApiCommandRequest(
        ApiCommandRequestMethod.post,
        const DummyData(1),
      ),
      strategy: CommandReplaceStrategy.multiple,
      status: ApiCommandStatus.idle,
      attemptCount: 0,
      firstFailureAt: null,
      lastUpdated: DateTime.now(),
      outcome: outcome,
      barrier: barrier,
    );
  }

  @override
  Future<ApiCommandResponse<DummyData>?> execute() async {
    await barrier.future;
    return switch (outcome) {
      _ControlledOutcome.success =>
        ApiCommandResponse<DummyData>(request.data, false, status: 200),
      _ControlledOutcome.failure => ApiCommandResponse<DummyData>(
          null,
          false,
          status: 500,
          error: 'forced failure',
        ),
      _ControlledOutcome.exception => throw StateError('boom'),
    };
  }

  @override
  _ControlledCommand copyWith({
    ApiCommandRequest<DummyData>? request,
    CommandReplaceStrategy? strategy,
    ApiCommandStatus? status,
    DateTime? lastUpdated,
    int? attemptCount,
    DateTime? firstFailureAt,
    ApiCommandResponse<DummyData?>? apiResponse,
  }) {
    return _ControlledCommand._(
      uuid: uuid,
      request: request ?? this.request,
      strategy: strategy ?? this.strategy,
      status: status ?? this.status,
      attemptCount: attemptCount ?? this.attemptCount,
      firstFailureAt: firstFailureAt ?? this.firstFailureAt,
      lastUpdated: lastUpdated ?? this.lastUpdated,
      apiResponse: apiResponse ?? this.apiResponse,
      outcome: outcome,
      barrier: barrier,
    );
  }

  @override
  DummyData mergePayload(DummyData update) => update;

  @override
  Object? requestDataToJson(DummyData requestData) => requestData.toJson();

  @override
  Object? responseDataToJson(DummyData? responseData) => responseData?.toJson();
}

final class _ControlledQueue extends ApiCommandQueue<DummyData,
    ApiCommandRequest<DummyData>, DummyData, _ControlledCommand> {
  _ControlledQueue()
      : super(
          commandFromJson: (_) => throw UnimplementedError(),
          retryPolicy: const ExponentialBackoffRetryPolicy(
            maxAttempts: 1,
            initialDelay: Duration.zero,
            backoffFactor: 1,
            maxDelay: Duration.zero,
          ),
        );
}

void main() {
  test('queue close cancels debounce timers', () {
    fakeAsync((async) {
      final queue = DebouncedQueue();

      queue.addCommand(
        DummyCommand.createPending(
          id: 'debounced',
          value: 1,
          strategy: CommandReplaceStrategy.single,
        ),
      );

      queue.close();
      async.elapse(const Duration(milliseconds: 200));

      expect(queue.state.pending, isEmpty);
    });
  });

  test('orchestrator dispose closes queues', () async {
    final queue = DebouncedQueue();
    final orchestrator = ApiCommandOrchestrator(
      commandQueues: {DummyCommand: queue},
    );

    await orchestrator.close();

    expect(orchestrator.isClosed, isTrue);
    expect(queue.isClosed, isTrue);
  });

  test('package example runs successfully', () async {
    await example.runExample();
  });

  for (final outcome in _ControlledOutcome.values) {
    test('queue close is safe during in-flight ${outcome.name}', () async {
      final queue = _ControlledQueue();
      final barrier = Completer<void>();
      final events = <Object>[];
      final subscription = queue.results.listen(events.add);

      queue.addCommand(
        _ControlledCommand.create(
          id: outcome.name,
          outcome: outcome,
          barrier: barrier,
        ),
        processNow: true,
      );
      await Future<void>.delayed(Duration.zero);

      final closeFuture = queue.close();
      barrier.complete();

      await closeFuture;
      await subscription.cancel();

      expect(queue.isClosed, isTrue);
      expect(events, isEmpty);
    });
  }
}
