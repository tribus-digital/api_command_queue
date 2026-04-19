import 'dart:async';

import 'package:api_command_queue/api_command_queue.dart';
import 'package:test/test.dart';

import 'test_utils.dart';

final List<String> testEvents = [];

final class FakeDelayedCommand extends ApiCommand<DummyData,
    ApiCommandRequest<DummyData>, DummyData, FakeDelayedCommand> {
  final bool willSucceed;
  final Duration delay;

  const FakeDelayedCommand({
    required super.uuid,
    required super.request,
    required super.strategy,
    required super.status,
    required super.attemptCount,
    required super.lastUpdated,
    super.firstFailureAt,
    super.apiResponse,
    this.willSucceed = true,
    this.delay = const Duration(milliseconds: 50),
  });

  factory FakeDelayedCommand.create({
    required String id,
    required int value,
    bool willSucceed = true,
    Duration delay = const Duration(milliseconds: 50),
  }) {
    return FakeDelayedCommand(
      uuid: id,
      request:
          ApiCommandRequest(ApiCommandRequestMethod.post, DummyData(value), {}),
      strategy: CommandReplaceStrategy.multiple,
      status: ApiCommandStatus.idle,
      attemptCount: 0,
      lastUpdated: DateTime.now(),
      willSucceed: willSucceed,
      delay: delay,
    );
  }

  @override
  Future<ApiCommandResponse<DummyData>> execute() async {
    final start = DateTime.now().millisecondsSinceEpoch;
    testEvents.add('$uuid-start-$start');
    await Future<void>.delayed(delay);
    final end = DateTime.now().millisecondsSinceEpoch;
    testEvents.add('$uuid-end-$end');

    return willSucceed
        ? ApiCommandResponse<DummyData>(request.data, false, status: 200)
        : ApiCommandResponse<DummyData>(
            null,
            false,
            status: 500,
            error: 'fail',
          );
  }

  @override
  FakeDelayedCommand copyWith({
    ApiCommandRequest<DummyData>? request,
    CommandReplaceStrategy? strategy,
    ApiCommandStatus? status,
    DateTime? lastUpdated,
    int? attemptCount,
    DateTime? firstFailureAt,
    ApiCommandResponse<DummyData?>? apiResponse,
  }) {
    return FakeDelayedCommand(
      uuid: uuid,
      request: request ?? this.request,
      strategy: strategy ?? this.strategy,
      status: status ?? this.status,
      attemptCount: attemptCount ?? this.attemptCount,
      lastUpdated: lastUpdated ?? this.lastUpdated,
      firstFailureAt: firstFailureAt ?? this.firstFailureAt,
      apiResponse: apiResponse ?? this.apiResponse,
      willSucceed: willSucceed,
      delay: delay,
    );
  }

  static FakeDelayedCommand fromJson(Map<String, dynamic> _) =>
      throw UnimplementedError();

  @override
  DummyData mergePayload(DummyData update) => update;

  @override
  Object? requestDataToJson(DummyData requestData) => requestData.toJson();

  @override
  Object? responseDataToJson(DummyData? responseData) => responseData?.toJson();
}

final class TestQueueFake extends ApiCommandQueue<DummyData,
    ApiCommandRequest<DummyData>, DummyData, FakeDelayedCommand> {
  TestQueueFake({
    required super.commandFromJson,
    super.retryPolicy,
    super.failedCapacity,
    super.concurrencyLimit,
  });
}

void main() {
  setUp(() {
    testEvents.clear();
  });

  group('concurrencyLimit behavior', () {
    test('strict FIFO with concurrencyLimit=1 processes A then B', () async {
      final queue = TestQueueFake(
        commandFromJson: FakeDelayedCommand.fromJson,
        retryPolicy: const ExponentialBackoffRetryPolicy(
          maxAttempts: 1,
          initialDelay: Duration.zero,
          backoffFactor: 1.0,
          maxDelay: Duration.zero,
          maxAge: Duration(hours: 1),
        ),
        concurrencyLimit: 1,
      );

      queue.addCommand(FakeDelayedCommand.create(
          id: 'A', value: 1, delay: const Duration(milliseconds: 50)));
      queue.addCommand(FakeDelayedCommand.create(
          id: 'B', value: 2, delay: const Duration(milliseconds: 50)));

      await queue.flush();

      expect(testEvents.length, equals(4));
      expect(testEvents[0].startsWith('A-start'), isTrue);
      expect(testEvents[1].startsWith('A-end'), isTrue);
      expect(testEvents[2].startsWith('B-start'), isTrue);
      expect(testEvents[3].startsWith('B-end'), isTrue);
    });

    test('parallel up to concurrencyLimit=2 starts B before A ends', () async {
      final queue = TestQueueFake(
        commandFromJson: FakeDelayedCommand.fromJson,
        retryPolicy: const ExponentialBackoffRetryPolicy(
          maxAttempts: 1,
          initialDelay: Duration.zero,
          backoffFactor: 1.0,
          maxDelay: Duration.zero,
          maxAge: Duration(hours: 1),
        ),
        concurrencyLimit: 2,
      );

      queue.addCommand(FakeDelayedCommand.create(
          id: 'A', value: 1, delay: const Duration(milliseconds: 100)));
      queue.addCommand(FakeDelayedCommand.create(
          id: 'B', value: 2, delay: const Duration(milliseconds: 100)));

      await queue.flush();

      final aStartIdx =
          testEvents.indexWhere((event) => event.startsWith('A-start'));
      final bStartIdx =
          testEvents.indexWhere((event) => event.startsWith('B-start'));
      final aEndIdx =
          testEvents.indexWhere((event) => event.startsWith('A-end'));

      expect(testEvents.length, equals(4));
      expect(aStartIdx, lessThan(bStartIdx));
      expect(bStartIdx, lessThan(aEndIdx));
    });

    test('flush waits for all concurrent work before completing', () async {
      final queue = TestQueueFake(
        commandFromJson: FakeDelayedCommand.fromJson,
        retryPolicy: const ExponentialBackoffRetryPolicy(
          maxAttempts: 1,
          initialDelay: Duration.zero,
          backoffFactor: 1.0,
          maxDelay: Duration.zero,
          maxAge: Duration(hours: 1),
        ),
        concurrencyLimit: 2,
      );

      queue.addCommand(FakeDelayedCommand.create(id: 'A', value: 1));
      queue.addCommand(FakeDelayedCommand.create(id: 'B', value: 2));
      final results = <ApiCommandResult<FakeDelayedCommand, DummyData>>[];
      final subscription = queue.results.listen(results.add);

      await queue.flush();
      await Future<void>.delayed(Duration.zero);

      expect(testEvents.where((event) => event.contains('-end-')).length, 2);
      expect(results.length, 2);
      expect(queue.state.flushStatus, QueueFlushStatus.idle);

      await subscription.cancel();
    });
  });
}
