import 'dart:async';

import 'package:api_command_queue/api_command_queue.dart';
import 'package:test/test.dart';

class DummyValue {
  const DummyValue();
}

final class FakeAnyCmd extends ApiCommand<DummyValue,
    ApiCommandRequest<DummyValue>, DummyValue, FakeAnyCmd> {
  FakeAnyCmd(String id)
      : super(
          uuid: id,
          request: ApiCommandRequest(
            ApiCommandRequestMethod.post,
            DummyValue(),
            {},
          ),
          strategy: CommandReplaceStrategy.multiple,
          status: ApiCommandStatus.idle,
          attemptCount: 0,
          firstFailureAt: null,
          lastUpdated: DateTime.now(),
        );

  @override
  Future<ApiCommandResponse<DummyValue>?> execute() async {
    return ApiCommandResponse<DummyValue>(request.data, false, status: 200);
  }

  @override
  FakeAnyCmd copyWith({
    ApiCommandRequest<DummyValue>? request,
    CommandReplaceStrategy? strategy,
    ApiCommandStatus? status,
    DateTime? lastUpdated,
    int? attemptCount,
    DateTime? firstFailureAt,
    ApiCommandResponse<DummyValue?>? apiResponse,
  }) {
    return FakeAnyCmd(uuid);
  }

  @override
  DummyValue mergePayload(DummyValue update) => update;

  @override
  Object? requestDataToJson(DummyValue requestData) => const {};

  @override
  Object? responseDataToJson(DummyValue? responseData) => null;
}

final class FakeQueue implements AnyApiCommandQueueHandle {
  FakeQueue(this.name);

  final String name;
  final StreamController<
      SyncState<dynamic, ApiCommandRequest<dynamic>, dynamic,
          AnyApiCommand>> _controller = StreamController<
      SyncState<dynamic, ApiCommandRequest<dynamic>, dynamic,
          AnyApiCommand>>.broadcast();

  AnyApiCommand? lastAdded;
  bool lastProcessNow = false;
  int pauseCount = 0;
  int resumeCount = 0;
  int flushCount = 0;
  bool _isClosed = false;
  final SyncState<dynamic, ApiCommandRequest<dynamic>, dynamic, AnyApiCommand>
      _state =
      SyncState<dynamic, ApiCommandRequest<dynamic>, dynamic, AnyApiCommand>(
    pending: {},
    failed: {},
  );

  @override
  int get inFlightCount => 0;

  @override
  bool get isPaused => false;

  @override
  bool get isFlushing => false;

  @override
  bool get isClosed => _isClosed;

  @override
  Stream<ApiCommandResult<AnyApiCommand, dynamic>> get results =>
      const Stream.empty();

  @override
  SyncState<dynamic, ApiCommandRequest<dynamic>, dynamic, AnyApiCommand>
      get state => _state;

  @override
  Stream<SyncState<dynamic, ApiCommandRequest<dynamic>, dynamic, AnyApiCommand>>
      get stream => _controller.stream;

  @override
  void addCommand(
    covariant AnyApiCommand command, {
    bool processNow = false,
    Duration? debounce,
  }) {
    lastAdded = command;
    lastProcessNow = processNow;
  }

  @override
  Future<void> flush() async => flushCount++;

  @override
  void pause() => pauseCount++;

  @override
  void resume({bool flushNow = false}) => resumeCount++;

  @override
  Future<void> close() async {
    _isClosed = true;
    await _controller.close();
  }
}

final class RecordingQueue extends FakeQueue {
  RecordingQueue(super.name, this.recording);

  final List<String> recording;

  @override
  Future<void> flush() async {
    recording.add(name);
  }
}

void main() {
  late FakeQueue queueA;
  late FakeQueue queueB;
  late FakeQueue queueC;
  late ApiCommandOrchestrator orchestrator;

  setUp(() {
    queueA = FakeQueue('A');
    orchestrator = ApiCommandOrchestrator(
      commandQueues: {FakeAnyCmd: queueA},
    );
  });

  test('enqueue sends to the right queue with processNow when enabled', () {
    final command = FakeAnyCmd('foo');

    orchestrator.enqueue(command);

    expect(queueA.lastAdded, same(command));
    expect(queueA.lastProcessNow, isTrue);
  });

  test('enqueue sends with processNow=false when processing is disabled', () {
    orchestrator = ApiCommandOrchestrator(
      commandQueues: {FakeAnyCmd: queueA},
      processingEnabled: false,
    );

    final command = FakeAnyCmd('bar');
    orchestrator.enqueue(command);

    expect(queueA.lastAdded, same(command));
    expect(queueA.lastProcessNow, isFalse);
  });

  test('flushAll unlimited invokes flush on every queue', () async {
    queueB = FakeQueue('B');
    orchestrator = ApiCommandOrchestrator(
      commandQueues: {
        FakeAnyCmd: queueA,
        String: queueB,
      },
    );

    await orchestrator.flushAll();

    expect(queueA.flushCount, equals(1));
    expect(queueB.flushCount, equals(1));
  });

  test('flushAll with limit splits into ordered batches', () async {
    final callOrder = <String>[];
    queueA = RecordingQueue('A', callOrder);
    queueB = RecordingQueue('B', callOrder);
    queueC = RecordingQueue('C', callOrder);
    orchestrator = ApiCommandOrchestrator(
      commandQueues: {
        FakeAnyCmd: queueA,
        String: queueB,
        int: queueC,
      },
      flushConcurrency: 2,
    );

    await orchestrator.flushAll();

    expect(callOrder, equals(['A', 'B', 'C']));
  });

  test('setProcessingEnabled pauses and then resumes plus flushes', () async {
    orchestrator.setProcessingEnabled(false);
    expect(queueA.pauseCount, equals(1));
    expect(queueA.resumeCount, equals(0));
    expect(queueA.flushCount, equals(0));

    orchestrator.setProcessingEnabled(true);
    await Future<void>.delayed(Duration.zero);

    expect(queueA.pauseCount, equals(1));
    expect(queueA.resumeCount, equals(1));
    expect(queueA.flushCount, equals(1));
  });

  test('flushAll with flushConcurrency=0 behaves like unlimited', () async {
    final callOrder = <String>[];
    queueA = RecordingQueue('A', callOrder);
    queueB = RecordingQueue('B', callOrder);
    queueC = RecordingQueue('C', callOrder);
    orchestrator = ApiCommandOrchestrator(
      commandQueues: {
        FakeAnyCmd: queueA,
        String: queueB,
        int: queueC,
      },
      flushConcurrency: 0,
    );

    await orchestrator.flushAll();

    expect(callOrder.toSet(), equals({'A', 'B', 'C'}));
    expect(callOrder.length, equals(3));
  });

  test('flushAll respects explicit queueOrder parameter', () async {
    final callOrder = <String>[];
    queueA = RecordingQueue('A', callOrder);
    queueB = RecordingQueue('B', callOrder);
    queueC = RecordingQueue('C', callOrder);
    orchestrator = ApiCommandOrchestrator(
      commandQueues: {
        FakeAnyCmd: queueA,
        String: queueB,
        int: queueC,
      },
      queueOrder: [int, FakeAnyCmd],
    );

    await orchestrator.flushAll();

    expect(callOrder, equals(['C', 'A', 'B']));
  });
}
