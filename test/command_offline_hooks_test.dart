import 'package:api_command_queue/api_command_queue.dart';
import 'package:test/test.dart';

class IntHolder {
  final int value;

  const IntHolder(this.value);

  @override
  bool operator ==(Object other) =>
      identical(this, other) || other is IntHolder && other.value == value;

  @override
  int get hashCode => value.hashCode;
}

final class DummyOfflineCommand extends ApiCommand<IntHolder,
    ApiCommandRequest<IntHolder>, IntHolder, DummyOfflineCommand> {
  const DummyOfflineCommand._({
    required super.uuid,
    required super.request,
    required super.lastUpdated,
  }) : super(
          strategy: CommandReplaceStrategy.multiple,
          status: ApiCommandStatus.idle,
          attemptCount: 0,
          firstFailureAt: null,
        );

  factory DummyOfflineCommand.createPending({
    required String id,
    required ApiCommandRequest<IntHolder> request,
  }) {
    return DummyOfflineCommand._(
      uuid: id,
      request: request,
      lastUpdated: DateTime.now(),
    );
  }

  @override
  Future<ApiCommandResponse<IntHolder>?> execute() async {
    return ApiCommandResponse<IntHolder>(request.data, true, status: 200);
  }

  @override
  IntHolder? offlineResult() => IntHolder(request.data.value + 1);

  @override
  IntHolder? offlineRollback() => const IntHolder(-1);

  @override
  IntHolder offlineMerge(IntHolder apiResult) =>
      IntHolder(apiResult.value + 100);

  @override
  DummyOfflineCommand copyWith({
    ApiCommandRequest<IntHolder>? request,
    CommandReplaceStrategy? strategy,
    ApiCommandStatus? status,
    DateTime? lastUpdated,
    int? attemptCount,
    DateTime? firstFailureAt,
    ApiCommandResponse<IntHolder?>? apiResponse,
  }) {
    return DummyOfflineCommand.createPending(
      id: uuid,
      request: request ?? this.request,
    );
  }

  @override
  IntHolder mergePayload(IntHolder update) => update;

  @override
  Object? requestDataToJson(IntHolder requestData) => {
        'value': requestData.value,
      };

  @override
  Object? responseDataToJson(IntHolder? responseData) => {
        'value': responseData?.value,
      };
}

void main() {
  group('Offline hooks on ApiCommand', () {
    final request = ApiCommandRequest(
      ApiCommandRequestMethod.post,
      const IntHolder(10),
    );

    test('offlineResult returns modified value', () {
      final command =
          DummyOfflineCommand.createPending(id: 'x', request: request);
      expect(command.offlineResult(), equals(const IntHolder(11)));
    });

    test('offlineRollback returns sentinel', () {
      final command =
          DummyOfflineCommand.createPending(id: 'x', request: request);
      expect(command.offlineRollback(), equals(const IntHolder(-1)));
    });

    test('offlineMerge augments API response', () {
      final command =
          DummyOfflineCommand.createPending(id: 'x', request: request);
      expect(command.offlineMerge(const IntHolder(5)),
          equals(const IntHolder(105)));
    });
  });
}
