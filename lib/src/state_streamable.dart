/// Minimal reactive state interface used by the core queue and orchestrator.
abstract interface class StateStreamable<State> {
  /// The current in-memory state snapshot.
  State get state;

  /// Emits subsequent state changes.
  Stream<State> get stream;

  /// Whether the object has been closed.
  bool get isClosed;

  /// Releases resources and closes any internal streams.
  Future<void> close();
}
