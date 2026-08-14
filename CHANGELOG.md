# Changelog

## 0.3.0

- Retry backoff is now scheduled rather than slept. `flush()` processes the
  commands that are due and returns; a command that fails waits for its backoff
  between flushes instead of inside one.

  Previously the delay was awaited inside `_processCommand` while the flush loop
  kept re-selecting the same command, so a single failing command held its queue
  for its entire retry ladder — minutes under the default policy — and, for
  consumers that flush queues in sequence, held every queue behind it too.

  Retry pacing is unchanged: the same policy produces the same intervals. What
  changes is that the caller is no longer blocked across them, so flushes need
  to be driven often enough to pick commands up as they come due. Connectivity
  changes, app resume, and user-triggered syncs are usually enough; `nextDueAt`
  is there for consumers that would rather schedule a timer.

- Added `ApiCommandQueue.nextAttemptAt` and `nextDueAt`, so a consumer can tell
  when a command is next eligible and when to flush again.

- Time is now read through `package:clock`, so retry scheduling can be driven by
  `fake_async` in tests. No behavioural change outside tests.

- `SyncState.fromJson` no longer throws on state it can only partly read.

  An unrecognised or wrongly typed `flushStatus` now reads as
  `QueueFlushStatus.idle`, and a command bucket that is not a map is skipped
  with the other bucket left intact. Both are reported through
  `apiCommandQueueLogger`.

  Previously either would throw, and a consumer persisting the state had no say
  in what that cost: `hydrated_bloc` catches the error, falls back to an empty
  queue and, by default, writes that back over the stored copy. `flushStatus` in
  particular records whether a flush happened to be running when the state was
  written — worth nothing on restore, but able to destroy every queued command
  alongside it.

  Command buckets are also read as `Map` rather than cast to
  `Map<String, dynamic>`, so a storage backend handing them back loosely typed
  no longer fails the restore.

## 0.2.0

- Added terminal failure predicates for queue-wide and command-specific
  non-retryable API failures.
- Added `ApiCommandTerminalFailureRule` for reusable status, data, error, and
  full-response matching.
- Documented terminal failure usage and expanded the package example.

## 0.1.0

- First public release of the pure Dart `api_command_queue` core package.
