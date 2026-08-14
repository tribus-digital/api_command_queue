# Changelog

## 0.4.0

- Added `ApiCommandOrchestrator.autoFlushWhenDue`, off by default.

  0.3.0 stopped `flush()` sleeping a failed command's backoff, which means a
  retry now waits for something to trigger the next flush. A consumer driving
  flushes from connectivity changes and user activity will not retry at all on
  a device sitting idle - the ladder simply stops.

  With this on, the orchestrator keeps a single timer set to the earliest
  `nextDueAt` across its queues and flushes when it fires, so retries progress
  on their own again. The timer is cancelled while processing is disabled and
  on close, and has a one second floor so a due command a flush cannot clear
  becomes a slow poll rather than a spin.

  Consumers that already drive flushing on their own schedule do not need it.

- `nextDueAt` no longer counts a command that is currently being sent. It is
  not waiting for a flush, and reporting it as due made a caller scheduling
  against it fire repeatedly for as long as the request took.

- `flushAll` visits each queue once rather than once per command type it is
  registered under. The repeats were no-ops, but they made the walk several
  times longer than it needed to be and the ordering log unreadable.

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
