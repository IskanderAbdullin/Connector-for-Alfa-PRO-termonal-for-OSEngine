# Durable order journal — stage 3

**Status: implemented and tested against the in-process PRO WebSocket simulator. This is not a production-trading approval.**

## Why a durable journal is necessary

A TCP/WebSocket disconnect can occur *after* the terminal received `#Order.Enter.Query` but *before* the client received its response. The client cannot safely infer whether that order exists. Simply retrying could execute a duplicate order.

The official PRO API 2.2 describes `OrderEntity` order status and `ClientOperationEntity` executions, but those publications do not provide an atomic transaction spanning the local disk and the remote broker. The connector must preserve an **unknown** order state until an authoritative external resolution is available.

This stage implements a local, append-only log, deterministic replay and hard interlocks against duplicate submissions. It does **not** implement historical broker recovery, automatic reauthorization, or exactly-once exchange execution.

## Storage

The new OsEngine connection parameter is **Durable order journal path**.

Default on Windows:

```text
%LOCALAPPDATA%\OsEngine\AlfaPro\orders.v1.jsonl
```

The journal is automatically opened when the OsEngine connector starts. The directory is created if missing. An exclusive lock prevents two OsEngine processes using the same file concurrently. A missing path, unreadable file, checksum failure, incomplete tail or invalid event sequence prevents connector startup. No malformed data is silently truncated, repaired or discarded.

**Operational guidance:** use a stable local NTFS path on the server running OsEngine. Do not put it in a temporary directory, untrusted synchronized cloud folder or the source repository. Preserve it when reinstalling OsEngine, backing up the server or replacing the disk. Back up only while the connector is stopped, so the archive is a consistent snapshot. The file contains account IDs, order details and trade identifiers: restrict filesystem permissions and handle backups as confidential. Bearer tokens are **not** written to this journal.

## Write-ahead sequence

1. **SubmitIntent**: local ID, account/section, instrument, side, price, quantity and broker-facing comment are serialized and fsynced **before** the WebSocket request is sent.
2. **SubmitAccepted**: the returned `ClientOrderNum` and `NumEDocument` are persisted before treating the response as acknowledged.
3. **SubmitRejected**: an explicit broker service rejection is persisted.
4. **BrokerState**: `OrderEntity` changes, remaining quantity, terminal status and order revision are appended.
5. **OwnExecution**: `ClientOperationEntity` execution IDs are persisted before the fill is forwarded to the strategy.
6. **CancelIntent**: the request to cancel is written before sending it.
7. **CancelAcknowledged**: the service response is recorded without treating it as confirmed broker cancellation.

Entries are newline-delimited JSON records containing monotonic sequence numbers, UTC event timestamps, serialized event data and SHA-256 integrity checksums. A single process/thread-safe writer serializes all append operations. A per-entry `FileStream.Flush(true)` requests durable storage before the operation continues.

**Performance caveat:** synchronous fsync on order/state/execution events prioritizes crash consistency over the lowest possible latency. Actual local SSD, Windows and remote-server performance has not been benchmarked. Market-depth messages are not written to this journal.

## Restart behavior

On restart, the complete validated journal reconstructs:

- each known local order ID (including any never-acknowledged submission);
- broker IDs and client order numbers, if previously acknowledged;
- last persisted status, remaining quantity and revision;
- pending cancellation intent;
- execution-ID de-duplication state.

The connector rejects reuse of a local ID already present in the durable journal. A submission missing its broker acknowledgement remains `OutcomeUnknown`. A cancellation awaiting authoritative confirmation remains pending. Late cancel acknowledgements do not reverse a terminal fill. Conflicting broker or client order identities fail closed.

**No order or cancellation is resent just because the application restarted.**

Importantly, replay does **not** set `AlfaProOrders.Reconciled` to `true`. A clean audit of the current session alone also does not authorize trading; prior sessions may have unresolved orders.

## How to inspect a recovery result

At the core level, `AlfaProOrders.AllOrders` exposes restored records, and `RecoveredOrderCount` reports how many local IDs were reconstructed. `AlfaProSession.AuditRecovery()` compares known local records with the available current-session broker snapshot and returns structured issues.

The OsEngine adapter logs the number of recovered records on connection and writes the read-only audit issue count to its logs. It does not silently mark uncertain orders as cancelled. The full operator workflow for resolving orders whose histories are not present in the current-session API remains unimplemented.

## Tests

Run:

```powershell
dotnet run --project tests/AlfaPro.Core.Tests/AlfaPro.Core.Tests.csproj -c Release
dotnet run --project tests/AlfaPro.SimulatorTests/AlfaPro.SimulatorTests.csproj -c Release
```

The new `JournalStageTests` cover write-ahead persistence, unknown requests after restart, partial fills and broker IDs, execution deduplication, persistent pending cancellations, corrupt checksums, truncated tails, file locking, orphan event rejection, storage loss, identity collisions, late cancellation acknowledgements, and the read-only broker audit not granting trading readiness.

The simulator connects only to the **in-process mock terminal**. GitHub Actions also builds the modified OsEngine project on Windows. Test reports are saved as JSON artifacts in each workflow run.

## Outstanding before live trading

- Persist and reconcile the complete broker/account/instrument reference context, including all pre-existing orders and prior sessions where available.
- Establish authoritative broker-side historical order/fill reconciliation or a deliberately explicit operator review workflow.
- Validate real `OrderEntity` and `ClientOperationEntity` messages and identity correlations against a running PRO terminal.
- Rebuild the OsEngine UI/strategy order projection from restored broker records in an independently verifiable manner.
- Implement transaction-specific risk controls, rate limits and live acceptance tests; assess disk flush latency on the target Windows VPS.
- Define versioned journal migration/archival and tested disaster recovery with server backups.

**Never delete or reset the journal just to work around a duplicate local order ID or unresolved broker submission. That can cause a second real order.**
