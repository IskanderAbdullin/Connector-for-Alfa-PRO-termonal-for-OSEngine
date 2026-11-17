# Alfa-Investments PRO WebSocket connector for OsEngine

A native OsEngine connector **under active development**, based on Alfa-Investments PRO WebSocket API 2.2.

- Official protocol: https://alfadt.servicecdn.ru/alfadt/ad5/Alfa-Investments-Pro-API.pdf
- OsEngine upstream: https://github.com/AlexWan/OsEngine
- Upstream baseline inspected: `e33e66d947a4f584a19f49ebea2b351620a87463`
- Platform: OsEngine / C# / .NET 10 / Windows

## IMPORTANT SAFETY STATUS

**NOT PRODUCTION READY. DO NOT ENABLE "Allow LIVE trading" until a real-terminal integration test and order-reconciliation audit have passed.** The implementation has not been tested with a live Alfa PRO terminal or live trading account. Trading is off by default.

The repository contains code, not an independently installable trading application. It is intended to be injected into an OsEngine source checkout.

## Implemented so far

- WebSocket Bearer authentication and router requests with JSON-string `Payload`.
- Concurrent request correlation, send serialization, explicit request timeouts and connection-loss handling.
- Basic `#Data.Query` snapshots, bus subscriptions and renewal.
- Initial terminal-state request plus authorization/signature monitoring.
- Instrument identity mapping (`IdObject` vs `IdFi`), instrument parameters, order book and public trades.
- Account section, balance and position event conversion into OsEngine models.
- Basic allowed order parameter lookup and guarded limit/market order submission.
- Order cancellations, broker-status mapping, partial fills, execution de-duplication and early event reconciliation.
- Exclusive, checksummed, append-only durable order journal: write-ahead submit/cancel intents, fsync and restart replay of broker IDs, own fills and open/unknown outcomes.
- Read-only current-session recovery audit, subscription re-creation and typed broker-limit queries.
- Standard OsEngine `IServerRealization` and conservative `IServerPermission` integration.
- Unit-check console project and Windows OsEngine integration build workflow.
- Installer that makes minimal, reviewable edits to OsEngine `ServerMaster.cs`.

## Not completed / not verified

- Automatic reconciliation of orders and fills from previous broker sessions (durable local journal is implemented, but broker-side historical matching remains pending).
- Complete reconciliation of pre-existing, unmatched or manually placed broker orders.
- Subscription recovery after every possible expiry/failure (current implementation fails closed).
- Accurate `RCode` / account-section eligibility verification for all market types.
- Broker margin and limit-service integration, margin-risk calculations.
- Production-approved handling of bond price conventions and instrument-specific volume semantics.
- Native order modification (the API documents cancel + new order).
- Historical candle mapping, stop orders, additional order types, comprehensive tick history.
- Complete position deletion handling and full broker/account portability.
- Measured rate limits, latency guarantees, load testing, live terminal verification.
- Protected-at-rest token persistence beyond standard OsEngine password parameter behavior.

## Installation into an OsEngine checkout

On Windows (with .NET 10 SDK installed), clone this connector repository and the official OsEngine repository.

From this repository:

```powershell
python tools/install_into_osengine.py --osengine C:\\Source\\OsEngine\\project\\OsEngine --check
python tools/install_into_osengine.py --osengine C:\\Source\\OsEngine\\project\\OsEngine
dotnet build C:\\Source\\OsEngine\\project\\OsEngine\\OsEngine.csproj
```

The installer copies `src/OsEngine/*.cs` and `src/AlfaPro.Core/*.cs` into `Market/Servers/AlfaPro/` and adds the connector to `ServerMaster.cs` (factory, enum, permissions, server listings). It does **not** launch OsEngine or trade. It creates a one-time `ServerMaster.cs.alfapro-backup` when changing the master file. Review the resulting `git diff` before committing OsEngine changes.

OsEngine's project uses SDK-default recursive source inclusion, so adding the C# files under `Market/Servers/AlfaPro/` should compile them together with the host. The repository's standalone `AlfaPro.Core.csproj` is for isolated tests; it is not copied into the host.

## Configuration

1. Open Alfa-Investments PRO Terminal.
2. Enable the local API under **Integrations**.
3. Generate a token in the terminal.
4. Run OsEngine and select connector **AlfaPro**.
5. Set `PRO WebSocket URL` to `ws://127.0.0.1:3366/router/`.
6. Enter `PRO Bearer Token`.
7. Check the **Durable order journal path** setting: its default is a private file in the current Windows user's LocalAppData/OsEngine/AlfaPro directory. Keep that file across reinstalls and use a stable disk.
8. Keep **Allow LIVE trading = false** while validating read-only data and portfolio output.

## Tests

```powershell
dotnet run --project tests/AlfaPro.Core.Tests/AlfaPro.Core.Tests.csproj -c Release
dotnet run --project tests/AlfaPro.SimulatorTests/AlfaPro.SimulatorTests.csproj -c Release
```

The simulator tests use a real *local* WebSocket server; there are no real API tokens, broker connections or live orders. Scenarios include authorization, routing, parallel requests, subscriptions, order books, partial fills, duplicate executions, lost acknowledgements, cancellation/execution races and disconnects. Reports are written to `test-results/simulator-results.json` and uploaded by GitHub Actions.

See [Simulator design, scenario matrix and limitations](docs/SIMULATOR.md).

## Recovery and broker limits (stage 2)

The connector now includes a read-only reconciliation audit for broker accounts, trading sections, current-session orders, executions and positions; expired subscription leases can be reacquired; and the Alfa PRO limit service is accessible through a typed query.

**This is not full broker-side restart reconciliation.** The local journal now survives process restarts, but current-session snapshots cannot prove the outcome of orders from previous sessions. Live order submission remains disabled until independently verified reconciliation is implemented.

See [Recovery architecture, limit service and remaining blockers](docs/RECOVERY.md).

## Durable order journal (stage 3)

`AlfaProOrderJournal` now persists every new order intent **before** sending it over the WebSocket, then records broker acknowledgements, status transitions, own executions and cancellation requests. On restart, previous local order IDs and execution IDs are restored. Missing broker acknowledgements remain uncertain; there is **no automatic re-submission**.

Corrupted/truncated files or duplicate broker identity mappings fail closed. File locking prevents two OsEngine instances sharing one journal simultaneously. Journal storage costs an fsync on each trading event; latency on your own VPS remains to be measured.

See [Journal setup, crash-consistency design, replay tests and remaining risks](docs/JOURNAL.md).

The GitHub Actions workflow also builds an OsEngine Windows integration. **Simulator tests do not verify behavior against the actual Alfa PRO Terminal.**

## Architecture

```text
OsEngine Robots -> IServerRealization -> AlfaProSession
                                       |-> AlfaProOrders -> AlfaProOrderJournal
                                       |-> AlfaProSubscriptions
                                       |-> AlfaProTransport -> PRO Terminal WebSocket
```

### Key safety invariants

- An unknown order submission outcome is never automatically resubmitted.
- WebSocket connected does not mean broker authorized/signature ready.
- `OrderEntity` confirms broker status; a successful submission response does not equal execution.
- One own execution ID must not be processed twice.
- Order book quantities are lots; order submission quantities are individual units.
- No automatic 'change order price' implementation that could conceal cancellation/fill races.

## License

Connector source is independently authored for integration with OsEngine. Users must also comply with the license of the OsEngine host project. Neither project is affiliated with or endorsed by Alfa-Bank.
