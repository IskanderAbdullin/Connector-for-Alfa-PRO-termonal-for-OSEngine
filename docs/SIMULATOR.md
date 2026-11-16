# Alfa PRO simulator: test stand and acceptance boundaries

## Purpose

This repository contains a **local, deterministic, in-process broker simulator** for the Alfa-Investments PRO WebSocket router API. It exercises the *actual* `AlfaPro.Core` transport and order/subscription components over a WebSocket connection; no broker account, API token, market data subscription, or real trade is required.

**Important distinction:** the simulator implements only the subset of the documented protocol necessary for these tests. Passing does **not** confirm that the real Alfa PRO terminal behaves identically. The production connector is **not approved for live trading**.

Official API: https://alfadt.servicecdn.ru/alfadt/ad5/Alfa-Investments-Pro-API.pdf

## Run on your machine

Requires .NET 10 SDK. Does not use Computer Use or interact with a trading terminal.

```powershell
dotnet run --project tests/AlfaPro.Core.Tests/AlfaPro.Core.Tests.csproj -c Release
dotnet run --project tests/AlfaPro.SimulatorTests/AlfaPro.SimulatorTests.csproj -c Release
```

The simulator binds Kestrel to a dynamically assigned loopback port and responds to:
- WebSocket handshake with a mock Bearer token;
- `listen`, `unlisten`, `request`, `response` and `broadcast` envelopes;
- JSON text encoded inside the envelope's `Payload` string;
- `#ConnectionState.Bus`, `#Data.Query` and `#Data.Bus.*`;
- `#Order.Enter.Query` and `#Order.Cancel.Query`.

It can inject delayed, fragmented, suppressed and reordered replies; broker publications before order acknowledgements; rejection and cancellation/execution races; duplicate trades; and dropped WebSocket connections.

## Tested scenarios

| Group | Scenario |
|---|---|
| Authentication | Invalid Bearer token rejected |
| Routing | JSON-string payload and unique correlation IDs |
| Routing | Out-of-order concurrent replies |
| Transport | Fragmented WebSocket text frame |
| Transport | Request times out; pending map cleaned; late response ignored |
| Subscriptions | Initial snapshot, renewal, unsubscribe/unlisten |
| Subscriptions | 16 concurrent consumers of one book do not create duplicate broker subscriptions |
| Subscriptions | Failed renewal is surfaced, not ignored |
| Market data | Initial instrument and parameter snapshot |
| Market data | Order book update and public trade deduplication |
| Trading safety | Unauthenticated or unsigned terminal rejects submission |
| Order lifecycle | Submission, working, partial execution, own fill, full execution |
| Order lifecycle | Broker execution and order publication before submission response |
| Order lifecycle | Explicit broker rejection |
| Order lifecycle | Lost submit acknowledgement does not trigger duplicate submission |
| Order lifecycle | Cancellation/execution race |
| Order lifecycle | Old revision cannot reopen a final order |
| Disconnection | In-flight request fails and session loses trading readiness |
| Load | 200 concurrent independently correlated routing requests |
| Payload validation | Invalid depth/volume rejected; integer lot conversion |
| Subscription limits | 31st book rejected after 30 subscriptions |
| Load | 500 book broadcasts and most recent book delivered |
| Cancellation | Partial fill followed by confirmed cancellation preserves executed amount |
| Order parameters | Market order omits limit price and retains piece quantity |
| Account safety | Mismatched account event cannot modify an order |

The runner prints `PASS`/`FAIL` for each scenario and returns nonzero on any failure. It writes `test-results/simulator-results.json` with the complete per-scenario result. GitHub Actions uploads that JSON as **alfapro-simulator-results**.

## GitHub Actions

Workflow: [Alfa PRO connector build](../.github/workflows/build.yml)

The workflow runs three independent jobs:
1. `core` — fast core conversion checks on Linux.
2. `simulator` — complete WebSocket simulation scenarios on Linux.
3. `osengine-integration` — installs the connector source in a clean official OsEngine checkout and compiles OsEngine on Windows.

The Windows job verifies **source compatibility / compilation**, not that any PRO Terminal instance was connected.

Test results may vary if the actual Alfa PRO API schema changes. Compare with a real terminal capture before enabling live orders.

## Safety properties tested

- A routing timeout must not be treated as proof that a broker order was not executed.
- The same local order cannot be blindly resubmitted after uncertain results.
- A service cancellation acknowledgement is not final cancellation confirmation.
- An order filled while cancellation is pending must remain filled.
- Broker events with a conflicting account/subaccount/section cannot mutate an order.
- One own execution must not be counted twice.
- Subscription limits are enforced before extra remote requests are sent.
- Initial data snapshots alone must **not** mark all pre-existing orders as reconciled.

## Remaining coverage gaps

These tests do **not** exercise actual exchange fills, real terminal auth/signing, persistent order recovery between application restarts, genuine subscription expiration timing, broker-side rate quotas, margin/limit-service semantics, all instruments and bonds, history candles, multi-day sessions, or live latency.

The current OsEngine connector still has incomplete trading reconciliation and intentionally blocks live trading until that safety work is implemented and verified. Do not disable that safeguard just to pass a test.

A future real-terminal test stage should run **read-only** first: connect with API enabled, collect anonymized instrument/account/reference responses, validate correct mappings and subscriptions, and only then assess guarded live orders with separate explicit authorization.
