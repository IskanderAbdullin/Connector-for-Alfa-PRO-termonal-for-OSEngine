# Recovery and limit services — development stage 2

**Status:** Current-session audit and subscription recovery infrastructure. **Not production-approved.**

This stage adds `AlfaProRecoveryAudit`, `AlfaProLimitClient`, read-only diagnostics in `AlfaProSession` and automatic data-subscription reacquisition. It does **not** claim to be full historical broker order reconciliation.

## Sources

Official provider specification: [Alfa PRO WebSocket API v2.2](https://alfadt.servicecdn.ru/alfadt/ad5/Alfa-Investments-Pro-API.pdf), sections 3 (subscriptions), 5 (limits), and 8 (client data model).

The documentation states that `OrderEntity` includes orders for the **current session**. Consequently an empty order snapshot is not proof that a previous session's order was executed or cancelled. Pending submissions and previous-session identifiers must be correlated against a durable local journal and broker-confirmed history before automatic trading resumes.

## Current-session state audit

`AlfaProSession.Recovery` ingests initial snapshots and subsequent updates/deletes from:

- `ClientAccountEntity`
- `ClientSubAccountEntity`
- `SubAccountRazdelEntity`
- `ClientBalanceEntity`
- `ClientPositionEntity`
- `OrderEntity`
- `ClientOperationEntity`

Call `AlfaProSession.AuditRecovery()` to obtain a structured `AlfaProAuditReport`. It indicates when all required snapshots have arrived and whether the data has **detectable** inconsistencies.

The audit checks account → subaccount → trading-section membership, positive position identities, parseable actual position sizes, untracked active orders, untracked own executions, known broker order statuses, unknown local submit outcomes and mismatched account/order identities or quantities.

`Clean=true` means **only** that no contradiction was detected in current-session snapshots. It **never sets** `AlfaProOrders.Reconciled=true` or independently enables trading.

The connection is marked unreconciled after disconnect, subscription renewal/recreation, or loss of critical subscription state. A fresh read-only audit does not bypass the durable reconciliation requirement.

## Subscription recovery

The subscription manager now:

1. Renews before the expiry timestamp.
2. On a failed renewal, discards the old lease ID.
3. Creates a fresh subscription for the same entity and instrument keys with `Init=true`.
4. Restores the initial snapshot and notifies the session.
5. Fails closed when even the new subscription cannot be acquired.

Concurrent subscription requests and renewal are serialized. Critical broker-state snapshots are invalidated while the recovered stream is re-established, and trading stays blocked until a separately verified reconciliation step.

A successful WebSocket connection does not imply successful terminal authorization or signing readiness.

## Broker limit query

`AlfaProLimitClient.QueryAsync(AlfaProLimitQuery)` is a **read-only** call to `#Order.Limit.Query`.

Modes:

- `AlfaProLimitBasis.FreeFunds` = 3.
- `AlfaProLimitBasis.PortfolioValue` = 4.

The result contains `Quantity` and `QuantityForOwnAssets` in *individual units*, not lots. The caller must provide verified account, trading-section, instrument object, board, side, price and order type. Malformed/negative results are rejected.

**Important:** This feature does not yet enforce per-order buying-power limits. Adding a synchronous limit query in every order-book-driven bot's hot path may increase latency. A future phase should establish a tested freshness/caching/risk policy before wiring it into live order submission.

## OsEngine portfolio mapping changes

- `ClientBalanceEntity` is associated with `(IdAccount, IdSubAccount, IdRazdelGroup)`; balances are no longer blindly copied to all trading sections of a subaccount.
- `ClientPositionEntity` updates are checked against account/section identity, and ambiguous instrument identification is rejected.
- Deleted positions can be zeroed in OsEngine using their previously observed `IdPosition`. Unknown deletions are reported and trading remains unreconciled.

Volume/price semantics for every instrument class, especially bonds and derivatives, still require real-provider verification.

## Test evidence

Run locally:

```powershell
dotnet run --project tests/AlfaPro.Core.Tests/AlfaPro.Core.Tests.csproj -c Release
dotnet run --project tests/AlfaPro.SimulatorTests/AlfaPro.SimulatorTests.csproj -c Release
```

GitHub Actions runs both test sets and compiles a fresh official OsEngine checkout on Windows. Simulator results are uploaded as JSON artifacts.

The new scenarios test missing and complete snapshots, unmatched active orders, unknown order outcomes, contradictory account/section identity, invalid positions, deleted orders, untracked executions, unknown statuses, mismatched quantities, limit-service modes, invalid limits, failed renewals and successful lease recreation.

## Unresolved before real trading

1. Implement an atomic, durable journal of **all** submitted orders and executions with explicit recovery across application/terminal restart.
2. Identify a provider-supported way to query historical orders and executions beyond the current session, or require operator resolution.
3. Reconcile both broker positions and the strategies' internal expected positions.
4. Verify real terminal message envelopes and field types, including `OrderRevisionEntity`, subscription expiry and order status identifiers.
5. Determine broker-side request quotas, rate limits and cancellation penalties.
6. Validate all supported asset classes, market boards, price steps and quantity units.
7. Test read-only against a running PRO Terminal, then use separately authorized minimal-size test orders.

**Do not change the default live trading interlock to satisfy a simulator test.**
