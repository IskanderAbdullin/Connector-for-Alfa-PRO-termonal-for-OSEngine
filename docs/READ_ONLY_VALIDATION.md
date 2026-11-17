# Read-only Alfa PRO compatibility diagnostic and recovery review

This tool is **not a live-trading test**. It has no order-entry, order-cancel or order-modification commands. The probe never opens the trading journal or enables order submission. It only connects to a *loopback* WebSocket endpoint, subscribes to status/reference/account/order data, and optionally observes a single explicitly requested instrument's order book and public trades.

Official provider reference: https://alfadt.servicecdn.ru/alfadt/ad5/Alfa-Investments-Pro-API.pdf

**Real terminal verification has not yet been performed** in this repository. CI tests use a simulated terminal. A running authenticated PRO instance is required for actual acceptance.

## 1. Run on the Windows PC with Alfa PRO Terminal

Open the **PRO Terminal → Integrations** settings. Enable the local API, create the Bearer token and check that the API status indicator is green. The published default local endpoint is `ws://127.0.0.1:3366/router/`.

**Do not enable external network access to the PRO API.** The diagnostic intentionally rejects external hosts, TLS proxies and remote URLs. If the terminal is on a separate Windows VPS, run the diagnostic *on that VPS*, not across the public internet.

From the root of the cloned connector repository, in PowerShell:

```powershell
dotnet build tools/AlfaPro.Diagnostic/AlfaPro.Diagnostic.csproj -c Release
dotnet run --project tools/AlfaPro.Diagnostic/AlfaPro.Diagnostic.csproj -c Release -- probe --seconds 15 --output "$env:USERPROFILE\Desktop\alfapro-readonly-report.json"
```

The program requests the Bearer token **in a hidden console prompt** unless `ALFA_PRO_TOKEN` is already set in the environment. Never pass it through command-line arguments and do not save or publish the token. It is not written to JSON reports.

The probe subscribes only to the standard `#Data.Query`, `#Data.Bus.*` and `#ConnectionState.Bus` channels. It never issues `#Order.Enter.Query`, `#Order.Cancel.Query` or `#Order.Limit.Query`.

If you already know a real **IdFi** (not IdObject) for an instrument, you can additionally test market-data subscriptions:

```powershell
dotnet run --project tools/AlfaPro.Diagnostic/AlfaPro.Diagnostic.csproj -c Release -- probe --seconds 20 --fi 123456
```

Replace `123456` with an actual instrument IdFi. Omit `--fi` if you do not know it. A single instrument is intentionally sampled to avoid large subscription loads.

### What the JSON report contains

- Date/time (UTC) of the read-only test.
- Transport status, broker authorization and sign-service readiness.
- Initial reference-data snapshot completeness.
- Number of known instruments and count of broker orders/positions.
- Non-sensitive recovery-audit issue codes, **not full error messages**.
- Counts of observed entity updates, order books and public trades.
- A count of missing **fixed, expected API field names** (e.g. `ClientPositionEntity.TorgPos`), which helps identify a real terminal API-schema mismatch without exporting private field values.
- Two explicit safety flags: `NoTradingCommandsWereIssued=true`, `SuitableToAuthorizeLiveTrading=false`.

The report omits the token, account/subaccount/section IDs, instrument identifiers, security prices, order details and raw broker JSON. You can send the sanitized report for review. **Do not share logs, screenshots, broker statements or the underlying order journal without separately checking that they contain no private details.**

A report with `CurrentSessionAuditClean=false` does **not** necessarily mean the broker is malfunctioning: broker data types, unknown manually placed orders or previous-session events may require reconciliation. It must not be interpreted as authorization for live trading.

## 2. Offline operator recovery queue

**Stop OsEngine before examining the journal.** The journal intentionally uses an exclusive file lock to prevent concurrent mutation.

From PowerShell:

```powershell
dotnet run --project tools/AlfaPro.Diagnostic/AlfaPro.Diagnostic.csproj -c Release -- review --journal "$env:LOCALAPPDATA\OsEngine\AlfaPro\orders.v1.jsonl"
```

The offline command does **not** open a WebSocket or require a token. It lists outstanding local orders with local/broker IDs (if known), a pending-cancel flag, and the latest operator observation, if any. Its output contains **sensitive trading identifiers**: keep it on your machine, unlike the sanitized read-only probe report.

For each unresolved order, compare the broker's current order list, official order history or brokerage statements with the journal. In particular, a previous-session order that is absent from `OrderEntity` has **not** thereby been proved rejected or cancelled.

## 3. Append a referenced operator observation

An operator observation is a locally asserted fact, **not broker confirmation performed by the connector**. It does not change the order state or allow orders to be resubmitted.

For example, after checking a real broker statement:

```powershell
dotnet run --project tools/AlfaPro.Diagnostic/AlfaPro.Diagnostic.csproj -c Release -- note --journal "$env:LOCALAPPDATA\OsEngine\AlfaPro\orders.v1.jsonl" --id 10 --source BrokerStatement --outcome Filled --ref "Broker-statement-page-3" --note "Matched broker order number and quantity"
```

The allowed sources are `TerminalOrderHistory`, `BrokerStatement` and `BrokerSupport`. Outcomes are `Working`, `Filled`, `Cancelled`, `Rejected` and `StillUncertain`.

Notes must reference identifiable external evidence and are added as separate timestamped, checksummed journal records. Earlier records are never overwritten. The connector continues to treat the original broker outcome as unresolved until it is validated through an authoritative API or an independently reviewed recovery procedure.

Never delete the journal to suppress duplicate-ID warnings. Never mark an order `Rejected` solely because it is missing from a current-session snapshot. When in doubt, mark `StillUncertain` and contact the broker.

## 4. Acceptance criteria before enabling production orders

1. Confirm the real terminal endpoint and Bearer authorization without revealing the token.
2. Compare real `AssetInfoEntity`, account hierarchy, `OrderEntity`, `ClientOperationEntity`, and position schema against connector mappings. Avoid uploading unsanitized JSON.
3. Validate instrument lot sizes, price steps, market codes and book subscription renewal over multiple minutes.
4. Cross-check pending and historical orders against broker-originated evidence, including outcomes not present in current-session `OrderEntity`.
5. Measure durable journal write latency on the intended Windows VPS / disk.
6. Only after all unresolved cases have independent confirmation, implement and verify an explicit, audited recovery-clearance gate. **Such a gate has not been implemented in this stage.**

No real order was transmitted or simulated as a real terminal order by the development workflow.
