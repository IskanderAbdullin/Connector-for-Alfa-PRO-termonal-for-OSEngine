using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;

namespace AlfaPro.Core;

/// <summary>
/// An independent, read-only reconstruction of the PRO terminal's current-session state.
/// This is NOT a substitute for a durable order journal or a broker-side historical query.
/// </summary>
public sealed class AlfaProRecoveryAudit
{
    private static readonly string[] RequiredTables =
    [
        "ClientAccountEntity", "ClientSubAccountEntity", "SubAccountRazdelEntity",
        "ClientBalanceEntity", "ClientPositionEntity", "OrderEntity", "ClientOperationEntity"
    ];

    private static readonly HashSet<int> ActiveBrokerStatuses =
        [5, 6, 10, 12, 14, 15, 23, 24];

    private readonly object _sync = new();
    private readonly Dictionary<string, Dictionary<long, JsonElement>> _tables = new();
    private readonly HashSet<string> _snapshots = [];
    private readonly HashSet<string> _invalidTables = [];

    public bool HasAllSnapshots
    {
        get
        {
            lock (_sync) return RequiredTables.All(_snapshots.Contains);
        }
    }

    public void Invalidate(string type)
    {
        lock (_sync)
        {
            _snapshots.Remove(type);
            _invalidTables.Add(type);
        }
    }

    public void InvalidateAll()
    {
        lock (_sync)
        {
            _snapshots.Clear();
            foreach (var type in RequiredTables) _invalidTables.Add(type);
        }
    }

    public void AcceptSnapshot(string type, JsonElement data)
    {
        if (!RequiredTables.Contains(type)) return;
        if (data.ValueKind != JsonValueKind.Array)
        {
            Invalidate(type);
            return;
        }
        lock (_sync)
        {
            // Snapshot is authoritative for this specific table at the response boundary.
            var next = new Dictionary<long, JsonElement>();
            bool valid = true;
            foreach (var item in data.EnumerateArray())
            {
                long key = Key(type, item);
                if (key <= 0 || !next.TryAdd(key, item.Clone())) { valid = false; break; }
            }
            if (!valid)
            {
                _snapshots.Remove(type);
                _invalidTables.Add(type);
                return;
            }
            _tables[type] = next;
            _invalidTables.Remove(type);
            _snapshots.Add(type);
        }
    }

    public void AcceptUpdate(string type, JsonElement item, bool deleted = false)
    {
        if (!RequiredTables.Contains(type)) return;
        long key = Key(type, item);
        lock (_sync)
        {
            if (key <= 0)
            {
                _snapshots.Remove(type);
                _invalidTables.Add(type);
                return;
            }
            if (!_tables.TryGetValue(type, out var records))
                _tables[type] = records = new Dictionary<long, JsonElement>();
            if (deleted)
            {
                records.Remove(key);
                return;
            }
            // DataEntity.Version is a storage version; it need not be a trading status revision.
            if (records.TryGetValue(key, out var existing))
            {
                long incomingVersion = item.Field("Version").Int64();
                long currentVersion = existing.Field("Version").Int64();
                if (incomingVersion > 0 && currentVersion > incomingVersion) return;
            }
            records[key] = item.Clone();
        }
    }

    public AlfaProAuditReport Evaluate(IEnumerable<AlfaOrderRecord> trackedOrders)
    {
        var tracked = trackedOrders.ToArray();
        lock (_sync)
        {
            var issues = new List<AlfaProAuditIssue>();

            foreach (string required in RequiredTables)
            {
                if (!_snapshots.Contains(required))
                    issues.Add(new("SNAPSHOT_MISSING", required));
            }
            foreach (string invalid in _invalidTables.OrderBy(x => x))
                issues.Add(new("INVALID_TABLE", invalid));
            if (issues.Count > 0)
                return new(false, 0, 0, tracked.Length, issues);

            var accounts = Table("ClientAccountEntity").Values.ToArray();
            var subs = Table("ClientSubAccountEntity").Values.ToArray();
            var sections = Table("SubAccountRazdelEntity").Values.ToArray();
            var positions = Table("ClientPositionEntity").Values.ToArray();
            var orders = Table("OrderEntity");
            var trades = Table("ClientOperationEntity");

            var accountIds = accounts.Select(a => a.Field("IdAccount").Int64())
                .Where(id => id > 0).ToHashSet();
            if (accountIds.Count == 0)
                issues.Add(new("NO_ACCOUNT", "No identifiable authorized account in snapshot"));

            var subIds = new HashSet<long>();
            foreach (var sub in subs)
            {
                long subId = sub.Field("IdSubAccount").Int64();
                long accountId = sub.Field("IdAccount").Int64();
                if (subId <= 0 || !accountIds.Contains(accountId))
                    issues.Add(new("SUBACCOUNT_MISMATCH", "Subaccount not linked to an observed account"));
                else subIds.Add(subId);
            }
            if (subIds.Count == 0)
                issues.Add(new("NO_SUBACCOUNT", "No valid subaccount"));

            var sectionIds = new HashSet<long>();
            foreach (var section in sections)
            {
                long sectionId = section.Field("IdRazdel").Int64();
                if (sectionId <= 0 ||
                    !accountIds.Contains(section.Field("IdAccount").Int64()) ||
                    !subIds.Contains(section.Field("IdSubAccount").Int64()))
                    issues.Add(new("SECTION_MISMATCH", "Trading section has invalid account hierarchy"));
                else sectionIds.Add(sectionId);
            }
            if (sectionIds.Count == 0)
                issues.Add(new("NO_SECTION", "No valid portfolio section"));

            foreach (var position in positions)
            {
                var quantity = position.Field("TorgPos");
                if (!sectionIds.Contains(position.Field("IdRazdel").Int64()) ||
                    !accountIds.Contains(position.Field("IdAccount").Int64()) ||
                    !subIds.Contains(position.Field("IdSubAccount").Int64()) ||
                    position.Field("IdObject").Int64() <= 0 ||
                    quantity.ValueKind is not (JsonValueKind.Number or JsonValueKind.String) ||
                    !decimal.TryParse(quantity.Text(), System.Globalization.NumberStyles.Float,
                        System.Globalization.CultureInfo.InvariantCulture, out _))
                    issues.Add(new("POSITION_INVALID",
                        "Position has invalid identity, account hierarchy, or actual quantity"));
            }

            var trackedByMarket = tracked.Where(o => o.BrokerId > 0)
                .GroupBy(o => o.BrokerId).ToDictionary(g => g.Key, g => g.First());
            var trackedByClient = tracked.Where(o => o.ClientOrderNumber > 0)
                .GroupBy(o => o.ClientOrderNumber).ToDictionary(g => g.Key, g => g.First());

            foreach (var broker in orders.Values)
            {
                long id = broker.Field("NumEDocument").Int64();
                long client = broker.Field("ClientOrderNum").Int64();
                int status = (int)broker.Field("IdOrderStatus").Int64();
                bool known = trackedByMarket.TryGetValue(id, out var local) ||
                    (client > 0 && trackedByClient.TryGetValue(client, out local));
                if (status is not (5 or 6 or 10 or 12 or 14 or 15 or 18 or 20 or 21 or 23 or 24 or 25 or 26))
                    issues.Add(new("ORDER_STATUS_UNKNOWN", "Broker order " + id + ": " + status));
                if (!known)
                {
                    if (ActiveBrokerStatuses.Contains(status))
                        issues.Add(new("UNTRACKED_ACTIVE_ORDER", "Broker order " + id));
                    continue;
                }
                if (local is not null && !SameIdentity(broker, local.Request))
                    issues.Add(new("ORDER_IDENTITY_MISMATCH", "Broker order " + id));
                if (local is not null)
                {
                    long quantity = broker.Field("Quantity").Int64();
                    if (quantity > 0 && quantity != local.Request.QuantityUnits)
                        issues.Add(new("ORDER_QUANTITY_MISMATCH", "Broker order " + id));
                }
            }

            foreach (var local in tracked)
            {
                if (local.State is AlfaSubmissionState.Filled or AlfaSubmissionState.Cancelled
                    or AlfaSubmissionState.Rejected) continue;
                if (local.BrokerId <= 0 && local.ClientOrderNumber <= 0)
                {
                    issues.Add(new("ORDER_OUTCOME_UNKNOWN", "Local order " + local.Request.LocalOrderId));
                    continue;
                }
                bool found = (local.BrokerId > 0 && orders.ContainsKey(local.BrokerId)) ||
                    orders.Values.Any(o => local.ClientOrderNumber > 0 &&
                        o.Field("ClientOrderNum").Int64() == local.ClientOrderNumber);
                if (!found)
                    issues.Add(new("ORDER_NOT_IN_SNAPSHOT", "Local order " + local.Request.LocalOrderId));
            }

            foreach (var trade in trades.Values)
            {
                long brokerId = trade.Field("NumEDocument").Int64();
                long account = trade.Field("IdAccount").Int64();
                if (brokerId <= 0 || account <= 0 || !accountIds.Contains(account))
                    issues.Add(new("EXECUTION_INVALID", "Execution has unresolved identity or account"));
                if (brokerId > 0 && trackedByMarket.TryGetValue(brokerId, out var owned))
                {
                    if (!SameIdentity(trade, owned.Request))
                        issues.Add(new("EXECUTION_IDENTITY_MISMATCH", "Execution for order " + brokerId));
                }
                else
                {
                    // Manual / pre-existing operations require explicit operator reconciliation.
                    issues.Add(new("UNTRACKED_EXECUTION", "Execution for untracked order " + brokerId));
                }
            }

            return new(issues.Count == 0, orders.Count, positions.Length, tracked.Length, issues);
        }
    }

    private Dictionary<long, JsonElement> Table(string type)
        => _tables.TryGetValue(type, out var table) ? table : new();

    private static bool SameIdentity(JsonElement item, AlfaOrderRequest order)
    {
        return MatchesIfSupplied(item, "IdAccount", order.Account) &&
               MatchesIfSupplied(item, "IdSubAccount", order.SubAccount) &&
               MatchesIfSupplied(item, "IdRazdel", order.Section) &&
               MatchesIfSupplied(item, "IdObject", order.ObjectId);
    }

    private static bool MatchesIfSupplied(JsonElement item, string field, long expected)
    {
        long value = item.Field(field).Int64();
        return value == 0 || value == expected;
    }

    private static long Key(string type, JsonElement item)
        => type switch
        {
            "ClientAccountEntity" => item.Field("IdAccount").Int64(),
            "ClientSubAccountEntity" => item.Field("IdSubAccount").Int64(),
            "SubAccountRazdelEntity" => item.Field("IdRazdel").Int64(),
            "ClientBalanceEntity" => item.Field("DataId").Int64(
                item.Field("IdSubAccount").Int64() * 8 + item.Field("IdRazdelGroup").Int64()),
            "ClientPositionEntity" => item.Field("IdPosition").Int64(),
            "OrderEntity" => item.Field("NumEDocument").Int64(),
            "ClientOperationEntity" => item.Field("IdOperation").Int64(),
            _ => 0
        };
}

public sealed record AlfaProAuditIssue(string Code, string Detail);

/// <summary>
/// Clean=true means that these *current-session snapshots* contain no detected contradiction.
/// It never by itself authorizes live trading, because earlier sessions are not covered.
/// </summary>
public sealed record AlfaProAuditReport(
    bool Clean,
    int BrokerOrders,
    int BrokerPositions,
    int TrackedOrders,
    IReadOnlyList<AlfaProAuditIssue> Issues);
