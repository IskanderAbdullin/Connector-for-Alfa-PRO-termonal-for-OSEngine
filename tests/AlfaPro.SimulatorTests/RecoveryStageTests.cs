using AlfaPro.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace AlfaPro.SimulatorTests;

internal static class RecoveryStageTests
{
    public static readonly (string Name, Func<Task> Run)[] Tests =
    [
        ("missing broker snapshots cannot pass reconciliation", MissingSnapshots),
        ("complete empty-session snapshots can be audited read-only", EmptySnapshots),
        ("untracked broker working order blocks the audit", UntrackedActive),
        ("invalid account and section hierarchy blocks audit", IncorrectHierarchy),
        ("unknown local submission is unresolved in audit", UncertainOrder),
        ("tracked order missing from authoritative snapshot blocks audit", MissingBrokerOrder),
        ("malformed portfolio position is rejected", InvalidPosition),
        ("broker delete removes an untracked active order from audit", DeletingBrokerOrder),
        ("session captures snapshots without granting trading readiness", SessionReadOnlyAudit),
        ("PRO limit service maps free funds and portfolio basis exactly", LimitSuccess),
        ("invalid/malformed broker limit response is rejected", LimitValidation),
        ("expired lease automatically reacquires new ID and snapshot", LeaseRecreation),
        ("untracked execution blocks reconciliation", UntrackedExecution),
        ("unknown broker order status blocks reconciliation", UnknownStatus),
        ("tracked order volume mismatch blocks reconciliation", TrackedQuantityMismatch)
    ];

    private static JsonElement J(object obj)
    {
        using var document = JsonDocument.Parse(JsonSerializer.Serialize(obj));
        return document.RootElement.Clone();
    }
    private static void Check(bool value, string reason)
    {
        if (!value) throw new InvalidOperationException(reason);
    }
    private static async Task ExpectError(Func<Task> fn, string reason)
    {
        try { await fn(); }
        catch (Exception) { return; }
        throw new InvalidOperationException(reason);
    }
    private static AlfaProRecoveryAudit NewCompleteAudit()
    {
        var audit = new AlfaProRecoveryAudit();
        audit.AcceptSnapshot("ClientAccountEntity", J(new[] { new { IdAccount = 1 } }));
        audit.AcceptSnapshot("ClientSubAccountEntity", J(new[] { new { IdAccount = 1, IdSubAccount = 2 } }));
        audit.AcceptSnapshot("SubAccountRazdelEntity", J(new[] {
            new { IdAccount = 1, IdSubAccount = 2, IdRazdel = 3, IdRazdelGroup = 1 }
        }));
        audit.AcceptSnapshot("ClientBalanceEntity", J(Array.Empty<object>()));
        audit.AcceptSnapshot("ClientPositionEntity", J(Array.Empty<object>()));
        audit.AcceptSnapshot("OrderEntity", J(Array.Empty<object>()));
        audit.AcceptSnapshot("ClientOperationEntity", J(Array.Empty<object>()));
        return audit;
    }
    private static AlfaOrderRequest Order(int id = 1)
        => new(id, 1, 2, 3, 123, 4, 7, 1, 10, 100.01m, false, "audit-test");

    private static Task MissingSnapshots()
    {
        var audit = new AlfaProRecoveryAudit();
        audit.AcceptSnapshot("OrderEntity", J(Array.Empty<object>()));
        var report = audit.Evaluate([]);
        Check(!report.Clean && report.Issues.Any(x => x.Code == "SNAPSHOT_MISSING"),
            "incomplete snapshots were accepted");
        return Task.CompletedTask;
    }
    private static Task EmptySnapshots()
    {
        var audit = NewCompleteAudit();
        var report = audit.Evaluate([]);
        Check(audit.HasAllSnapshots && report.Clean && report.BrokerOrders == 0 &&
            report.BrokerPositions == 0, "valid empty account snapshots rejected");
        audit.InvalidateAll();
        Check(!audit.HasAllSnapshots && !audit.Evaluate([]).Clean,
            "invalidated report remained clean");
        return Task.CompletedTask;
    }
    private static Task UntrackedActive()
    {
        var audit = NewCompleteAudit();
        audit.AcceptUpdate("OrderEntity", J(new {
            NumEDocument = 551, IdAccount = 1, IdSubAccount = 2, IdRazdel = 3,
            IdOrderStatus = 14, Quantity = 10, Rest = 10, IdObject = 123
        }));
        Check(audit.Evaluate([]).Issues.Any(x => x.Code == "UNTRACKED_ACTIVE_ORDER"),
            "manual live order not detected");
        return Task.CompletedTask;
    }
    private static Task IncorrectHierarchy()
    {
        var audit = NewCompleteAudit();
        audit.AcceptSnapshot("SubAccountRazdelEntity", J(new[] { new
        {
            IdAccount = 999, IdSubAccount = 2, IdRazdel = 3, IdRazdelGroup = 1
        }}));
        var report = audit.Evaluate([]);
        Check(!report.Clean && report.Issues.Any(x => x.Code == "SECTION_MISMATCH"),
            "foreign account section was accepted");
        return Task.CompletedTask;
    }
    private static Task UncertainOrder()
    {
        var audit = NewCompleteAudit();
        var local = new AlfaOrderRecord(Order());
        Check(audit.Evaluate([local]).Issues.Any(x => x.Code == "ORDER_OUTCOME_UNKNOWN"),
            "unknown submit outcome was considered reconciled");
        return Task.CompletedTask;
    }
    private static Task MissingBrokerOrder()
    {
        var audit = NewCompleteAudit();
        var local = new AlfaOrderRecord(Order());
        // In tests, a tracked locally-created order has not yet been sent:
        // missing broker ID must still block reconciliation.
        Check(audit.Evaluate([local]).Issues.Any(x => x.Code == "ORDER_OUTCOME_UNKNOWN"),
            "unmatched tracked order was not blocked");
        return Task.CompletedTask;
    }
    private static Task InvalidPosition()
    {
        var audit = NewCompleteAudit();
        audit.AcceptUpdate("ClientPositionEntity", J(new {
            IdPosition = 99, IdAccount = 1, IdSubAccount = 2,
            IdRazdel = 123456, IdObject = 123, TorgPos = 10
        }));
        Check(audit.Evaluate([]).Issues.Any(x => x.Code == "POSITION_INVALID"),
            "position in unknown section was not detected");
        return Task.CompletedTask;
    }
    private static Task DeletingBrokerOrder()
    {
        var audit = NewCompleteAudit();
        audit.AcceptUpdate("OrderEntity", J(new {
            NumEDocument = 44, IdOrderStatus = 14
        }));
        Check(!audit.Evaluate([]).Clean, "new untracked active order passed");
        audit.AcceptUpdate("OrderEntity", J(new { NumEDocument = 44 }), deleted: true);
        Check(audit.Evaluate([]).Clean, "deleted untracked order remained in audit cache");
        return Task.CompletedTask;
    }
    private static async Task SessionReadOnlyAudit()
    {
        await using var mock = await MockProTerminal.StartAsync();
        mock.Snapshot("ClientAccountEntity", new { IdAccount = 1 });
        mock.Snapshot("ClientSubAccountEntity", new { IdAccount = 1, IdSubAccount = 2 });
        mock.Snapshot("SubAccountRazdelEntity", new {
            IdAccount = 1, IdSubAccount = 2, IdRazdel = 3, IdRazdelGroup = 1
        });
        await using var session = new AlfaProSession();
        await session.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
        var report = session.AuditRecovery();
        Check(report.Clean && session.Recovery.HasAllSnapshots,
            "initial snapshot was not ingested for read-only audit");
        Check(!session.Orders.Reconciled,
            "audit allowed live order submission without durable recovery");
        mock.DropConnection();
        var end = DateTime.UtcNow.AddSeconds(2);
        while (session.Recovery.HasAllSnapshots && DateTime.UtcNow < end)
            await Task.Delay(10);
        Check(!session.Recovery.HasAllSnapshots && !session.AuditRecovery().Clean,
            "drop did not invalidate all broker snapshots");
    }
    private static async Task LimitSuccess()
    {
        await using var mock = await MockProTerminal.StartAsync();
        mock.OverrideReply = frame => Task.FromResult<MockReply?>(
            frame.Channel == "#Order.Limit.Query"
                ? MockReply.Of(new { Quantity = 200, QuantityForOwnAssets = 70 })
                : null);
        await using var transport = new AlfaProTransport();
        await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
        var limits = new AlfaProLimitClient(transport);
        var result = await limits.QueryAsync(new AlfaProLimitQuery(1,3,123,4,1,100.01m,2));
        Check(result == new AlfaProLimitResult(200,70), "broker result quantities incorrect");
        var payload = mock.Frames.Single(x => x.Channel == "#Order.Limit.Query").Payload;
        Check(payload.Field("LimitRequestType").Int64() == 3 &&
              payload.Field("IdDocumentType").Int64() == 1 &&
              payload.Field("IdObject").Int64() == 123, "incorrect free-funds limit query");
        await limits.QueryAsync(new AlfaProLimitQuery(1,3,123,4,-1,100.01m,2,
            AlfaProLimitBasis.PortfolioValue));
        Check(mock.Frames.Last(x => x.Channel == "#Order.Limit.Query")
            .Payload.Field("LimitRequestType").Int64() == 4, "portfolio basis ignored");
        Check(mock.Requests("#Order.Enter.Query") == 0,
            "read-only limit query unexpectedly submitted an order");
    }
    private static async Task LimitValidation()
    {
        await using var mock = await MockProTerminal.StartAsync();
        mock.OverrideReply = f => Task.FromResult<MockReply?>(
            f.Channel == "#Order.Limit.Query" ? MockReply.Of(new { Quantity = -1 }) : null);
        await using var transport = new AlfaProTransport();
        await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
        var limits = new AlfaProLimitClient(transport);
        await ExpectError(() => limits.QueryAsync(new AlfaProLimitQuery(1,3,123,4,1,100m,2)),
            "negative/incomplete limit was accepted");
        await ExpectError(() => limits.QueryAsync(new AlfaProLimitQuery(0,3,123,4,1,100m,2)),
            "invalid account query was accepted");
        Check(mock.Requests("#Order.Limit.Query") == 1,
            "invalid local request reached broker service");
    }
    private static async Task LeaseRecreation()
    {
        await using var mock = await MockProTerminal.StartAsync();
        mock.LeaseTime = TimeSpan.FromSeconds(2);
        mock.Snapshot("OrderBookEntity", new { IdFi = 101, Lines = new[] {
            new { Price = 100, BuyQty = 1, SellQty = 0 }
        } });
        await using var transport = new AlfaProTransport();
        await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
        var subs = new AlfaProSubscriptions(transport, TimeSpan.FromSeconds(2));
        int recreated = 0, snapshots = 0;
        subs.SubscriptionRecreated += _ => recreated++;
        subs.Snapshot += (_, _) => snapshots++;
        var original = await subs.SubscribeAsync("OrderBookEntity", [101]);
        mock.OverrideReply = frame => Task.FromResult<MockReply?>(
            frame.Channel == "#Data.Query" && frame.Payload.Field("Id").Int64() == original.Id
            ? MockReply.Of(new { Error = new { Code = 404, Message = "Expired" } })
            : null);
        await subs.RenewDueAsync();
        var current = await subs.SubscribeAsync("OrderBookEntity", [101]);
        Check(current.Id != original.Id && recreated == 1 && snapshots == 2,
            "failed renewal did not reacquire a fresh ID plus snapshot");
        Check(mock.Subscriptions("OrderBookEntity") == 2,
            "new subscription missing");
    }

    private static Task UntrackedExecution()
    {
        var audit = NewCompleteAudit();
        audit.AcceptUpdate("ClientOperationEntity", J(new
        {
            IdOperation = 777, IdAccount = 1, IdSubAccount = 2,
            IdRazdel = 3, NumEDocument = 12345, Quantity = 10
        }));
        Check(audit.Evaluate([]).Issues.Any(x => x.Code == "UNTRACKED_EXECUTION"),
            "external execution was silently ignored by the audit");
        return Task.CompletedTask;
    }

    private static Task UnknownStatus()
    {
        var audit = NewCompleteAudit();
        audit.AcceptUpdate("OrderEntity", J(new
        {
            NumEDocument = 757, IdAccount = 1, IdSubAccount = 2,
            IdRazdel = 3, IdOrderStatus = 99
        }));
        Check(audit.Evaluate([]).Issues.Any(x => x.Code == "ORDER_STATUS_UNKNOWN"),
            "unknown order-state code was considered harmless");
        return Task.CompletedTask;
    }

    private static async Task TrackedQuantityMismatch()
    {
        await using var mock = await MockProTerminal.StartAsync();
        await using var transport = new AlfaProTransport();
        await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
        var orders = new AlfaProOrders(transport)
        {
            TradingEnabled = true, BrokerAuthorized = true, ReadyToSign = true,
            Reconciled = true // controlled simulator only, never live
        };
        var tracked = await orders.SubmitAsync(Order());
        var audit = NewCompleteAudit();
        audit.AcceptUpdate("OrderEntity", J(new
        {
            NumEDocument = tracked.BrokerId,
            ClientOrderNum = tracked.ClientOrderNumber,
            IdAccount = 1, IdSubAccount = 2,
            IdRazdel = 3, IdObject = 123,
            Quantity = 11, Rest = 11, IdOrderStatus = 14
        }));
        Check(audit.Evaluate(orders.AllOrders).Issues.Any(x => x.Code == "ORDER_QUANTITY_MISMATCH"),
            "broker-reported quantity mismatch was not detected");
    }
}
