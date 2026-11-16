using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Text.Json;

namespace AlfaPro.Core;

public sealed record AlfaOrderRequest(
    int LocalOrderId,
    long Account,
    long SubAccount,
    long Section,
    long ObjectId,
    long MarketBoardId,
    int AllowedOrderParamsId,
    int Side,
    long QuantityUnits,
    decimal LimitPrice,
    bool IsMarket,
    string Comment);

public enum AlfaSubmissionState { Preparing, OutcomeUnknown, ServiceAccepted, Rejected, Active, PartiallyFilled, Filled, Cancelled }

public sealed class AlfaOrderRecord
{
    public AlfaOrderRequest Request { get; }
    public long BrokerId { get; internal set; }
    public long ClientOrderNumber { get; internal set; }
    public AlfaSubmissionState State { get; internal set; } = AlfaSubmissionState.Preparing;
    public long RemainingUnits { get; internal set; }
    public HashSet<long> ExecutionIds { get; } = [];
    public bool CancelPending { get; internal set; }
    public long LastVersion { get; internal set; }
    public object Sync { get; } = new();

    public AlfaOrderRecord(AlfaOrderRequest request)
    {
        Request = request;
        RemainingUnits = request.QuantityUnits;
    }
}

/// <summary>
/// Broker-safe order command coordinator. No unknown submission is retried.
/// An acknowledged service request does NOT establish a final broker state.
/// </summary>
public sealed class AlfaProOrders
{
    private readonly AlfaProTransport _transport;
    private readonly TimeSpan _timeout;
    private readonly ConcurrentDictionary<int, AlfaOrderRecord> _local = new();
    private readonly ConcurrentDictionary<long, AlfaOrderRecord> _broker = new();
    private readonly ConcurrentDictionary<long, AlfaOrderRecord> _client = new();
    private readonly ConcurrentDictionary<string, byte> _dedupTrades = new();
    private readonly ConcurrentDictionary<long, JsonElement> _earlyBrokerOrders = new();
    private readonly ConcurrentDictionary<long, JsonElement> _earlyClientOrders = new();
    private readonly ConcurrentDictionary<string, JsonElement> _earlyExecutions = new();

    public event Action<AlfaOrderRecord>? OrderChanged;
    public event Action<JsonElement>? OwnExecution;

    public bool TradingEnabled { get; set; }
    public bool BrokerAuthorized { get; set; }
    public bool ReadyToSign { get; set; }
    public bool Reconciled { get; set; }

    public AlfaProOrders(AlfaProTransport transport, TimeSpan? timeout = null)
    {
        _transport = transport;
        _timeout = timeout ?? TimeSpan.FromSeconds(10);
    }

    public IReadOnlyCollection<AlfaOrderRecord> AllOrders => _local.Values.ToArray();

    public async Task<AlfaOrderRecord> SubmitAsync(AlfaOrderRequest order, CancellationToken ct = default)
    {
        if (!TradingEnabled || !BrokerAuthorized || !ReadyToSign || !Reconciled)
            throw new InvalidOperationException("Trading not ready or not explicitly enabled");
        if (order.Side != 1 && order.Side != -1) throw new ArgumentException("Invalid order side");
        if (order.Account <= 0 || order.SubAccount <= 0 || order.Section <= 0
            || order.ObjectId <= 0 || order.MarketBoardId <= 0 || order.AllowedOrderParamsId <= 0)
            throw new ArgumentException("Missing verified broker/account/order-parameter identifiers");
        if (order.QuantityUnits <= 0 || (!order.IsMarket && order.LimitPrice <= 0))
            throw new ArgumentException("Invalid order quantity or price");

        var record = new AlfaOrderRecord(order);
        if (!_local.TryAdd(order.LocalOrderId, record))
            throw new InvalidOperationException("An order with this local ID already exists; never resubmit blindly");
        record.State = AlfaSubmissionState.OutcomeUnknown;
        OrderChanged?.Invoke(record);
        try
        {
            var payload = new Dictionary<string, object?>
            {
                ["IdAccount"] = order.Account,
                ["IdSubAccount"] = order.SubAccount,
                ["IdRazdel"] = order.Section,
                ["IdPriceControlType"] = 0,
                ["IdObject"] = order.ObjectId,
                ["BuySell"] = order.Side,
                ["Quantity"] = order.QuantityUnits,
                ["Comment"] = order.Comment,
                ["IdAllowedOrderParams"] = order.AllowedOrderParamsId
            };
            if (!order.IsMarket) payload["LimitPrice"] = order.LimitPrice;
            var reply = AlfaProJson.Unwrap(await _transport.RequestAsync(
                "#Order.Enter.Query", payload, _timeout, ct).ConfigureAwait(false));
            var value = reply.Field("Value");
            if (reply.Field("ResponseStatus").Int64(-1) != 0 || value.Field("ErrorCode").Int64() != 0)
            {
                record.State = AlfaSubmissionState.Rejected;
                OrderChanged?.Invoke(record);
                throw new IOException("Order rejected: " + reply.Field("Message").Text()
                    + " " + value.Field("ErrorText").Text());
            }
            record.ClientOrderNumber = value.Field("ClientOrderNum").Int64();
            record.BrokerId = value.Field("NumEDocument").Int64();
            if (record.ClientOrderNumber > 0) _client[record.ClientOrderNumber] = record;
            if (record.BrokerId > 0) _broker[record.BrokerId] = record;
            record.State = AlfaSubmissionState.ServiceAccepted;
            // A broker publication may precede the response to #Order.Enter.Query.
            if (record.BrokerId > 0 && _earlyBrokerOrders.TryRemove(record.BrokerId, out var early))
                OnBrokerOrder(early);
            else if (record.ClientOrderNumber > 0 && _earlyClientOrders.TryRemove(record.ClientOrderNumber, out early))
                OnBrokerOrder(early);
            foreach (var pending in _earlyExecutions.ToArray())
            {
                if (pending.Value.Field("NumEDocument").Int64() != record.BrokerId) continue;
                if (_earlyExecutions.TryRemove(pending.Key, out var execution)) OnOwnExecution(execution);
            }
            OrderChanged?.Invoke(record);
            return record;
        }
        catch
        {
            // State remains UNKNOWN unless broker explicitly rejected. Do not retry automatically.
            throw;
        }
    }

    public async Task CancelAsync(int localOrderId, CancellationToken ct = default)
    {
        if (!_local.TryGetValue(localOrderId, out var record)) throw new KeyNotFoundException();
        if (!TradingEnabled || !BrokerAuthorized || !ReadyToSign || !Reconciled)
            throw new InvalidOperationException("Trading is not ready");
        lock (record.Sync)
        {
            if (record.BrokerId <= 0) throw new InvalidOperationException("Broker order ID is not known");
            if (record.State is AlfaSubmissionState.Filled or AlfaSubmissionState.Cancelled
                or AlfaSubmissionState.Rejected) return;
            if (record.CancelPending) return;
            record.CancelPending = true;
        }
        try
        {
            var req = record.Request;
            var response = AlfaProJson.Unwrap(await _transport.RequestAsync(
                "#Order.Cancel.Query", new
                {
                    IdAccount = req.Account, IdSubAccount = req.SubAccount,
                    IdRazdel = req.Section, NumEDocumentBase = record.BrokerId
                }, _timeout, ct).ConfigureAwait(false));
            if (response.Field("ResponseStatus").Int64(-1) != 0
                || response.Field("Value").Field("ErrorCode").Int64() != 0)
                throw new IOException("Cancel rejected; reconcile broker order state: "
                    + response.Field("Message").Text());
            // Successful request is NOT confirmed cancellation. Await OrderEntity.
        }
        catch
        {
            // Keep CancelPending true until authoritative reconciliation.
            throw;
        }
    }

    public AlfaOrderRecord? Find(long brokerId, long clientOrderNumber)
    {
        if (brokerId > 0 && _broker.TryGetValue(brokerId, out var found)) return found;
        if (clientOrderNumber > 0 && _client.TryGetValue(clientOrderNumber, out found)) return found;
        return null;
    }

    /// <summary>Only apply uniquely correlated broker records; never match by price/volume alone.</summary>
    public AlfaOrderRecord? OnBrokerOrder(JsonElement raw)
    {
        var item = AlfaProJson.Unwrap(raw);
        long brokerId = item.Field("NumEDocument").Int64();
        long clientId = item.Field("ClientOrderNum").Int64();
        var record = Find(brokerId, clientId);
        // Broker IDs must not allow cross-account contamination.
        if (record is not null && !MatchesAccount(item, record)) return null;
        if (record is null)
        {
            if (brokerId > 0) _earlyBrokerOrders[brokerId] = item.Clone();
            else if (clientId > 0) _earlyClientOrders[clientId] = item.Clone();
            return null; // manual order or publication before submission acknowledgement
        }
        var status = item.Field("IdOrderStatus").Int64();
        long rest = item.Field("Rest").Int64(-1);
        long version = item.Field("Version").Int64();
        lock (record.Sync)
        {
            if (version > 0 && record.LastVersion > version) return record;
            if (version > 0) record.LastVersion = version;
            if (brokerId > 0) { record.BrokerId = brokerId; _broker[brokerId] = record; }
            if (clientId > 0) { record.ClientOrderNumber = clientId; _client[clientId] = record; }
            if (rest >= 0 && rest <= record.Request.QuantityUnits) record.RemainingUnits = rest;

            // Never roll back an already-confirmed terminal state with an older event.
            if (record.State is AlfaSubmissionState.Filled or AlfaSubmissionState.Cancelled
                or AlfaSubmissionState.Rejected) return record;
            record.State = status switch
            {
                20 => AlfaSubmissionState.Filled,
                21 => AlfaSubmissionState.Cancelled,
                18 or 25 => AlfaSubmissionState.Rejected,
                14 when record.RemainingUnits < record.Request.QuantityUnits
                    => AlfaSubmissionState.PartiallyFilled,
                14 => AlfaSubmissionState.Active,
                _ => record.State
            };
            if (record.State is AlfaSubmissionState.Filled or AlfaSubmissionState.Cancelled
                or AlfaSubmissionState.Rejected) record.CancelPending = false;
        }
        OrderChanged?.Invoke(record);
        return record;
    }

    public bool OnOwnExecution(JsonElement raw)
    {
        var item = AlfaProJson.Unwrap(raw);
        long id = item.Field("IdOperation").Int64();
        long brokerId = item.Field("NumEDocument").Int64();
        string key = item.Field("IdAccount").Text() + ":" + id;
        if (id <= 0 || _dedupTrades.ContainsKey(key)) return false;
        if (brokerId <= 0 || !_broker.TryGetValue(brokerId, out var owner))
        {
            _earlyExecutions[key] = item.Clone();
            return false;
        }
        if (!MatchesAccount(item, owner)) return false;
        if (!_dedupTrades.TryAdd(key, 0)) return false;
        // OrderEntity.Rest remains authoritative: do not subtract the same fill twice.
        OwnExecution?.Invoke(item.Clone());
        return true;
    }

    private static bool MatchesAccount(JsonElement item, AlfaOrderRecord record)
    {
        // Missing optional fields do not establish a mismatch.
        long account = item.Field("IdAccount").Int64();
        long section = item.Field("IdRazdel").Int64();
        long sub = item.Field("IdSubAccount").Int64();
        return (account <= 0 || account == record.Request.Account)
            && (section <= 0 || section == record.Request.Section)
            && (sub <= 0 || sub == record.Request.SubAccount);
    }

    public void MarkUnreconciled() => Reconciled = false;
    public void MarkReconciled() => Reconciled = true;
    public void ClearAfterFullReconciliation() { /* Keep dedup keys for current session. */ }
}

/// <summary>Exact conversion used only when OsEngine's volume is in lots.</summary>
public static class AlfaProQuantities
{
    public static long LotsToUnits(decimal lots, decimal lotSize)
    {
        if (lots <= 0 || lotSize <= 0) throw new ArgumentOutOfRangeException(nameof(lots));
        var units = checked(lots * lotSize);
        if (units != decimal.Truncate(units) || units > long.MaxValue)
            throw new ArgumentException("Quantity must be an integer number of units");
        return checked((long)units);
    }

    public static decimal UnitsToLots(decimal units, decimal lotSize)
    {
        if (lotSize <= 0) throw new ArgumentOutOfRangeException(nameof(lotSize));
        return units / lotSize;
    }
}
