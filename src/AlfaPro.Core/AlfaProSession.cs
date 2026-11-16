using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Text.Json;

namespace AlfaPro.Core;

public sealed record AlfaInstrument(
    long ObjectId, long FiId, long MarketBoardId, long ObjectGroupId,
    string Ticker, string Name, string Isin, string RCode, bool IsLiquid);

public sealed record AlfaDepthLevel(decimal Price, decimal BidsInLots, decimal AsksInLots);
public sealed record AlfaDepth(long FiId, IReadOnlyList<AlfaDepthLevel> Levels, DateTimeOffset ReceivedUtc);
public sealed record AlfaPublicTrade(long FiId, string TradeNumber, decimal Price,
    decimal QuantityUnits, int Side, DateTimeOffset Time);

public static class AlfaProMarketData
{
    public static AlfaDepth? ParseDepth(JsonElement raw)
    {
        var item = AlfaProJson.Unwrap(raw);
        long id = item.Field("IdFi").Int64();
        if (id <= 0) return null;
        var lines = new List<AlfaDepthLevel>();
        foreach (var row in AlfaProJson.Items(item.Field("Lines")))
        {
            decimal p = row.Field("Price").Decimal();
            decimal bid = row.Field("BuyQty").Decimal();
            decimal ask = row.Field("SellQty").Decimal();
            if (p <= 0 || bid < 0 || ask < 0) return null;
            if (bid > 0 || ask > 0) lines.Add(new AlfaDepthLevel(p, bid, ask));
        }
        return new AlfaDepth(id, lines, DateTimeOffset.UtcNow);
    }

    public static AlfaPublicTrade? ParseTrade(JsonElement raw)
    {
        var item = AlfaProJson.Unwrap(raw);
        long id = item.Field("IdFI").Int64(item.Field("IdFi").Int64());
        decimal p = item.Field("Price").Decimal(), quantity = item.Field("Qty").Decimal();
        if (id <= 0 || p <= 0 || quantity <= 0) return null;
        return new AlfaPublicTrade(id, item.Field("TradeNo").Text(), p, quantity,
            (int)item.Field("BuySell").Int64(), item.Field("TradeTime").Date() ?? DateTimeOffset.UtcNow);
    }

    public static IEnumerable<AlfaInstrument> ParseAsset(JsonElement raw)
    {
        var item = AlfaProJson.Unwrap(raw);
        long objectId = item.Field("IdObject").Int64();
        var ticker = item.Field("Ticker").Text();
        var group = item.Field("IdObjectGroup").Int64();
        if (objectId <= 0 || string.IsNullOrWhiteSpace(ticker)) yield break;
        foreach (var instrument in AlfaProJson.Items(item.Field("Instruments")))
        {
            long fi = instrument.Field("IdFi").Int64();
            long board = instrument.Field("IdMarketBoard").Int64();
            if (fi > 0 && board > 0)
                yield return new AlfaInstrument(objectId, fi, board, group, ticker,
                    item.Field("Name").Text(ticker), item.Field("ISIN").Text(),
                    instrument.Field("RCode").Text(), instrument.Field("IsLiquid").Bool());
        }
    }
}

/// <summary>
/// Owns one transport generation. OsEngine handles retries/re-creation at a higher level.
/// </summary>
public sealed class AlfaProSession : IAsyncDisposable
{
    private readonly AlfaProTransport _transport = new();
    private readonly AlfaProSubscriptions _subscriptions;
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentDictionary<long, AlfaInstrument> _instruments = new();
    private readonly ConcurrentDictionary<long, (decimal Lot, decimal Step)> _params = new();
    private readonly ConcurrentDictionary<string, byte> _publicTrades = new();
    private readonly ConcurrentDictionary<long, byte> _marketSubscriptions = new();
    private readonly ConcurrentDictionary<string, byte> _desiredEntityTypes = new();
    private readonly TimeSpan _timeout = TimeSpan.FromSeconds(15);
    private Task? _renewLoop;

    public AlfaProOrders Orders { get; }
    public IReadOnlyCollection<AlfaInstrument> Instruments => _instruments.Values.ToArray();
    public bool IsConnected => _transport.IsConnected;
    public bool Authorized => Orders.BrokerAuthorized;
    public bool ReadyToSign => Orders.ReadyToSign;

    public event Action<string, JsonElement>? EntityUpdated;
    public event Action<string, JsonElement>? EntityDeleted;
    public event Action<AlfaDepth>? DepthUpdated;
    public event Action<AlfaPublicTrade>? PublicTrade;
    public event Action<string>? Error;
    public event Action? Disconnected;
    public event Action? AuthorizationChanged;

    public AlfaProSession()
    {
        _subscriptions = new AlfaProSubscriptions(_transport, _timeout);
        _subscriptions.Snapshot += (type, data) =>
        {
            foreach (var item in AlfaProJson.Items(data)) ProcessEntity(type, item, false);
        };
        Orders = new AlfaProOrders(_transport);
        _transport.Broadcast += ProcessBroadcast;
        _transport.Faulted += ex => Error?.Invoke(ex.Message);
        _transport.Disconnected += () =>
        {
            Orders.MarkUnreconciled();
            Orders.BrokerAuthorized = false;
            Orders.ReadyToSign = false;
            Disconnected?.Invoke();
        };
    }

    public async Task ConnectAsync(Uri endpoint, string token, CancellationToken ct = default)
    {
        await _transport.ConnectAsync(endpoint, token, ct).ConfigureAwait(false);
        await _transport.ListenAsync("#ConnectionState.Bus", ct).ConfigureAwait(false);
        // Official API §4: request an initial connection-state publication.
        try
        {
            var state = await _transport.RequestAsync("#Data.Query",
                new { Init = true, Subscribe = true }, _timeout, ct).ConfigureAwait(false);
            var states = AlfaProJson.Unwrap(state).Field("States");
            if (states.ValueKind == JsonValueKind.Object) UpdateAuthorization(states);
        }
        catch (Exception ex) { Error?.Invoke("Initial connection state unavailable: " + ex.Message); }
        // Account and order state streams must be installed before trading readiness is granted.
        foreach (var type in new[]
        {
            "AssetInfoEntity", "MarketBoardEntity", "AllowedOrderParamEntity",
            "ClientAccountEntity", "ClientSubAccountEntity", "SubAccountRazdelEntity",
            "ClientBalanceEntity", "ClientPositionEntity", "OrderEntity",
            "OrderRevisionEntity", "ClientOperationEntity"
        })
        {
            await SubscribeEntityAsync(type, null, ct).ConfigureAwait(false);
        }
        Orders.MarkReconciled(); // Initial snapshots obtained; broker authorization is checked separately.
        _renewLoop = Task.Run(() => RenewalLoopAsync(_stop.Token));
    }

    private async Task SubscribeEntityAsync(string type, IReadOnlyCollection<long>? keys, CancellationToken ct)
    {
        var key = type + ":" + (keys is null ? string.Empty : string.Join(",", keys.OrderBy(x => x)));
        _desiredEntityTypes.TryAdd(key, 0);
        await _subscriptions.SubscribeAsync(type, keys, ct).ConfigureAwait(false);
    }

    public async Task SubscribeMarketAsync(long fiId, CancellationToken ct = default)
    {
        if (fiId <= 0 || !_instruments.ContainsKey(fiId))
            throw new ArgumentException("Unrecognized IdFi");
        if (!_marketSubscriptions.TryAdd(fiId, 0)) return;
        try
        {
            if (_marketSubscriptions.Count > 30) throw new InvalidOperationException("30 order book limit exceeded");
            await SubscribeEntityAsync("FinInfoParamsEntity", [fiId], ct).ConfigureAwait(false);
            await SubscribeEntityAsync("OrderBookEntity", [fiId], ct).ConfigureAwait(false);
            await SubscribeEntityAsync("AllTradeEntity", [fiId], ct).ConfigureAwait(false);
        }
        catch { _marketSubscriptions.TryRemove(fiId, out _); throw; }
    }

    public (decimal Lot, decimal Step)? Parameters(long fiId)
        => _params.TryGetValue(fiId, out var value) ? value : null;

    public async Task<JsonElement> QueryArchiveAsync(object request, CancellationToken ct = default)
        => await _transport.RequestAsync("#Archive.Query", request, TimeSpan.FromSeconds(30), ct)
            .ConfigureAwait(false);

    private void ProcessBroadcast(string channel, JsonElement payload)
    {
        if (channel == "#ConnectionState.Bus")
        {
            var states = AlfaProJson.Unwrap(payload).Field("States");
            if (states.ValueKind == JsonValueKind.Object) UpdateAuthorization(states);
            return;
        }

        const string prefix = "#Data.Bus.";
        if (!channel.StartsWith(prefix, StringComparison.Ordinal)) return;
        var type = channel[prefix.Length..];
        var data = AlfaProJson.Unwrap(payload);
        foreach (var item in AlfaProJson.Items(data.Field("Updated"))) ProcessEntity(type, item, false);
        foreach (var item in AlfaProJson.Items(data.Field("Deleted"))) ProcessEntity(type, item, true);
    }

    private void UpdateAuthorization(JsonElement states)
    {
        Orders.BrokerAuthorized = states.Field("User").Field("AuthStatus").Int64() == 2;
        Orders.ReadyToSign = states.Field("SignService").Field("ReadyToSign").Bool();
        AuthorizationChanged?.Invoke();
    }

    private void ProcessEntity(string type, JsonElement item, bool deleted)
    {
        if (deleted)
        {
            EntityDeleted?.Invoke(type, item.Clone());
            return;
        }

        switch (type)
        {
            case "AssetInfoEntity":
                foreach (var instrument in AlfaProMarketData.ParseAsset(item))
                    _instruments[instrument.FiId] = instrument;
                break;
            case "FinInfoParamsEntity":
            {
                var fi = item.Field("IdFi").Int64();
                // Field names follow official API 2.2; missing or zero values are not invented.
                if (fi > 0)
                {
                    var lot = item.Field("Lot").Decimal();
                    var step = item.Field("PriceStep").Decimal();
                    if (lot > 0 && step > 0) _params[fi] = (lot, step);
                }
                break;
            }
            case "OrderBookEntity":
            {
                var depth = AlfaProMarketData.ParseDepth(item);
                if (depth is not null) DepthUpdated?.Invoke(depth);
                break;
            }
            case "AllTradeEntity":
            {
                var trade = AlfaProMarketData.ParseTrade(item);
                if (trade is not null && _publicTrades.TryAdd(trade.FiId + ":" + trade.TradeNumber, 0))
                    PublicTrade?.Invoke(trade);
                break;
            }
            case "OrderEntity":
                Orders.OnBrokerOrder(item);
                break;
            case "ClientOperationEntity":
                Orders.OnOwnExecution(item);
                break;
        }
        EntityUpdated?.Invoke(type, item.Clone());
    }

    private async Task RenewalLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(10), ct).ConfigureAwait(false);
                try { await _subscriptions.RenewDueAsync(ct).ConfigureAwait(false); }
                catch (Exception ex)
                {
                    Error?.Invoke("Subscription renewal failed: " + ex.Message);
                    // A lost critical subscription makes state reconciliation unreliable.
                    Orders.MarkUnreconciled();
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        if (_renewLoop is not null)
        {
            try { await _renewLoop.ConfigureAwait(false); } catch (OperationCanceledException) { }
        }
        await _transport.DisposeAsync().ConfigureAwait(false);
        _stop.Dispose();
    }
}
