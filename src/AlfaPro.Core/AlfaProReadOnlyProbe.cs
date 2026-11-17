using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AlfaPro.Core;

/// <summary>
/// Read-only PRO terminal compatibility probe. No order submit/cancel method is invoked.
/// The generated report omits credentials, account IDs, instrument IDs, prices and raw JSON.
/// </summary>
public static class AlfaProReadOnlyProbe
{
    public sealed record Report(
        DateTimeOffset StartedUtc,
        DateTimeOffset FinishedUtc,
        bool ConnectedAtCompletion,
        bool BrokerAuthorized,
        bool SignatureServiceReady,
        bool AllRequiredSnapshotsLoaded,
        bool CurrentSessionAuditClean,
        int AuditIssueCount,
        IReadOnlyList<string> AuditIssueCodes,
        int InstrumentCount,
        int MarketDepthEvents,
        int PublicTradeEvents,
        int ErrorEventCount,
        bool ConnectionLostDuringObservation,
        bool RequestedInstrumentParametersAvailable,
        IReadOnlyDictionary<string, int> ReceivedEntityCounts,
        bool NoTradingCommandsWereIssued = true,
        bool SuitableToAuthorizeLiveTrading = false);

    /// <summary>
    /// Production probe intentionally limits access to a local WebSocket only.
    /// No journal is opened and trading is never enabled, regardless of auth state.
    /// </summary>
    public static async Task<Report> RunAsync(
        Uri endpoint, string token, TimeSpan observation,
        long? instrumentId = null, CancellationToken cancellationToken = default)
    {
        if (endpoint is null) throw new ArgumentNullException(nameof(endpoint));
        if (endpoint.Scheme != "ws" ||
            !(endpoint.Host.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase) ||
              endpoint.Host.Equals("localhost", StringComparison.OrdinalIgnoreCase) ||
              endpoint.Host.Equals("::1", StringComparison.OrdinalIgnoreCase)))
            throw new ArgumentException("Diagnostic only permits local ws:// loopback endpoints");
        if (string.IsNullOrWhiteSpace(token)) throw new ArgumentException("Bearer token missing");
        if (observation < TimeSpan.Zero || observation > TimeSpan.FromMinutes(2))
            throw new ArgumentOutOfRangeException(nameof(observation));
        if (instrumentId is <= 0) throw new ArgumentOutOfRangeException(nameof(instrumentId));

        var start = DateTimeOffset.UtcNow;
        var counts = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        int books = 0, trades = 0, errors = 0, disconnected = 0;
        await using var session = new AlfaProSession();
        // Never enable trading: this component is strictly a passive diagnostic.
        session.Orders.TradingEnabled = false;
        session.EntityUpdated += (type, _) =>
        {
            if (type is "AssetInfoEntity" or "MarketBoardEntity" or "AllowedOrderParamEntity"
                or "ClientAccountEntity" or "ClientSubAccountEntity"
                or "SubAccountRazdelEntity" or "ClientBalanceEntity"
                or "ClientPositionEntity" or "OrderEntity"
                or "OrderRevisionEntity" or "ClientOperationEntity"
                or "FinInfoParamsEntity" or "OrderBookEntity" or "AllTradeEntity")
                counts.AddOrUpdate(type, 1, (_, n) => checked(n + 1));
        };
        session.DepthUpdated += _ => Interlocked.Increment(ref books);
        session.PublicTrade += _ => Interlocked.Increment(ref trades);
        session.Error += _ => Interlocked.Increment(ref errors);
        session.Disconnected += () => Interlocked.Exchange(ref disconnected, 1);
        await session.ConnectAsync(endpoint, token, cancellationToken).ConfigureAwait(false);
        if (instrumentId.HasValue)
            await session.SubscribeMarketAsync(instrumentId.Value, cancellationToken)
                .ConfigureAwait(false);
        if (observation > TimeSpan.Zero)
            await Task.Delay(observation, cancellationToken).ConfigureAwait(false);
        var audit = session.AuditRecovery();
        bool paramAvailable = instrumentId.HasValue &&
            session.Parameters(instrumentId.Value).HasValue;
        return new Report(start, DateTimeOffset.UtcNow, session.IsConnected,
            session.Authorized, session.ReadyToSign,
            session.Recovery.HasAllSnapshots, audit.Clean,
            audit.Issues.Count, audit.Issues.Select(i => i.Code)
                .Distinct(StringComparer.Ordinal).OrderBy(x => x).ToArray(),
            session.Instruments.Count, Volatile.Read(ref books), Volatile.Read(ref trades),
            Volatile.Read(ref errors), Volatile.Read(ref disconnected) != 0,
            paramAvailable, counts.OrderBy(x => x.Key)
                .ToDictionary(x => x.Key, x => x.Value, StringComparer.Ordinal));
    }
}
