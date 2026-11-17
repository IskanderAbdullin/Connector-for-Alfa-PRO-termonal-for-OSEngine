using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace AlfaPro.Core;

/// <summary>How the PRO limit service determines available quantity, in individual units.</summary>
public enum AlfaProLimitBasis
{
    FreeFunds = 3,
    PortfolioValue = 4
}

public sealed record AlfaProLimitQuery(
    long Account,
    long Section,
    long ObjectId,
    long MarketBoardId,
    int Side,
    decimal Price,
    int OrderType,
    AlfaProLimitBasis Basis = AlfaProLimitBasis.FreeFunds);

/// <summary>Read-only broker limit result; amounts are in individual securities, not lots.</summary>
public sealed record AlfaProLimitResult(long Quantity, long QuantityForOwnAssets);

public sealed class AlfaProLimitClient
{
    private readonly AlfaProTransport _transport;
    private readonly TimeSpan _timeout;

    public AlfaProLimitClient(AlfaProTransport transport, TimeSpan? timeout = null)
    {
        _transport = transport;
        _timeout = timeout ?? TimeSpan.FromSeconds(10);
    }

    public async Task<AlfaProLimitResult> QueryAsync(
        AlfaProLimitQuery query, CancellationToken cancellationToken = default)
    {
        if (query.Account <= 0 || query.Section <= 0 || query.ObjectId <= 0
            || query.MarketBoardId <= 0)
            throw new ArgumentException("Missing account, section, security or board ID");
        if (query.Side is not (1 or -1))
            throw new ArgumentException("Direction must be 1 or -1");
        if (query.OrderType is not (1 or 2))
            throw new ArgumentException("Only documented market/limit order types may be queried");
        if (query.Price <= 0)
            throw new ArgumentException("A positive reference price is required for limit calculation");
        if (query.Basis is not (AlfaProLimitBasis.FreeFunds or AlfaProLimitBasis.PortfolioValue))
            throw new ArgumentException("Unknown broker limit basis");

        var result = AlfaProJson.Unwrap(await _transport.RequestAsync(
            "#Order.Limit.Query",
            new
            {
                IdAccount = query.Account,
                IdRazdel = query.Section,
                IdObject = query.ObjectId,
                IdMarketBoard = query.MarketBoardId,
                IdDocumentType = 1,
                BuySell = query.Side,
                Price = query.Price,
                IdOrderType = query.OrderType,
                LimitRequestType = (int)query.Basis
            }, _timeout, cancellationToken).ConfigureAwait(false));

        var q = result.Field("Quantity");
        var own = result.Field("QuantityForOwnAssets");
        if (q.ValueKind is not (JsonValueKind.Number or JsonValueKind.String) ||
            own.ValueKind is not (JsonValueKind.Number or JsonValueKind.String))
            throw new InvalidDataException("Limit response omits broker quantity fields");
        long available = q.Int64(-1);
        long unleveraged = own.Int64(-1);
        if (available < 0 || unleveraged < 0)
            throw new InvalidDataException("Invalid negative or noninteger broker limit response");
        return new AlfaProLimitResult(available, unleveraged);
    }
}
