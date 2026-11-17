using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

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

public enum AlfaSubmissionState
{
    Preparing, OutcomeUnknown, ServiceAccepted, Rejected, Active, PartiallyFilled, Filled, Cancelled
}

public sealed class AlfaOrderRecord
{
    public AlfaOrderRequest Request { get; }
    public long BrokerId { get; internal set; }
    public long ClientOrderNumber { get; internal set; }
    public AlfaSubmissionState State { get; internal set; } = AlfaSubmissionState.Preparing;
    public long RemainingUnits { get; internal set; }
    public bool CancelPending { get; internal set; }
    public long LastVersion { get; internal set; }
    public object Sync { get; } = new();

    public AlfaOrderRecord(AlfaOrderRequest request)
    {
        Request = request;
        RemainingUnits = request.QuantityUnits;
    }
}

/// <summary>Unresolved local order and its latest unverified operator observation.</summary>
public sealed record AlfaProRecoveryCase(
    int LocalOrderId, long BrokerId, long ClientOrderNumber,
    AlfaSubmissionState State, bool CancelPending, string Reason,
    AlfaProOperatorObservation? LatestObservation);

/// <summary>
/// Fail-closed order coordinator. Submit intents are journaled and flushed before transport send.
/// Requests with unknown results are never retried automatically, including after process restart.
/// </summary>
public sealed class AlfaProOrders
{
    private readonly AlfaProTransport _transport;
    private readonly AlfaProOrderJournal? _journal;
    private readonly TimeSpan _timeout;
    private readonly object _submissionSync = new();
    private readonly object _executionSync = new();
    private readonly ConcurrentDictionary<int, AlfaOrderRecord> _local = new();
    private readonly ConcurrentDictionary<long, AlfaOrderRecord> _broker = new();
    private readonly ConcurrentDictionary<long, AlfaOrderRecord> _client = new();
    private readonly ConcurrentDictionary<string, byte> _dedupTrades = new();
    private readonly ConcurrentDictionary<int, AlfaProOperatorObservation> _operatorObservations = new();
    private readonly ConcurrentDictionary<long, JsonElement> _earlyBrokerOrders = new();
    private readonly ConcurrentDictionary<long, JsonElement> _earlyClientOrders = new();
    private readonly ConcurrentDictionary<string, JsonElement> _earlyExecutions = new();

    public event Action<AlfaOrderRecord>? OrderChanged;
    public event Action<JsonElement>? OwnExecution;

    public bool TradingEnabled { get; set; }
    public bool BrokerAuthorized { get; set; }
    public bool ReadyToSign { get; set; }
    public bool Reconciled { get; private set; }

    /// <summary>
    /// This may ONLY be disabled in isolated simulator tests. Production defaults to fail closed.
    /// It does not replace broker state reconciliation.
    /// </summary>
    public bool RequireDurableJournal { get; set; } = true;

    public bool HasDurableJournal => _journal?.Healthy == true;
    public int RecoveredOrderCount { get; }
    public IReadOnlyCollection<AlfaOrderRecord> AllOrders => _local.Values.ToArray();

    public AlfaProOrders(AlfaProTransport transport, TimeSpan? timeout = null,
        AlfaProOrderJournal? journal = null)
    {
        _transport = transport;
        _timeout = timeout ?? TimeSpan.FromSeconds(10);
        _journal = journal;
        if (journal is not null)
        {
            ReplayJournal(journal.Entries);
            RecoveredOrderCount = _local.Count;
        }
        // Never derive readiness from the restored journal alone.
        Reconciled = false;
    }

    private void ReplayJournal(IReadOnlyList<AlfaProJournalEntry> events)
    {
        foreach (var item in events)
        {
            if (item.LocalOrderId <= 0)
                throw new InvalidDataException("Invalid local ID in trading journal");
            if (item.Kind == AlfaProJournalKind.SubmitIntent)
            {
                if (item.Request is null || item.Request.LocalOrderId != item.LocalOrderId ||
                    !_local.TryAdd(item.LocalOrderId, new AlfaOrderRecord(item.Request)))
                    throw new InvalidDataException("Missing request or duplicate submit intent in journal");
                _local[item.LocalOrderId].State = AlfaSubmissionState.OutcomeUnknown;
                continue;
            }

            if (!_local.TryGetValue(item.LocalOrderId, out var order))
                throw new InvalidDataException("Journal event without preceding submit intent");
            switch (item.Kind)
            {
                case AlfaProJournalKind.SubmitAccepted:
                    if (item.BrokerId <= 0 || item.ClientOrderNumber <= 0)
                        throw new InvalidDataException("Acknowledgement has invalid broker identifiers");
                    if ((order.BrokerId > 0 && order.BrokerId != item.BrokerId) ||
                        (order.ClientOrderNumber > 0 && order.ClientOrderNumber != item.ClientOrderNumber))
                        throw new InvalidDataException("Conflicting broker identifiers in journal");
                    if ((_broker.TryGetValue(item.BrokerId, out var otherBroker) &&
                            !ReferenceEquals(otherBroker, order)) ||
                        (_client.TryGetValue(item.ClientOrderNumber, out var otherClient) &&
                            !ReferenceEquals(otherClient, order)))
                        throw new InvalidDataException("Conflicting order identity in durable history");
                    order.BrokerId = item.BrokerId;
                    order.ClientOrderNumber = item.ClientOrderNumber;
                    _broker[order.BrokerId] = order;
                    _client[order.ClientOrderNumber] = order;
                    if (order.State == AlfaSubmissionState.OutcomeUnknown)
                        order.State = AlfaSubmissionState.ServiceAccepted;
                    break;
                case AlfaProJournalKind.SubmitRejected:
                    order.State = AlfaSubmissionState.Rejected;
                    order.CancelPending = false;
                    break;
                case AlfaProJournalKind.BrokerState:
                    if (item.RemainingUnits < 0 ||
                        item.RemainingUnits > order.Request.QuantityUnits ||
                        item.Revision < order.LastVersion ||
                        !Enum.IsDefined(item.State))
                        throw new InvalidDataException("Invalid order state in journal");
                    if (order.BrokerId != item.BrokerId || order.BrokerId <= 0)
                        throw new InvalidDataException("Mismatched broker order in journal state");
                    order.State = item.State;
                    order.RemainingUnits = item.RemainingUnits;
                    order.LastVersion = item.Revision;
                    if (IsTerminal(item.State)) order.CancelPending = false;
                    break;
                case AlfaProJournalKind.OwnExecution:
                    if (item.ExecutionId <= 0 || item.Account != order.Request.Account ||
                        item.BrokerId != order.BrokerId || order.BrokerId <= 0)
                        throw new InvalidDataException("Uncorrelated execution in journal");
                    if (!_dedupTrades.TryAdd(item.Account + ":" + item.ExecutionId, 0))
                        throw new InvalidDataException("Duplicate own execution in durable journal");
                    break;
                case AlfaProJournalKind.CancelIntent:
                    if (order.BrokerId <= 0)
                        throw new InvalidDataException("Cancellation before broker acknowledgement");
                    order.CancelPending = true;
                    break;
                case AlfaProJournalKind.CancelAcknowledged:
                    // A late service acknowledgement MUST NOT resurrect CancelPending
                    // after a terminal Fill/Cancel/Reject was already journaled.
                    if (!IsTerminal(order.State)) order.CancelPending = true;
                    break;
                case AlfaProJournalKind.OperatorObservation:
                    if (item.Observation is null)
                        throw new InvalidDataException("Operator observation lacks evidence");
                    ValidateObservation(item.Observation);
                    _operatorObservations[item.LocalOrderId] = item.Observation;
                    // Human observations never mutate authoritative broker state.
                    break;
                default:
                    throw new InvalidDataException("Unsupported journal event kind");
            }
        }
    }

    private static bool IsTerminal(AlfaSubmissionState state) =>
        state is AlfaSubmissionState.Filled or AlfaSubmissionState.Cancelled or
            AlfaSubmissionState.Rejected;

    private void Persist(AlfaProJournalEntry entry)
    {
        if (_journal is null)
        {
            if (RequireDurableJournal)
                throw new IOException("No durable journal: live trading is blocked");
            return;
        }
        try { _journal.Append(entry); }
        catch
        {
            TradingEnabled = false;
            Reconciled = false;
            throw;
        }
    }

    private void CheckTradingReady()
    {
        if (!TradingEnabled || !BrokerAuthorized || !ReadyToSign || !Reconciled ||
            (RequireDurableJournal && !HasDurableJournal))
            throw new InvalidOperationException(
                "Trading blocked: authorization, reconciliation or durable journal not ready");
    }

    public async Task<AlfaOrderRecord> SubmitAsync(AlfaOrderRequest request,
        CancellationToken ct = default)
    {
        CheckTradingReady();
        if (request.Side is not (1 or -1) || request.Account <= 0 || request.SubAccount <= 0 ||
            request.Section <= 0 || request.ObjectId <= 0 || request.MarketBoardId <= 0 ||
            request.AllowedOrderParamsId <= 0 || request.QuantityUnits <= 0 ||
            (!request.IsMarket && request.LimitPrice <= 0))
            throw new ArgumentException("Invalid order/account/price/volume identifiers");

        var record = new AlfaOrderRecord(request);
        lock (_submissionSync)
        {
            if (_local.ContainsKey(request.LocalOrderId))
                throw new InvalidOperationException(
                    "Local order ID already exists in durable history; never blindly resubmit");
            // Critical ordering: persist and fsync BEFORE creating an in-memory order
            // and, especially, before sending a broker request.
            Persist(new AlfaProJournalEntry(AlfaProJournalKind.SubmitIntent,
                request.LocalOrderId, Request: request));
            if (!_local.TryAdd(request.LocalOrderId, record))
                throw new InvalidOperationException("Concurrent local order ID collision");
            record.State = AlfaSubmissionState.OutcomeUnknown;
        }
        OrderChanged?.Invoke(record);

        var payload = new Dictionary<string, object?>
        {
            ["IdAccount"] = request.Account,
            ["IdSubAccount"] = request.SubAccount,
            ["IdRazdel"] = request.Section,
            ["IdPriceControlType"] = 0,
            ["IdObject"] = request.ObjectId,
            ["BuySell"] = request.Side,
            ["Quantity"] = request.QuantityUnits,
            ["Comment"] = request.Comment,
            ["IdAllowedOrderParams"] = request.AllowedOrderParamsId
        };
        if (!request.IsMarket) payload["LimitPrice"] = request.LimitPrice;
        var reply = AlfaProJson.Unwrap(await _transport.RequestAsync(
            "#Order.Enter.Query", payload, _timeout, ct).ConfigureAwait(false));
        var value = reply.Field("Value");

        if (reply.Field("ResponseStatus").Int64(-1) != 0 ||
            value.Field("ErrorCode").Int64() != 0)
        {
            lock (record.Sync)
            {
                Persist(new AlfaProJournalEntry(AlfaProJournalKind.SubmitRejected,
                    request.LocalOrderId, State: AlfaSubmissionState.Rejected));
                record.State = AlfaSubmissionState.Rejected;
            }
            OrderChanged?.Invoke(record);
            throw new IOException("Order rejected: " + reply.Field("Message").Text()
                + " " + value.Field("ErrorText").Text());
        }

        long clientNum = value.Field("ClientOrderNum").Int64();
        long brokerId = value.Field("NumEDocument").Int64();
        // Missing IDs mean ambiguous outcome, not confirmed success or rejection.
        if (clientNum <= 0 || brokerId <= 0)
            throw new InvalidDataException("Broker acknowledgement lacks verified order identifiers");

        lock (_submissionSync)
        {
            lock (record.Sync)
            {
                if ((_broker.TryGetValue(brokerId, out var otherBroker) &&
                        !ReferenceEquals(otherBroker, record)) ||
                    (_client.TryGetValue(clientNum, out var otherClient) &&
                        !ReferenceEquals(otherClient, record)))
                {
                    // Two requests sharing one broker identity must never be conflated.
                    TradingEnabled = false;
                    Reconciled = false;
                    throw new InvalidDataException(
                        "Conflicting broker order identifiers: explicit reconciliation required");
                }
                Persist(new AlfaProJournalEntry(AlfaProJournalKind.SubmitAccepted,
                    request.LocalOrderId, BrokerId: brokerId, ClientOrderNumber: clientNum));
                record.ClientOrderNumber = clientNum;
                record.BrokerId = brokerId;
                _client[clientNum] = record;
                _broker[brokerId] = record;
                record.State = AlfaSubmissionState.ServiceAccepted;
            }
        }
        // Broker broadcasts may precede the response, including terminal fill states.
        if (_earlyBrokerOrders.TryRemove(brokerId, out var early))
            OnBrokerOrder(early);
        else if (_earlyClientOrders.TryRemove(clientNum, out early))
            OnBrokerOrder(early);
        foreach (var pending in _earlyExecutions.ToArray())
        {
            if (pending.Value.Field("NumEDocument").Int64() != brokerId) continue;
            if (_earlyExecutions.TryRemove(pending.Key, out var execution))
                OnOwnExecution(execution);
        }
        OrderChanged?.Invoke(record);
        return record;
    }

    public async Task CancelAsync(int localOrderId, CancellationToken ct = default)
    {
        CheckTradingReady();
        if (!_local.TryGetValue(localOrderId, out var record))
            throw new KeyNotFoundException("Local order is not tracked");
        lock (record.Sync)
        {
            if (record.BrokerId <= 0)
                throw new InvalidOperationException("Broker order ID is unknown");
            if (IsTerminal(record.State) || record.CancelPending) return;
            Persist(new AlfaProJournalEntry(AlfaProJournalKind.CancelIntent, localOrderId,
                BrokerId: record.BrokerId));
            record.CancelPending = true;
        }
        var request = record.Request;
        var response = AlfaProJson.Unwrap(await _transport.RequestAsync(
            "#Order.Cancel.Query", new
            {
                IdAccount = request.Account,
                IdSubAccount = request.SubAccount,
                IdRazdel = request.Section,
                NumEDocumentBase = record.BrokerId
            }, _timeout, ct).ConfigureAwait(false));
        if (response.Field("ResponseStatus").Int64(-1) != 0 ||
            response.Field("Value").Field("ErrorCode").Int64() != 0)
            throw new IOException("Cancel rejected; broker state must be reconciled");

        lock (record.Sync)
        {
            Persist(new AlfaProJournalEntry(AlfaProJournalKind.CancelAcknowledged,
                localOrderId, BrokerId: record.BrokerId));
            // Never mark the order Cancelled here; only OrderEntity can confirm it.
        }
    }

    public AlfaOrderRecord? Find(long brokerId, long clientOrderNumber)
    {
        if (brokerId > 0 && _broker.TryGetValue(brokerId, out var found)) return found;
        if (clientOrderNumber > 0 && _client.TryGetValue(clientOrderNumber, out found))
            return found;
        return null;
    }

    public AlfaOrderRecord? OnBrokerOrder(JsonElement raw)
    {
        var item = AlfaProJson.Unwrap(raw);
        long brokerId = item.Field("NumEDocument").Int64();
        long clientId = item.Field("ClientOrderNum").Int64();
        var record = Find(brokerId, clientId);
        if (record is not null && !MatchesAccount(item, record)) return null;
        if (record is null)
        {
            if (brokerId > 0) _earlyBrokerOrders[brokerId] = item.Clone();
            else if (clientId > 0) _earlyClientOrders[clientId] = item.Clone();
            return null; // External, manual, or as-yet-uncorrelated broker order.
        }
        long revision = item.Field("OrderStatusRevision").Int64(
            item.Field("Version").Int64());
        long remaining = item.Field("Rest").Int64(-1);
        long status = item.Field("IdOrderStatus").Int64();

        lock (record.Sync)
        {
            if (record.BrokerId > 0 && brokerId > 0 && brokerId != record.BrokerId)
                return null;
            if (revision > 0 && record.LastVersion > revision) return record;
            if (IsTerminal(record.State)) return record;
            long nextRemaining = remaining >= 0 && remaining <= record.Request.QuantityUnits
                ? remaining : record.RemainingUnits;
            AlfaSubmissionState nextState = status switch
            {
                20 => AlfaSubmissionState.Filled,
                21 or 26 => AlfaSubmissionState.Cancelled,
                18 or 25 => AlfaSubmissionState.Rejected,
                14 when nextRemaining < record.Request.QuantityUnits =>
                    AlfaSubmissionState.PartiallyFilled,
                14 => AlfaSubmissionState.Active,
                _ => record.State
            };
            if (nextState == AlfaSubmissionState.Filled) nextRemaining = 0;
            bool unchanged = nextState == record.State &&
                nextRemaining == record.RemainingUnits &&
                (revision <= 0 || record.LastVersion == revision);
            if (unchanged) return record;

            Persist(new AlfaProJournalEntry(AlfaProJournalKind.BrokerState,
                record.Request.LocalOrderId, BrokerId: record.BrokerId,
                State: nextState, RemainingUnits: nextRemaining,
                Revision: Math.Max(revision, record.LastVersion)));
            record.RemainingUnits = nextRemaining;
            record.State = nextState;
            if (revision > 0) record.LastVersion = revision;
            if (IsTerminal(nextState)) record.CancelPending = false;
        }
        OrderChanged?.Invoke(record);
        return record;
    }

    public bool OnOwnExecution(JsonElement raw)
    {
        var item = AlfaProJson.Unwrap(raw);
        long operationId = item.Field("IdOperation").Int64();
        long brokerId = item.Field("NumEDocument").Int64();
        long account = item.Field("IdAccount").Int64();
        string key = account + ":" + operationId;
        if (operationId <= 0 || account <= 0) return false;
        if (brokerId <= 0 || !_broker.TryGetValue(brokerId, out var owner))
        {
            _earlyExecutions[key] = item.Clone();
            return false;
        }
        if (!MatchesAccount(item, owner)) return false;
        lock (_executionSync)
        {
            if (_dedupTrades.ContainsKey(key)) return false;
            Persist(new AlfaProJournalEntry(AlfaProJournalKind.OwnExecution,
                owner.Request.LocalOrderId, BrokerId: brokerId,
                ExecutionId: operationId, Account: account));
            if (!_dedupTrades.TryAdd(key, 0))
                throw new InvalidOperationException("Concurrent execution de-duplication failed");
        }
        OwnExecution?.Invoke(item.Clone());
        return true;
    }

    private static bool MatchesAccount(JsonElement item, AlfaOrderRecord record)
    {
        long account = item.Field("IdAccount").Int64();
        long section = item.Field("IdRazdel").Int64();
        long sub = item.Field("IdSubAccount").Int64();
        return (account <= 0 || account == record.Request.Account) &&
            (section <= 0 || section == record.Request.Section) &&
            (sub <= 0 || sub == record.Request.SubAccount);
    }

    public IReadOnlyList<AlfaProRecoveryCase> GetRecoveryCases()
        => _local.Values.Where(record => !IsTerminal(record.State) || record.CancelPending)
            .OrderBy(record => record.Request.LocalOrderId)
            .Select(record => new AlfaProRecoveryCase(
                record.Request.LocalOrderId,
                record.BrokerId,
                record.ClientOrderNumber,
                record.State,
                record.CancelPending,
                record.State == AlfaSubmissionState.OutcomeUnknown
                    ? "SUBMISSION_OUTCOME_UNKNOWN"
                    : record.CancelPending ? "CANCEL_OUTCOME_UNKNOWN"
                    : "BROKER_ORDER_NOT_PROVEN_TERMINAL",
                _operatorObservations.TryGetValue(record.Request.LocalOrderId, out var observation)
                    ? observation : null))
            .ToArray();

    private static void ValidateObservation(AlfaProOperatorObservation evidence)
    {
        if (!Enum.IsDefined(evidence.Source) || !Enum.IsDefined(evidence.Outcome)
            || string.IsNullOrWhiteSpace(evidence.Reference) ||
            evidence.Reference.Length > 200 || evidence.Reference.Any(char.IsControl) ||
            evidence.Note is null || evidence.Note.Length > 500 ||
            evidence.Note.Any(char.IsControl))
            throw new ArgumentException(
                "Evidence must have a known source, outcome, short external reference and no control characters");
    }

    /// <summary>
    /// Persist a user-reported external observation without changing any order state.
    /// This method never sends a request to the terminal or marks orders reconciled.
    /// </summary>
    public void RecordOperatorObservation(int localOrderId, AlfaProOperatorObservation observation)
    {
        if (observation is null) throw new ArgumentNullException(nameof(observation));
        ValidateObservation(observation);
        if (!_local.TryGetValue(localOrderId, out var record))
            throw new KeyNotFoundException("Unknown local order");
        lock (record.Sync)
        {
            // A review record cannot be added without a healthy journal. Persist first.
            if (_journal is null || !_journal.Healthy)
                throw new IOException("An accessible durable journal is required");
            Persist(new AlfaProJournalEntry(AlfaProJournalKind.OperatorObservation,
                localOrderId, Observation: observation));
            _operatorObservations[localOrderId] = observation;
            MarkUnreconciled();
        }
    }

    public void MarkUnreconciled() => Reconciled = false;
    public void MarkReconciled() => Reconciled = true;
    public void ClearAfterFullReconciliation() { /* Preserve durable execution IDs. */ }
}

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
