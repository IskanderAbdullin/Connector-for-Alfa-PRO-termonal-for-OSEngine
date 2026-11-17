using AlfaPro.Core;
using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace AlfaPro.SimulatorTests;

internal static class DiagnosticStageTests
{
    public static readonly (string Name, Func<Task> Run)[] Tests =
    [
        ("read-only diagnostic connects and never sends an order or cancellation", ReadOnlyProbe),
        ("read-only diagnostic does not include token, account IDs or raw order payloads", Privacy),
        ("read-only diagnostic captures requested market stream without trading", InstrumentStream),
        ("diagnostic refuses external or secure proxy endpoints before connecting", RejectNonLocalEndpoint),
        ("diagnostic refuses absent token and invalid duration", RejectInvalidProbeOptions),
        ("diagnostic supports unauthorized terminal without enabling trading", UnauthorizedProbe),
        ("unresolved local order produces a manual review case", PendingReviewCase),
        ("operator observation is append-only and never marks order filled", NoteDoesNotChangeState),
        ("operator observation survives process restart and preserves unknown outcome", NoteSurvivesRestart),
        ("invalid operator evidence is rejected without journal changes", InvalidEvidence),
        ("operator evidence cannot be added without a durable journal", ReviewRequiresJournal)
    ];

    private static string JournalPath()
    {
        string directory = Path.Combine(Path.GetTempPath(),
            "alfapro-readonly-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        return Path.Combine(directory, "journal.jsonl");
    }

    private static void Clean(string path)
    {
        var directory = Path.GetDirectoryName(path);
        if (directory is not null && Directory.Exists(directory))
            Directory.Delete(directory, recursive: true);
    }

    private static void Check(bool condition, string reason)
    {
        if (!condition) throw new InvalidOperationException(reason);
    }

    private static async Task ExpectError(Func<Task> fn, string reason)
    {
        try { await fn(); }
        catch { return; }
        throw new InvalidOperationException(reason);
    }

    private static AlfaOrderRequest Request(int localId = 10) =>
        new(localId, 111111, 222222, 333333, 444444, 55, 66, 1, 10, 100.01m,
            false, "simulated-confidential");

    private static AlfaProOperatorObservation Evidence() =>
        new(AlfaProEvidenceSource.TerminalOrderHistory, AlfaProObservedOutcome.Filled,
            "external-document-reference-001", "Operator observed a matching row");

    private static async Task ReadOnlyProbe()
    {
        await using var mock = await MockProTerminal.StartAsync();
        mock.Snapshot("ClientAccountEntity", new { IdAccount = 111111 });
        mock.Snapshot("ClientSubAccountEntity", new {
            IdAccount = 111111, IdSubAccount = 222222
        });
        mock.Snapshot("SubAccountRazdelEntity", new {
            IdAccount = 111111, IdSubAccount = 222222, IdRazdel = 333333, IdRazdelGroup = 1
        });
        var report = await AlfaProReadOnlyProbe.RunAsync(
            mock.Endpoint, mock.ExpectedToken, TimeSpan.FromMilliseconds(40));
        Check(report.ConnectedAtCompletion && report.BrokerAuthorized &&
              report.AllRequiredSnapshotsLoaded && report.CurrentSessionAuditClean,
              "read-only terminal status or snapshot audit not received");
        Check(report.NoTradingCommandsWereIssued && !report.SuitableToAuthorizeLiveTrading,
            "probe implied that diagnostics enabled trading");
        Check(mock.Requests("#Order.Enter.Query") == 0 &&
            mock.Requests("#Order.Cancel.Query") == 0 &&
            mock.Requests("#Order.Limit.Query") == 0, "read-only tool called trading service");
    }

    private static async Task Privacy()
    {
        await using var mock = await MockProTerminal.StartAsync();
        mock.ExpectedToken = "PRIVATE_BEARER_TOKEN_777";
        mock.Snapshot("ClientAccountEntity", new { IdAccount = 77777777 });
        mock.Snapshot("ClientSubAccountEntity", new {
            IdAccount = 77777777, IdSubAccount = 88888888
        });
        mock.Snapshot("SubAccountRazdelEntity", new {
            IdAccount = 77777777, IdSubAccount = 88888888,
            IdRazdel = 99999999, IdRazdelGroup = 2
        });
        var report = await AlfaProReadOnlyProbe.RunAsync(
            mock.Endpoint, mock.ExpectedToken, TimeSpan.Zero);
        string json = JsonSerializer.Serialize(report);
        Check(!json.Contains("PRIVATE_BEARER_TOKEN_777", StringComparison.Ordinal) &&
              !json.Contains("77777777", StringComparison.Ordinal) &&
              !json.Contains("88888888", StringComparison.Ordinal) &&
              !json.Contains("99999999", StringComparison.Ordinal),
              "sanitized report included broker credential or account IDs");
        Check(report.ReceivedEntityCounts.ContainsKey("ClientAccountEntity"),
              "count-only account activity was lost");
    }

    private static async Task InstrumentStream()
    {
        await using var mock = await MockProTerminal.StartAsync();
        mock.Snapshot("AssetInfoEntity", new {
            IdObject = 7777, IdObjectGroup = 2, Ticker = "TESTPROBE",
            Instruments = new[] { new {
                IdFi = 123456, IdMarketBoard = 92, RCode = "TQBR", IsLiquid = true
            } }
        });
        mock.Snapshot("FinInfoParamsEntity", new {
            IdFi = 123456, Lot = 10, PriceStep = 0.01
        });
        var task = AlfaProReadOnlyProbe.RunAsync(
            mock.Endpoint, mock.ExpectedToken, TimeSpan.FromMilliseconds(350), 123456);
        var deadline = DateTime.UtcNow.AddSeconds(3);
        while (mock.Subscriptions("OrderBookEntity") == 0 && DateTime.UtcNow < deadline)
            await Task.Delay(10);
        Check(mock.Subscriptions("OrderBookEntity") == 1, "book subscription was not created");
        await mock.BroadcastAsync("#Data.Bus.OrderBookEntity", new { Updated = new[] {
            new { IdFi = 123456, Lines = new[] {
                new { Price = 100, BuyQty = 3, SellQty = 0 }
            } }
        } });
        var report = await task;
        Check(report.InstrumentCount == 1 && report.RequestedInstrumentParametersAvailable &&
              report.MarketDepthEvents >= 1, "read-only instrument events were not counted");
        Check(mock.Requests("#Order.Enter.Query") == 0, "market probe submitted an order");
    }

    private static async Task RejectNonLocalEndpoint()
    {
        await ExpectError(() => AlfaProReadOnlyProbe.RunAsync(
            new Uri("ws://example.com:3366/router/"), "token", TimeSpan.Zero),
            "remote WebSocket URI was accepted by local-only probe");
        await ExpectError(() => AlfaProReadOnlyProbe.RunAsync(
            new Uri("wss://127.0.0.1:3366/router/"), "token", TimeSpan.Zero),
            "unsupported transport URI was accepted");
    }

    private static async Task RejectInvalidProbeOptions()
    {
        await ExpectError(() => AlfaProReadOnlyProbe.RunAsync(
            new Uri("ws://127.0.0.1:3366/router/"), "", TimeSpan.Zero),
            "empty token accepted");
        await ExpectError(() => AlfaProReadOnlyProbe.RunAsync(
            new Uri("ws://127.0.0.1:3366/router/"), "abc", TimeSpan.FromMinutes(3)),
            "probe allowed an unbounded observation");
        await ExpectError(() => AlfaProReadOnlyProbe.RunAsync(
            new Uri("ws://127.0.0.1:3366/router/"), "abc", TimeSpan.Zero, -1),
            "invalid instrument ID accepted");
    }

    private static async Task UnauthorizedProbe()
    {
        await using var mock = await MockProTerminal.StartAsync();
        mock.Authorized = false;
        mock.ReadyToSign = false;
        var report = await AlfaProReadOnlyProbe.RunAsync(
            mock.Endpoint, mock.ExpectedToken, TimeSpan.Zero);
        Check(!report.BrokerAuthorized && !report.SignatureServiceReady &&
              !report.SuitableToAuthorizeLiveTrading, "unauthorized terminal treated as trade-ready");
        Check(mock.Requests("#Order.Enter.Query") == 0, "unauthorized probe sent an order");
    }

    private static Task PendingReviewCase()
    {
        string path = JournalPath();
        try
        {
            using var journal = AlfaProOrderJournal.Open(path);
            journal.Append(new AlfaProJournalEntry(
                AlfaProJournalKind.SubmitIntent, 10, Request: Request()));
            var transport = new AlfaProTransport();
            var orders = new AlfaProOrders(transport, journal: journal);
            var cases = orders.GetRecoveryCases();
            Check(cases.Count == 1 && cases[0].Reason == "SUBMISSION_OUTCOME_UNKNOWN" &&
                  cases[0].BrokerId == 0 && cases[0].LocalOrderId == 10,
                  "unknown order was not added to the operator queue");
            Check(!orders.Reconciled, "review queue authorized trading");
            transport.DisposeAsync().AsTask().GetAwaiter().GetResult();
            return Task.CompletedTask;
        }
        finally { Clean(path); }
    }

    private static Task NoteDoesNotChangeState()
    {
        string path = JournalPath();
        try
        {
            using var journal = AlfaProOrderJournal.Open(path);
            journal.Append(new AlfaProJournalEntry(
                AlfaProJournalKind.SubmitIntent, 10, Request: Request()));
            var transport = new AlfaProTransport();
            var orders = new AlfaProOrders(transport, journal: journal);
            orders.RecordOperatorObservation(10, Evidence());
            var work = orders.GetRecoveryCases().Single();
            Check(work.State == AlfaSubmissionState.OutcomeUnknown &&
                  work.LatestObservation?.Outcome == AlfaProObservedOutcome.Filled &&
                  !orders.Reconciled, "operator note mutated broker state or enabled trading");
            Check(journal.Entries.Last().Kind == AlfaProJournalKind.OperatorObservation,
                  "observation was not saved in append-only journal");
            transport.DisposeAsync().AsTask().GetAwaiter().GetResult();
            return Task.CompletedTask;
        }
        finally { Clean(path); }
    }

    private static Task NoteSurvivesRestart()
    {
        string path = JournalPath();
        try
        {
            using (var journal = AlfaProOrderJournal.Open(path))
            {
                journal.Append(new AlfaProJournalEntry(
                    AlfaProJournalKind.SubmitIntent, 10, Request: Request()));
                var transport = new AlfaProTransport();
                var orders = new AlfaProOrders(transport, journal: journal);
                orders.RecordOperatorObservation(10, Evidence());
                transport.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            using var reopened = AlfaProOrderJournal.Open(path);
            var secondTransport = new AlfaProTransport();
            var restored = new AlfaProOrders(secondTransport, journal: reopened);
            var work = restored.GetRecoveryCases().Single();
            Check(work.LatestObservation?.Source == AlfaProEvidenceSource.TerminalOrderHistory &&
                  work.LatestObservation.Outcome == AlfaProObservedOutcome.Filled &&
                  work.State == AlfaSubmissionState.OutcomeUnknown &&
                  !restored.Reconciled, "operator observation was lost or promoted to broker fact");
            secondTransport.DisposeAsync().AsTask().GetAwaiter().GetResult();
            return Task.CompletedTask;
        }
        finally { Clean(path); }
    }

    private static Task InvalidEvidence()
    {
        string path = JournalPath();
        try
        {
            using var journal = AlfaProOrderJournal.Open(path);
            journal.Append(new AlfaProJournalEntry(
                AlfaProJournalKind.SubmitIntent, 10, Request: Request()));
            var transport = new AlfaProTransport();
            var orders = new AlfaProOrders(transport, journal: journal);
            long before = journal.LastSequence;
            try
            {
                orders.RecordOperatorObservation(10,
                    new AlfaProOperatorObservation(
                        AlfaProEvidenceSource.BrokerSupport, AlfaProObservedOutcome.Filled,
                        " ", "empty reference"));
                throw new InvalidOperationException("empty reference was accepted");
            }
            catch (ArgumentException) { }
            Check(journal.LastSequence == before,
                "invalid evidence appended a journal record");
            try
            {
                orders.RecordOperatorObservation(777, Evidence());
                throw new InvalidOperationException("unknown local ID accepted");
            }
            catch (System.Collections.Generic.KeyNotFoundException) { }
            Check(journal.LastSequence == before, "unknown order was modified");
            transport.DisposeAsync().AsTask().GetAwaiter().GetResult();
            return Task.CompletedTask;
        }
        finally { Clean(path); }
    }

    private static Task ReviewRequiresJournal()
    {
        var transport = new AlfaProTransport();
        var orders = new AlfaProOrders(transport);
        try
        {
            orders.RecordOperatorObservation(10, Evidence());
            throw new InvalidOperationException("missing order/journal was accepted");
        }
        catch (System.Collections.Generic.KeyNotFoundException) { }
        transport.DisposeAsync().AsTask().GetAwaiter().GetResult();
        return Task.CompletedTask;
    }
}
