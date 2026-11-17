using AlfaPro.Core;
using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace AlfaPro.SimulatorTests;

/// <summary>
/// Durable restart tests. All order requests terminate at the in-process mock broker.
/// Every test uses a fresh real on-disk journal and disposes it before reopening.
/// </summary>
internal static class JournalStageTests
{
    public static readonly (string Name, Func<Task> Run)[] Tests =
    [
        ("journal is mandatory for simulated production order submission", JournalMandatory),
        ("submit intent is durable before the WebSocket request", IntentBeforeNetwork),
        ("lost acknowledgement survives restart as unknown and is not resent", UnknownAfterRestart),
        ("acknowledgement, partial fill and execution replay with broker IDs", FullReplay),
        ("execution deduplication survives process restart", FillDedupAcrossRestart),
        ("cancel in flight survives restart without blind cancellation retry", PendingCancelReplay),
        ("journal checksum corruption is rejected without repair", RejectCorruption),
        ("truncated final journal record is rejected without repair", RejectTruncation),
        ("journal is locked against concurrent writers", ExclusiveOpen),
        ("events without submit intent cannot replay", RejectOrphanEvent),
        ("journal failure blocks a new send before it reaches the broker", JournalFailureNoSend),
        ("replayed records remain unapproved after a clean current-session audit", AuditDoesNotAuthorize)
    ];

    private static string TempPath()
    {
        string root = Path.Combine(Path.GetTempPath(), "alfapro-journal-tests-"
            + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        return Path.Combine(root, "orders.v1.jsonl");
    }

    private static void Cleanup(string path)
    {
        string? directory = Path.GetDirectoryName(path);
        if (directory is not null && Directory.Exists(directory))
            Directory.Delete(directory, recursive: true);
    }

    private static AlfaOrderRequest Request(int localId = 1)
        => new(localId, 1, 2, 3, 123, 4, 7, 1, 10, 100.01m, false, "journal-test");

    private static AlfaProOrders Ready(AlfaProTransport transport, AlfaProOrderJournal? journal,
        TimeSpan? timeout = null)
        => new(transport, timeout, journal)
        {
            TradingEnabled = true, BrokerAuthorized = true, ReadyToSign = true,
            Reconciled = true // Explicit test-only readiness; never derived from a journal.
        };

    private static void Check(bool condition, string reason)
    {
        if (!condition) throw new InvalidOperationException(reason);
    }

    private static async Task ExpectError(Func<Task> action, string reason)
    {
        try { await action(); }
        catch (Exception) { return; }
        throw new InvalidOperationException(reason);
    }

    private static void ExpectOpenFailure(string path, string reason)
    {
        try
        {
            using var invalid = AlfaProOrderJournal.Open(path);
        }
        catch (InvalidDataException) { return; }
        throw new InvalidOperationException(reason);
    }

    private static JsonElement Json(object value)
    {
        using var document = JsonDocument.Parse(JsonSerializer.Serialize(value));
        return document.RootElement.Clone();
    }

    private static async Task JournalMandatory()
    {
        await using var mock = await MockProTerminal.StartAsync();
        await using var transport = new AlfaProTransport();
        await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
        var orders = Ready(transport, null);
        await ExpectError(() => orders.SubmitAsync(Request()),
            "order accepted without a durable journal");
        Check(mock.Requests("#Order.Enter.Query") == 0, "order reached broker without journal");
    }

    private static async Task IntentBeforeNetwork()
    {
        string path = TempPath();
        try
        {
            using var journal = AlfaProOrderJournal.Open(path);
            await using var mock = await MockProTerminal.StartAsync();
            mock.OverrideReply = f => Task.FromResult<MockReply?>(
                f.Channel == "#Order.Enter.Query"
                    ? MockReply.Silent()
                    : null);
            await using var transport = new AlfaProTransport();
            await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
            var orders = Ready(transport, journal, TimeSpan.FromMilliseconds(130));
            await ExpectError(() => orders.SubmitAsync(Request()),
                "silent response should time out");
            Check(mock.Requests("#Order.Enter.Query") == 1, "request was never sent");
            Check(journal.Entries.Count == 1 &&
                journal.Entries[0].Kind == AlfaProJournalKind.SubmitIntent &&
                journal.LastSequence == 1, "submit intent not durably written before send");
            Check(orders.AllOrders.Single().State == AlfaSubmissionState.OutcomeUnknown,
                "timed-out order marked as rejected");
        }
        finally { Cleanup(path); }
    }

    private static async Task UnknownAfterRestart()
    {
        string path = TempPath();
        try
        {
            await using var mock = await MockProTerminal.StartAsync();
            mock.OverrideReply = f => Task.FromResult<MockReply?>(
                f.Channel == "#Order.Enter.Query" ? MockReply.Silent() : null);
            await using var transport = new AlfaProTransport();
            await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
            using (var journal = AlfaProOrderJournal.Open(path))
            {
                var orders = Ready(transport, journal, TimeSpan.FromMilliseconds(140));
                await ExpectError(() => orders.SubmitAsync(Request()),
                    "order with lost acknowledgement was treated as successful");
            }

            using (var reopened = AlfaProOrderJournal.Open(path))
            {
                var restored = Ready(transport, reopened, TimeSpan.FromMilliseconds(140));
                Check(restored.RecoveredOrderCount == 1 &&
                    restored.AllOrders.Single().State == AlfaSubmissionState.OutcomeUnknown,
                    "unknown order outcome did not survive restart");
                await ExpectError(() => restored.SubmitAsync(Request()),
                    "reused local order ID was submitted a second time");
                Check(mock.Requests("#Order.Enter.Query") == 1,
                    "duplicate order was sent after restart");
                restored.MarkUnreconciled();
                await ExpectError(() => restored.SubmitAsync(Request(2)),
                    "journal replay alone authorized a new live order");
            }
        }
        finally { Cleanup(path); }
    }

    private static async Task FullReplay()
    {
        string path = TempPath();
        try
        {
            await using var mock = await MockProTerminal.StartAsync();
            await using var transport = new AlfaProTransport();
            await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
            long brokerId, clientId;
            using (var journal = AlfaProOrderJournal.Open(path))
            {
                var orders = Ready(transport, journal);
                var order = await orders.SubmitAsync(Request());
                brokerId = order.BrokerId;
                clientId = order.ClientOrderNumber;
                orders.OnBrokerOrder(Json(new
                {
                    NumEDocument = brokerId, ClientOrderNum = clientId,
                    IdAccount = 1, IdSubAccount = 2, IdRazdel = 3,
                    IdOrderStatus = 14, Rest = 6, OrderStatusRevision = 2
                }));
                var accepted = orders.OnOwnExecution(Json(new
                {
                    IdAccount = 1, IdSubAccount = 2, IdRazdel = 3,
                    IdOperation = 801, NumEDocument = brokerId,
                    Quantity = 4, Price = 100.01m
                }));
                Check(accepted, "first own fill was rejected");
                Check(journal.Entries.Select(x => x.Kind).SequenceEqual(new[]
                {
                    AlfaProJournalKind.SubmitIntent,
                    AlfaProJournalKind.SubmitAccepted,
                    AlfaProJournalKind.BrokerState,
                    AlfaProJournalKind.OwnExecution
                }), "unexpected event ordering in durable journal");
            }
            using (var reopened = AlfaProOrderJournal.Open(path))
            {
                var restored = new AlfaProOrders(transport, journal: reopened);
                var record = restored.AllOrders.Single();
                Check(record.BrokerId == brokerId && record.ClientOrderNumber == clientId &&
                    record.RemainingUnits == 6 &&
                    record.State == AlfaSubmissionState.PartiallyFilled &&
                    record.LastVersion == 2, "replay lost IDs, partial fill or version");
                Check(!restored.Reconciled && !restored.TradingEnabled,
                    "restart incorrectly authorized trading");
                Check(reopened.LastSequence == 4, "event sequence changed after replay");
            }
        }
        finally { Cleanup(path); }
    }

    private static async Task FillDedupAcrossRestart()
    {
        string path = TempPath();
        try
        {
            await using var mock = await MockProTerminal.StartAsync();
            await using var transport = new AlfaProTransport();
            await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
            long brokerId;
            var execution = new { IdAccount = 1, IdSubAccount = 2, IdRazdel = 3,
                IdOperation = 452, NumEDocument = 0L, Quantity = 10 };
            using (var journal = AlfaProOrderJournal.Open(path))
            {
                var orders = Ready(transport, journal);
                brokerId = (await orders.SubmitAsync(Request())).BrokerId;
                Check(orders.OnOwnExecution(Json(execution with { })) == false,
                    "uncorrelated fill must not be emitted");
                Check(orders.OnOwnExecution(Json(new
                {
                    IdAccount = 1, IdSubAccount = 2, IdRazdel = 3,
                    IdOperation = 453, NumEDocument = brokerId, Quantity = 10
                })), "own fill was not persisted");
            }
            using (var reopened = AlfaProOrderJournal.Open(path))
            {
                var orders = new AlfaProOrders(transport, journal: reopened);
                int count = 0;
                orders.OwnExecution += _ => count++;
                var duplicate = Json(new
                {
                    IdAccount = 1, IdSubAccount = 2, IdRazdel = 3,
                    IdOperation = 453, NumEDocument = brokerId, Quantity = 10
                });
                long originalCount = reopened.LastSequence;
                Check(!orders.OnOwnExecution(duplicate) && count == 0 &&
                    reopened.LastSequence == originalCount,
                    "fill was delivered/journaled a second time after restart");
            }
        }
        finally { Cleanup(path); }
    }

    private static async Task PendingCancelReplay()
    {
        string path = TempPath();
        try
        {
            await using var mock = await MockProTerminal.StartAsync();
            await using var transport = new AlfaProTransport();
            await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
            using (var journal = AlfaProOrderJournal.Open(path))
            {
                var orders = Ready(transport, journal, TimeSpan.FromMilliseconds(130));
                var order = await orders.SubmitAsync(Request());
                orders.OnBrokerOrder(Json(new
                {
                    NumEDocument = order.BrokerId, IdOrderStatus = 14,
                    Rest = 10, OrderStatusRevision = 1
                }));
                mock.OverrideReply = f => Task.FromResult<MockReply?>(
                    f.Channel == "#Order.Cancel.Query" ? MockReply.Silent() : null);
                await ExpectError(() => orders.CancelAsync(1),
                    "simulated missing cancel reply should time out");
                Check(order.CancelPending, "cancel intent was lost");
            }
            using (var reopened = AlfaProOrderJournal.Open(path))
            {
                var orders = Ready(transport, reopened);
                Check(orders.AllOrders.Single().CancelPending,
                    "in-flight cancellation did not survive restart");
                await orders.CancelAsync(1);
                Check(mock.Requests("#Order.Cancel.Query") == 1,
                    "pending cancellation request was repeated after restart");
            }
        }
        finally { Cleanup(path); }
    }

    private static Task RejectCorruption()
    {
        string path = TempPath();
        try
        {
            using (var journal = AlfaProOrderJournal.Open(path))
                journal.Append(new AlfaProJournalEntry(
                    AlfaProJournalKind.SubmitIntent, 1, Request: Request()));
            string text = File.ReadAllText(path);
            const string prefix = "\"Sha256\":\"";
            int index = text.IndexOf(prefix, StringComparison.Ordinal);
            Check(index >= 0, "test fixture does not contain checksum");
            index += prefix.Length;
            char[] chars = text.ToCharArray();
            chars[index] = chars[index] == 'A' ? 'B' : 'A';
            File.WriteAllText(path, new string(chars));
            string unchanged = File.ReadAllText(path);
            ExpectOpenFailure(path, "checksum tampering was not detected");
            Check(File.ReadAllText(path) == unchanged, "corrupt journal was auto-repaired");
            return Task.CompletedTask;
        }
        finally { Cleanup(path); }
    }

    private static Task RejectTruncation()
    {
        string path = TempPath();
        try
        {
            using (var journal = AlfaProOrderJournal.Open(path))
                journal.Append(new AlfaProJournalEntry(
                    AlfaProJournalKind.SubmitIntent, 1, Request: Request()));
            var bytes = File.ReadAllBytes(path);
            File.WriteAllBytes(path, bytes[..^1]);
            ExpectOpenFailure(path, "truncated journal tail was auto-accepted");
            Check(File.ReadAllBytes(path).Length == bytes.Length - 1,
                "truncated journal was silently repaired");
            return Task.CompletedTask;
        }
        finally { Cleanup(path); }
    }

    private static Task ExclusiveOpen()
    {
        string path = TempPath();
        try
        {
            using var first = AlfaProOrderJournal.Open(path);
            bool prevented = false;
            try { using var second = AlfaProOrderJournal.Open(path); }
            catch (IOException) { prevented = true; }
            Check(prevented, "multiple writers opened the same journal simultaneously");
            return Task.CompletedTask;
        }
        finally { Cleanup(path); }
    }

    private static async Task RejectOrphanEvent()
    {
        string path = TempPath();
        try
        {
            using var mock = await MockProTerminal.StartAsync();
            using (var journal = AlfaProOrderJournal.Open(path))
                journal.Append(new AlfaProJournalEntry(
                    AlfaProJournalKind.SubmitAccepted, 1,
                    BrokerId: 8001, ClientOrderNumber: 8002));
            using var replay = AlfaProOrderJournal.Open(path);
            await using var transport = new AlfaProTransport();
            await ExpectError(() => Task.Run(() =>
            {
                _ = new AlfaProOrders(transport, journal: replay);
            }), "orphan acknowledgement was replayed as valid trading history");
        }
        finally { Cleanup(path); }
    }

    private static async Task JournalFailureNoSend()
    {
        string path = TempPath();
        try
        {
            await using var mock = await MockProTerminal.StartAsync();
            await using var transport = new AlfaProTransport();
            await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
            var journal = AlfaProOrderJournal.Open(path);
            var orders = Ready(transport, journal);
            journal.Dispose(); // Simulates unavailable durable storage before submission.
            await ExpectError(() => orders.SubmitAsync(Request()),
                "order sent even though journal had failed");
            Check(mock.Requests("#Order.Enter.Query") == 0,
                "broker received an order without a committed submit intent");
        }
        finally { Cleanup(path); }
    }

    private static async Task AuditDoesNotAuthorize()
    {
        string path = TempPath();
        try
        {
            await using var mock = await MockProTerminal.StartAsync();
            await using var transport = new AlfaProTransport();
            await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
            long brokerId, clientId;
            using (var journal = AlfaProOrderJournal.Open(path))
            {
                var orders = Ready(transport, journal);
                var order = await orders.SubmitAsync(Request());
                brokerId = order.BrokerId;
                clientId = order.ClientOrderNumber;
                orders.OnBrokerOrder(Json(new
                {
                    NumEDocument = brokerId, ClientOrderNum = clientId,
                    IdAccount = 1, IdSubAccount = 2, IdRazdel = 3,
                    IdObject = 123, Quantity = 10, Rest = 10,
                    IdOrderStatus = 14, OrderStatusRevision = 1
                }));
            }
            using (var reopened = AlfaProOrderJournal.Open(path))
            {
                var restored = new AlfaProOrders(transport, journal: reopened);
                var audit = new AlfaProRecoveryAudit();
                audit.AcceptSnapshot("ClientAccountEntity", Json(new[] { new { IdAccount = 1 } }));
                audit.AcceptSnapshot("ClientSubAccountEntity", Json(new[] {
                    new { IdAccount = 1, IdSubAccount = 2 }
                }));
                audit.AcceptSnapshot("SubAccountRazdelEntity", Json(new[] {
                    new { IdAccount = 1, IdSubAccount = 2, IdRazdel = 3 }
                }));
                audit.AcceptSnapshot("ClientBalanceEntity", Json(Array.Empty<object>()));
                audit.AcceptSnapshot("ClientPositionEntity", Json(Array.Empty<object>()));
                audit.AcceptSnapshot("OrderEntity", Json(new[] { new
                {
                    NumEDocument = brokerId, ClientOrderNum = clientId,
                    IdAccount = 1, IdSubAccount = 2, IdRazdel = 3,
                    IdObject = 123, Quantity = 10, Rest = 10,
                    IdOrderStatus = 14
                } }));
                audit.AcceptSnapshot("ClientOperationEntity", Json(Array.Empty<object>()));
                Check(audit.Evaluate(restored.AllOrders).Clean,
                    "clean same-session order snapshot was not recognized");
                Check(!restored.Reconciled,
                    "clean snapshot incorrectly enabled live trading after restart");
                restored.TradingEnabled = restored.BrokerAuthorized =
                    restored.ReadyToSign = true;
                await ExpectError(() => restored.SubmitAsync(Request(2)),
                    "clean read-only audit allowed live trading without operator verification");
            }
        }
        finally { Cleanup(path); }
    }
}
