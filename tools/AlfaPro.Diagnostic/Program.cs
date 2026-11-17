using AlfaPro.Core;
using System.Globalization;
using System.Text;
using System.Text.Json;

static string Usage() => """
Alfa PRO read-only terminal and recovery diagnostic (.NET 10).
Commands:
  probe [--endpoint ws://127.0.0.1:3366/router/] [--seconds 5]
        [--fi ID] [--output sanitized-report.json]
  review --journal path/to/orders.v1.jsonl
  note --journal path/to/orders.v1.jsonl --id LOCAL_ID
       --source TerminalOrderHistory|BrokerStatement|BrokerSupport
       --outcome Working|Filled|Cancelled|Rejected|StillUncertain
       --ref EXTERNAL_DOCUMENT_REFERENCE [--note TEXT]
No order submissions, cancellations or authentication changes are possible in this tool.
For 'probe' set ALFA_PRO_TOKEN in the environment, or enter it at the hidden prompt.
The diagnostic accepts only local ws:// endpoints and never writes the token into reports.
'Note' records an operator claim, NOT a broker-confirmed reconciliation.
""";

static Dictionary<string, string> Options(string[] arguments, int from)
{
    var result = new Dictionary<string, string>(StringComparer.Ordinal);
    for (int i = from; i < arguments.Length; i += 2)
    {
        if (i + 1 >= arguments.Length || !arguments[i].StartsWith("--", StringComparison.Ordinal))
            throw new ArgumentException("Every option must have a value: " + arguments[i]);
        if (!result.TryAdd(arguments[i], arguments[i + 1]))
            throw new ArgumentException("Duplicate option: " + arguments[i]);
    }
    return result;
}

static string Option(Dictionary<string, string> options, string key, string? fallback = null)
{
    if (options.TryGetValue(key, out var value)) return value;
    if (fallback is not null) return fallback;
    throw new ArgumentException("Missing required option " + key);
}

static void EnsureKnownOptions(Dictionary<string, string> options, params string[] allowed)
{
    foreach (var name in options.Keys)
        if (!allowed.Contains(name, StringComparer.Ordinal))
            throw new ArgumentException("Unsupported option: " + name);
}

static string ReadHiddenToken()
{
    string? value = Environment.GetEnvironmentVariable("ALFA_PRO_TOKEN");
    if (!string.IsNullOrWhiteSpace(value)) return value;
    if (Console.IsInputRedirected)
        throw new ArgumentException("ALFA_PRO_TOKEN must be set for noninteractive execution");
    Console.Error.Write("Alfa PRO Bearer token (hidden): ");
    var buffer = new StringBuilder();
    while (true)
    {
        var key = Console.ReadKey(intercept: true);
        if (key.Key == ConsoleKey.Enter) break;
        if (key.Key == ConsoleKey.Backspace)
        {
            if (buffer.Length > 0) buffer.Length--;
        }
        else if (!char.IsControl(key.KeyChar) && buffer.Length < 8192)
            buffer.Append(key.KeyChar);
    }
    Console.Error.WriteLine();
    value = buffer.ToString();
    buffer.Clear();
    if (string.IsNullOrWhiteSpace(value)) throw new ArgumentException("Empty token");
    return value;
}

static string Json(object item) =>
    JsonSerializer.Serialize(item, new JsonSerializerOptions { WriteIndented = true });

static async Task<int> RunAsync(string[] args)
{
    if (args.Length == 0 || args[0] is "-h" or "--help" or "help")
    {
        Console.WriteLine(Usage());
        return 0;
    }
    var o = Options(args, 1);
    if (args[0] == "probe")
    {
        EnsureKnownOptions(o, "--endpoint", "--seconds", "--fi", "--output");
        var endpoint = new Uri(Option(o, "--endpoint", "ws://127.0.0.1:3366/router/"));
        int seconds = int.Parse(Option(o, "--seconds", "5"), CultureInfo.InvariantCulture);
        if (seconds is < 0 or > 120)
            throw new ArgumentOutOfRangeException(nameof(seconds));
        long? fi = o.TryGetValue("--fi", out var input)
            ? long.Parse(input, CultureInfo.InvariantCulture) : null;
        // A probe never opens the order journal or enables trading.
        var report = await AlfaProReadOnlyProbe.RunAsync(
            endpoint, ReadHiddenToken(), TimeSpan.FromSeconds(seconds), fi);
        string text = Json(report);
        Console.WriteLine(text);
        if (o.TryGetValue("--output", out var output))
        {
            var path = Path.GetFullPath(output);
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            var tmp = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
            try
            {
                File.WriteAllText(tmp, text);
                File.Move(tmp, path, overwrite: true);
            }
            finally { if (File.Exists(tmp)) File.Delete(tmp); }
        }
        return 0; // A non-clean broker snapshot is diagnostic data, not API connectivity failure.
    }

    if (args[0] is "review" or "note")
    {
        if (args[0] == "review") EnsureKnownOptions(o, "--journal");
        else EnsureKnownOptions(o, "--journal", "--id", "--source",
            "--outcome", "--ref", "--note");
        using var journal = AlfaProOrderJournal.Open(Option(o, "--journal"));
        await using var transport = new AlfaProTransport(); // No ConnectAsync call, ever.
        var orders = new AlfaProOrders(transport, journal: journal);
        if (args[0] == "note")
        {
            int local = int.Parse(Option(o, "--id"), CultureInfo.InvariantCulture);
            var source = Enum.Parse<AlfaProEvidenceSource>(Option(o, "--source"), ignoreCase: false);
            var outcome = Enum.Parse<AlfaProObservedOutcome>(Option(o, "--outcome"), ignoreCase: false);
            orders.RecordOperatorObservation(local, new AlfaProOperatorObservation(
                source, outcome, Option(o, "--ref"), Option(o, "--note", "")));
            Console.WriteLine("Operator observation appended. Broker state unchanged; trading remains blocked.");
        }
        else
        {
            var cases = orders.GetRecoveryCases();
            Console.WriteLine(Json(new
            {
                JournalEntries = journal.LastSequence,
                RestoredOrders = orders.RecoveredOrderCount,
                UnresolvedCases = cases.Count,
                Cases = cases,
                LiveTradingAuthorized = false
            }));
        }
        return 0;
    }
    throw new ArgumentException("Unknown command. Use help to see allowed commands.");
}

try
{
    return await RunAsync(args);
}
catch (Exception ex)
{
    // Never print raw network exceptions: provider messages may contain private identifiers.
    string category = ex switch
    {
        ArgumentException => "Invalid option or evidence",
        InvalidDataException => "Invalid or corrupted journal",
        IOException => "Storage or connection unavailable",
        System.Net.WebSockets.WebSocketException => "PRO WebSocket unavailable",
        OperationCanceledException => "Diagnostic cancelled",
        _ => "Diagnostic failed"
    };
    Console.Error.WriteLine(category + " (" + ex.GetType().Name + ").");
    return 2;
}
