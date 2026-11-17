using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace AlfaPro.Core;

public enum AlfaProJournalKind
{
    SubmitIntent = 1,
    SubmitAccepted = 2,
    SubmitRejected = 3,
    BrokerState = 4,
    OwnExecution = 5,
    CancelIntent = 6,
    CancelAcknowledged = 7,
    OperatorObservation = 8
}

public enum AlfaProEvidenceSource
{
    TerminalOrderHistory = 1,
    BrokerStatement = 2,
    BrokerSupport = 3
}

public enum AlfaProObservedOutcome
{
    Working = 1,
    Filled = 2,
    Cancelled = 3,
    Rejected = 4,
    StillUncertain = 5
}

/// <summary>Unverified operator assertion with an external reference; NEVER broker-authoritative.</summary>
public sealed record AlfaProOperatorObservation(
    AlfaProEvidenceSource Source, AlfaProObservedOutcome Outcome,
    string Reference, string Note);

/// <summary>
/// Immutable trading-event payload. No access token or terminal credentials are stored.
/// A SubmitIntent is written and flushed BEFORE the order is submitted.
/// </summary>
public sealed record AlfaProJournalEntry(
    AlfaProJournalKind Kind,
    int LocalOrderId,
    AlfaOrderRequest? Request = null,
    long BrokerId = 0,
    long ClientOrderNumber = 0,
    AlfaSubmissionState State = AlfaSubmissionState.OutcomeUnknown,
    long RemainingUnits = -1,
    long Revision = 0,
    long ExecutionId = 0,
    long Account = 0,
    DateTimeOffset RecordedAtUtc = default,
    AlfaProOperatorObservation? Observation = null);

internal sealed record AlfaProJournalLine(long Sequence, string Payload, string Sha256);

/// <summary>
/// Single-writer, checksummed append-only journal. Opening refuses corrupted or truncated logs.
/// Every event is synchronously flushed to stable storage; the file is locked per process.
/// It deliberately has no repair/truncate-on-error mode.
/// </summary>
public sealed class AlfaProOrderJournal : IDisposable
{
    private const long MaxJournalBytes = 128L * 1024 * 1024;
    private const int MaxEventBytes = 256 * 1024;
    private static readonly UTF8Encoding Utf8 = new(false, true);
    private readonly object _sync = new();
    private readonly FileStream _stream;
    private readonly List<AlfaProJournalEntry> _events = [];
    private long _sequence;
    private bool _healthy = true;
    private bool _disposed;

    public string Path { get; }
    public long LastSequence { get { lock (_sync) return _sequence; } }
    public bool Healthy { get { lock (_sync) return _healthy && !_disposed; } }

    public IReadOnlyList<AlfaProJournalEntry> Entries
    {
        get { lock (_sync) return _events.ToArray(); }
    }

    public static AlfaProOrderJournal Open(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Journal file path is required", nameof(path));
        var absolute = System.IO.Path.GetFullPath(
            Environment.ExpandEnvironmentVariables(path));
        Directory.CreateDirectory(System.IO.Path.GetDirectoryName(absolute)!);
        // FileShare.None prevents independent OsEngine instances from editing one account journal.
        var stream = new FileStream(absolute, FileMode.OpenOrCreate, FileAccess.ReadWrite,
            FileShare.None, 4096, FileOptions.WriteThrough);
        try { return new AlfaProOrderJournal(absolute, stream); }
        catch { stream.Dispose(); throw; }
    }

    private AlfaProOrderJournal(string path, FileStream stream)
    {
        Path = path;
        _stream = stream;
        LoadStrict();
    }

    private void LoadStrict()
    {
        if (_stream.Length > MaxJournalBytes)
            throw new InvalidDataException("Journal exceeds configured maximum size");
        if (_stream.Length == 0) return;
        if (_stream.Length > int.MaxValue)
            throw new InvalidDataException("Journal is too large for strict replay");
        byte[] bytes = new byte[checked((int)_stream.Length)];
        _stream.Position = 0;
        _stream.ReadExactly(bytes);
        var input = Utf8.GetString(bytes);
        if (!input.EndsWith("\n", StringComparison.Ordinal))
            throw new InvalidDataException("Journal has a truncated, uncommitted tail; operator repair required");

        var lines = input.Split('\n');
        for (int i = 0; i < lines.Length - 1; i++)
        {
            var line = lines[i];
            if (line.Length == 0)
                throw new InvalidDataException("Empty journal record at line " + (i + 1));
            AlfaProJournalLine? envelope;
            AlfaProJournalEntry? entry;
            try
            {
                envelope = JsonSerializer.Deserialize<AlfaProJournalLine>(line);
                if (envelope is null || envelope.Sequence != _sequence + 1
                    || !string.Equals(envelope.Sha256, Hash(envelope.Sequence, envelope.Payload),
                        StringComparison.Ordinal))
                    throw new InvalidDataException("Invalid sequence or checksum");
                entry = JsonSerializer.Deserialize<AlfaProJournalEntry>(envelope.Payload);
                if (entry is null || entry.LocalOrderId <= 0 || !Enum.IsDefined(entry.Kind))
                    throw new InvalidDataException("Invalid journal event");
            }
            catch (Exception ex) when (ex is JsonException or InvalidDataException or ArgumentException)
            {
                throw new InvalidDataException("Cannot replay journal line " + (i + 1)
                    + ": " + ex.Message, ex);
            }
            _events.Add(entry);
            _sequence++;
        }
        _stream.Seek(0, SeekOrigin.End);
    }

    private static string Hash(long sequence, string payload)
        => Convert.ToHexString(SHA256.HashData(Utf8.GetBytes(
            sequence.ToString(CultureInfo.InvariantCulture) + "\n" + payload)));

    public void Append(AlfaProJournalEntry entry)
    {
        if (entry.LocalOrderId <= 0 || !Enum.IsDefined(entry.Kind))
            throw new ArgumentException("Invalid journal event", nameof(entry));
        lock (_sync)
        {
            if (_disposed || !_healthy)
                throw new IOException("Journal is closed or faulted; live trading is prohibited");
            long sequence = checked(_sequence + 1);
            var stamped = entry.RecordedAtUtc == default
                ? entry with { RecordedAtUtc = DateTimeOffset.UtcNow }
                : entry;
            string payload = JsonSerializer.Serialize(stamped);
            string text = JsonSerializer.Serialize(
                new AlfaProJournalLine(sequence, payload, Hash(sequence, payload))) + "\n";
            byte[] encoded = Utf8.GetBytes(text);
            if (encoded.Length > MaxEventBytes ||
                _stream.Length + encoded.Length > MaxJournalBytes)
                throw new IOException("Journal event or journal file exceeds safety bounds");
            try
            {
                _stream.Seek(0, SeekOrigin.End);
                _stream.Write(encoded);
                // Flush(true) asks the OS to flush file data to stable storage.
                _stream.Flush(true);
                _sequence = sequence;
                _events.Add(stamped);
            }
            catch
            {
                // A partial write is NOT silently repaired or retried.
                _healthy = false;
                throw;
            }
        }
    }

    public void Dispose()
    {
        lock (_sync)
        {
            if (_disposed) return;
            _disposed = true;
            _stream.Dispose();
        }
    }
}
