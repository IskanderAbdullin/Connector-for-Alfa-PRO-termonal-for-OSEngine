using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Text.Json;

namespace AlfaPro.Core;

/// <summary>Creates, renews and restores #Data.Query subscriptions for one connected transport.</summary>
public sealed class AlfaProSubscriptions
{
    private readonly AlfaProTransport _transport;
    private readonly TimeSpan _timeout;
    private readonly ConcurrentDictionary<string, Lease> _leases = new();
    private readonly ConcurrentDictionary<string, byte> _listened = new();
    private readonly SemaphoreSlim _subscribeGate = new(1, 1);

    public AlfaProSubscriptions(AlfaProTransport transport, TimeSpan timeout)
    {
        _transport = transport;
        _timeout = timeout;
    }

    public event Action<string, JsonElement>? Snapshot;
    public event Action<string>? SubscriptionRecreated;

    public async Task<Lease> SubscribeAsync(
        string type, IReadOnlyCollection<long>? keys = null, CancellationToken ct = default)
    {
        string identity = type + ":" + (keys is null ? string.Empty : string.Join(",", keys.OrderBy(x => x)));
        JsonElement snapshot = default;
        Lease lease;

        // A single gate is intentionally used for the full subscribe handshake:
        // simultaneous OsEngine robots must not allocate duplicate leases.
        await _subscribeGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_leases.TryGetValue(identity, out var existing)) return existing;

            var channel = "#Data.Bus." + type;
            if (_listened.TryAdd(channel, 0))
            {
                try { await _transport.ListenAsync(channel, ct).ConfigureAwait(false); }
                catch { _listened.TryRemove(channel, out _); throw; }
            }
            var payload = new Dictionary<string, object?> { ["Type"] = type, ["Init"] = true };
            if (keys is { Count: > 0 }) payload["Keys"] = keys;
            var response = AlfaProJson.Unwrap(await _transport.RequestAsync(
                "#Data.Query", payload, _timeout, ct).ConfigureAwait(false));
            AssertSubscribed(response, type);
            lease = new Lease(identity, type, keys?.ToArray() ?? [],
                response.Field("Id").Int64(),
                response.Field("Expires").Date() ?? DateTimeOffset.UtcNow.AddMinutes(1));
            if (lease.Id <= 0) throw new InvalidDataException("Subscription ID missing: " + type);
            _leases[identity] = lease;
            var data = response.Field("Data");
            if (data.ValueKind == JsonValueKind.Array) snapshot = data.Clone();
        }
        finally { _subscribeGate.Release(); }

        // Do not invoke external event handlers under a subscription lock.
        if (snapshot.ValueKind == JsonValueKind.Array) Snapshot?.Invoke(type, snapshot);
        return lease;
    }

    /// <summary>
    /// Renew a valid lease or reacquire a new lease and initial snapshot after expiry.
    /// All changes are serialized with SubscribeAsync, so two callers cannot race to
    /// create the same subscription. A failed reacquisition throws and fails closed.
    /// </summary>
    public async Task RenewDueAsync(CancellationToken ct = default)
    {
        var refreshed = new List<(string Type, JsonElement Data)>();
        var recreated = new List<string>();
        await _subscribeGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            foreach (var lease in _leases.Values.ToArray())
            {
                if (lease.Expires - DateTimeOffset.UtcNow > TimeSpan.FromSeconds(30)) continue;
                try
                {
                    var response = AlfaProJson.Unwrap(await _transport.RequestAsync(
                        "#Data.Query", new { Id = lease.Id }, _timeout, ct).ConfigureAwait(false));
                    AssertSubscribed(response, lease.Type);
                    var expiry = response.Field("Expires").Date();
                    if (expiry is null || expiry <= DateTimeOffset.UtcNow)
                        throw new InvalidDataException("Renewal did not supply a valid expiry");
                    lease.Expires = expiry.Value;
                }
                catch (Exception) when (!ct.IsCancellationRequested)
                {
                    // The old lease may be gone. Do not continue using its ID.
                    _leases.TryRemove(lease.Key, out _);
                    var payload = new Dictionary<string, object?>
                    {
                        ["Type"] = lease.Type,
                        ["Init"] = true
                    };
                    if (lease.Keys.Length > 0) payload["Keys"] = lease.Keys;
                    var response = AlfaProJson.Unwrap(await _transport.RequestAsync(
                        "#Data.Query", payload, _timeout, ct).ConfigureAwait(false));
                    AssertSubscribed(response, lease.Type);
                    long newId = response.Field("Id").Int64();
                    var expires = response.Field("Expires").Date();
                    if (newId <= 0 || expires is null || expires <= DateTimeOffset.UtcNow)
                        throw new InvalidDataException("Recreated subscription has invalid ID or expiry");
                    _leases[lease.Key] = new Lease(
                        lease.Key, lease.Type, lease.Keys, newId, expires.Value);
                    recreated.Add(lease.Type);
                    var data = response.Field("Data");
                    if (data.ValueKind == JsonValueKind.Array)
                        refreshed.Add((lease.Type, data.Clone()));
                }
            }
        }
        finally { _subscribeGate.Release(); }

        // Client callbacks must never run under the subscription gate.
        foreach (var type in recreated) SubscriptionRecreated?.Invoke(type);
        foreach (var item in refreshed) Snapshot?.Invoke(item.Type, item.Data);
    }

    public async Task UnsubscribeAllAsync(CancellationToken ct = default)
    {
        foreach (var lease in _leases.Values)
        {
            try
            {
                await _transport.RequestAsync("#Data.Query",
                    new { Type = lease.Type, Id = lease.Id, Unsubscribe = true }, _timeout, ct)
                    .ConfigureAwait(false);
            }
            catch (Exception) { /* best-effort unsubscribe */ }
        }
        _leases.Clear();
        foreach (var channel in _listened.Keys)
        {
            try { await _transport.UnlistenAsync(channel, ct).ConfigureAwait(false); }
            catch (Exception) { }
        }
        _listened.Clear();
    }

    private static void AssertSubscribed(JsonElement response, string type)
    {
        var error = response.Field("Error");
        if (error.Field("Code").Int64() != 0)
            throw new IOException(type + " subscription failed: " + error.Field("Message").Text());
        if (response.Field("Subscribed").ValueKind == JsonValueKind.False)
            throw new IOException(type + " subscription rejected");
    }

    public sealed class Lease(string key, string type, long[] keys, long id, DateTimeOffset expires)
    {
        public string Key { get; } = key;
        public string Type { get; } = type;
        public long[] Keys { get; } = keys;
        public long Id { get; } = id;
        public DateTimeOffset Expires { get; internal set; } = expires;
    }
}
