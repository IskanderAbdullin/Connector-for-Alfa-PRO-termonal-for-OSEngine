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

    public AlfaProSubscriptions(AlfaProTransport transport, TimeSpan timeout)
    {
        _transport = transport;
        _timeout = timeout;
    }

    public event Action<string, JsonElement>? Snapshot;

    public async Task<Lease> SubscribeAsync(
        string type, IReadOnlyCollection<long>? keys = null, CancellationToken ct = default)
    {
        string identity = type + ":" + (keys is null ? string.Empty : string.Join(",", keys.OrderBy(x => x)));
        if (_leases.TryGetValue(identity, out var existing)) return existing;

        var channel = "#Data.Bus." + type;
        if (_listened.TryAdd(channel, 0))
        {
            try { await _transport.ListenAsync(channel, ct).ConfigureAwait(false); }
            catch { _listened.TryRemove(channel, out _); throw; }
        }
        var payload = new Dictionary<string, object?> { ["Type"] = type, ["Init"] = true };
        if (keys is { Count: > 0 }) payload["Keys"] = keys;
        var response = AlfaProJson.Unwrap(await _transport.RequestAsync("#Data.Query", payload, _timeout, ct)
            .ConfigureAwait(false));
        AssertSubscribed(response, type);
        var lease = new Lease(identity, type, keys?.ToArray() ?? [], response.Field("Id").Int64(),
            response.Field("Expires").Date() ?? DateTimeOffset.UtcNow.AddMinutes(1));
        if (lease.Id <= 0) throw new InvalidDataException("Subscription ID missing: " + type);
        _leases[identity] = lease;
        var data = response.Field("Data");
        if (data.ValueKind == JsonValueKind.Array) Snapshot?.Invoke(type, data);
        return lease;
    }

    public async Task RenewDueAsync(CancellationToken ct = default)
    {
        foreach (var lease in _leases.Values)
        {
            if (lease.Expires - DateTimeOffset.UtcNow > TimeSpan.FromSeconds(30)) continue;
            try
            {
                var payload = new { Id = lease.Id };
                var response = AlfaProJson.Unwrap(await _transport.RequestAsync("#Data.Query", payload, _timeout, ct)
                    .ConfigureAwait(false));
                AssertSubscribed(response, lease.Type);
                lease.Expires = response.Field("Expires").Date() ?? DateTimeOffset.UtcNow.AddSeconds(30);
            }
            catch
            {
                _leases.TryRemove(lease.Key, out _);
                throw;
            }
        }
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
