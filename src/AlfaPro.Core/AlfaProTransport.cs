using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

namespace AlfaPro.Core;

public sealed class AlfaProTransport : IAsyncDisposable
{
    private readonly ClientWebSocket _socket = new();
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly ConcurrentDictionary<string, TaskCompletionSource<JsonElement>> _pending = new();
    private readonly CancellationTokenSource _stop = new();
    private Task? _receiveTask;
    private int _closed;

    public event Action<string, JsonElement>? Broadcast;
    public event Action<Exception>? Faulted;
    public event Action? Disconnected;

    public bool IsConnected => _socket.State == WebSocketState.Open;
    public int PendingRequests => _pending.Count;
    public const int MaxMessageBytes = 8 * 1024 * 1024;

    public async Task ConnectAsync(Uri endpoint, string token, CancellationToken cancellationToken = default)
    {
        if (_receiveTask is not null) throw new InvalidOperationException("Transport already started");
        if (endpoint.Scheme != "ws" && endpoint.Scheme != "wss") throw new ArgumentException("WebSocket endpoint required");
        if (string.IsNullOrWhiteSpace(token)) throw new ArgumentException("Bearer token missing");
        _socket.Options.SetRequestHeader("Authorization", "Bearer " + token);
        await _socket.ConnectAsync(endpoint, cancellationToken).ConfigureAwait(false);
        _receiveTask = Task.Run(() => ReceiveLoopAsync(_stop.Token));
    }

    public Task ListenAsync(string channel, CancellationToken cancellationToken = default)
        => SendAsync("listen", channel, null, null, cancellationToken);

    public Task UnlistenAsync(string channel, CancellationToken cancellationToken = default)
        => SendAsync("unlisten", channel, null, null, cancellationToken);

    public async Task<JsonElement> RequestAsync(
        string channel, object payload, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        if (timeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeout));
        var id = Guid.NewGuid().ToString("D");
        var completion = new TaskCompletionSource<JsonElement>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_pending.TryAdd(id, completion)) throw new InvalidOperationException("Duplicate routing ID");

        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stop.Token);
        deadline.CancelAfter(timeout);
        using var registration = deadline.Token.Register(() => completion.TrySetCanceled(deadline.Token));
        try
        {
            await SendAsync("request", channel, id, payload, deadline.Token).ConfigureAwait(false);
            return await completion.Task.ConfigureAwait(false);
        }
        finally
        {
            _pending.TryRemove(id, out _);
        }
    }

    private async Task SendAsync(
        string command, string channel, string? id, object? payload, CancellationToken ct)
    {
        if (!IsConnected) throw new WebSocketException("PRO terminal WebSocket is not connected");
        // Payload MUST be a serialized JSON string, not a nested object.
        var routing = new Dictionary<string, object?>
        {
            ["Command"] = command,
            ["Channel"] = channel
        };
        if (id is not null) routing["Id"] = id;
        if (payload is not null) routing["Payload"] = JsonSerializer.Serialize(payload);
        var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(routing));

        await _sendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await _socket.SendAsync(bytes.AsMemory(), WebSocketMessageType.Text, true, ct)
                .ConfigureAwait(false);
        }
        finally { _sendLock.Release(); }
    }

    private async Task ReceiveLoopAsync(CancellationToken ct)
    {
        var chunk = new byte[65536];
        try
        {
            while (!ct.IsCancellationRequested && IsConnected)
            {
                using var buffer = new MemoryStream();
                WebSocketReceiveResult frame;
                do
                {
                    frame = await _socket.ReceiveAsync(new ArraySegment<byte>(chunk), ct).ConfigureAwait(false);
                    if (frame.MessageType == WebSocketMessageType.Close) return;
                    if (frame.MessageType != WebSocketMessageType.Text) throw new InvalidDataException("Binary router frame");
                    if (buffer.Length + frame.Count > MaxMessageBytes) throw new InvalidDataException("Router frame too large");
                    buffer.Write(chunk, 0, frame.Count);
                } while (!frame.EndOfMessage);
                ProcessFrame(buffer.ToArray());
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex) { Faulted?.Invoke(ex); }
        finally
        {
            foreach (var item in _pending) item.Value.TrySetException(
                new IOException("Alfa PRO transport closed; request outcome may be unknown"));
            if (Interlocked.Exchange(ref _closed, 1) == 0) Disconnected?.Invoke();
        }
    }

    private void ProcessFrame(byte[] bytes)
    {
        using var doc = JsonDocument.Parse(bytes);
        var root = doc.RootElement;
        var command = root.Field("Command").Text();
        var channel = root.Field("Channel").Text();
        var id = root.Field("Id").Text();
        var payload = root.Field("Payload");
        JsonElement value = default;

        if (payload.ValueKind == JsonValueKind.String)
        {
            var raw = payload.GetString();
            if (!string.IsNullOrEmpty(raw))
            {
                using var inner = JsonDocument.Parse(raw);
                value = inner.RootElement.Clone();
            }
        }
        else if (payload.ValueKind == JsonValueKind.Object) value = payload.Clone();

        if (command == "response" && !string.IsNullOrEmpty(id))
        {
            if (_pending.TryGetValue(id, out var completion))
                completion.TrySetResult(value);
        }
        else if (command == "broadcast")
        {
            try { Broadcast?.Invoke(channel, value); }
            catch (Exception ex) { Faulted?.Invoke(ex); }
        }
        else if (root.Field("Code").ValueKind != JsonValueKind.Undefined)
        {
            Faulted?.Invoke(new InvalidDataException(
                "PRO router error: " + root.Field("Code").Text() + " " + root.Field("Message").Text()));
        }
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        foreach (var item in _pending) item.Value.TrySetCanceled();
        if (_socket.State == WebSocketState.Open)
        {
            try { await _socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "shutdown", CancellationToken.None)
                .ConfigureAwait(false); } catch (WebSocketException) { }
        }
        if (_receiveTask is not null)
        {
            try { await _receiveTask.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
        }
        _socket.Dispose();
        _sendLock.Dispose();
        _stop.Dispose();
    }
}
