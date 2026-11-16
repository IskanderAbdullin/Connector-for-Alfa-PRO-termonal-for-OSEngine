using System.Collections.Concurrent;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AlfaPro.SimulatorTests;

/// <summary>Local in-process PRO-like WebSocket router. Never contacts the broker.</summary>
internal sealed class MockProTerminal : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly SemaphoreSlim _sendGate = new(1, 1);
    private readonly ConcurrentDictionary<string, object[]> _snapshots = new();
    private readonly ConcurrentQueue<RouterFrame> _seen = new();
    private readonly Channel<RouterFrame> _received = Channel.CreateUnbounded<RouterFrame>();
    private WebSocket? _client;
    private long _leaseId = 1000;
    private long _brokerOrderId = 5000;
    private readonly object _socketGate = new();

    public Uri Endpoint { get; private set; } = null!;
    public string ExpectedToken { get; set; } = "mock-token-only";
    public bool RejectAuthentication { get; set; }
    public bool Authorized { get; set; } = true;
    public bool ReadyToSign { get; set; } = true;
    public TimeSpan LeaseTime { get; set; } = TimeSpan.FromMinutes(5);
    public Func<RouterFrame, Task<MockReply?>>? OverrideReply { get; set; }
    public IReadOnlyCollection<RouterFrame> Frames => _seen.ToArray();
    public int Requests(string channel) => _seen.Count(f => f.Command == "request" && f.Channel == channel);
    public int Subscriptions(string type) => _seen.Count(f =>
        f.Command == "request" && f.Channel == "#Data.Query" && f.Payload.Field("Type").Text() == type);

    private MockProTerminal(WebApplication app) => _app = app;

    public static async Task<MockProTerminal> StartAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options => options.Listen(IPAddress.Loopback, 0));
        var app = builder.Build();
        var mock = new MockProTerminal(app);
        app.UseWebSockets();
        app.Map("/router/", mock.HandleAsync);
        await app.StartAsync();
        var feature = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        string address = feature?.Addresses.FirstOrDefault(a => a.Contains("127.0.0.1")) ??
            throw new InvalidOperationException("No Kestrel loopback endpoint");
        mock.Endpoint = new Uri(address.Replace("http://", "ws://") + "/router/");
        return mock;
    }

    public void Snapshot(string entityType, params object[] values) => _snapshots[entityType] = values;

    public async Task<RouterFrame> NextAsync(TimeSpan? timeout = null)
    {
        using var deadline = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(4));
        return await _received.Reader.ReadAsync(deadline.Token);
    }

    private async Task HandleAsync(HttpContext context)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            context.Response.StatusCode = 400;
            return;
        }
        if (RejectAuthentication ||
            context.Request.Headers.Authorization.ToString() != "Bearer " + ExpectedToken)
        {
            context.Response.StatusCode = 401;
            return;
        }
        using var socket = await context.WebSockets.AcceptWebSocketAsync();
        lock (_socketGate) _client = socket;
        try
        {
            while (socket.State == WebSocketState.Open)
            {
                var frame = await ReadFrameAsync(socket, context.RequestAborted);
                if (frame is null) break;
                _seen.Enqueue(frame);
                _received.Writer.TryWrite(frame);
                if (frame.Command == "request")
                {
                    // Replies may be delayed and reordered independently.
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            var response = OverrideReply is null
                                ? DefaultReply(frame)
                                : await OverrideReply(frame) ?? DefaultReply(frame);
                            if (response.Suppress) return;
                            if (response.Delay > TimeSpan.Zero) await Task.Delay(response.Delay);
                            foreach (var b in response.Before)
                                await BroadcastAsync(b.Channel, b.Payload);
                            await ReplyAsync(frame.Id, frame.Channel, response.Payload, response.Fragment);
                        }
                        catch (Exception) { /* client may have disconnected */ }
                    });
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
        finally { lock (_socketGate) { if (ReferenceEquals(_client, socket)) _client = null; } }
    }

    private MockReply DefaultReply(RouterFrame frame)
    {
        if (frame.Channel == "#Data.Query")
        {
            var type = frame.Payload.Field("Type").Text();
            if (!string.IsNullOrEmpty(type))
                return MockReply.Of(new
                {
                    Id = Interlocked.Increment(ref _leaseId),
                    Subscribed = true,
                    Expires = DateTimeOffset.UtcNow.Add(LeaseTime).ToString("O"),
                    Data = _snapshots.TryGetValue(type, out var items) ? items : Array.Empty<object>()
                });
            if (frame.Payload.Field("Id").Int64() > 0)
                return MockReply.Of(new
                {
                    Id = frame.Payload.Field("Id").Int64(),
                    Subscribed = true,
                    Expires = DateTimeOffset.UtcNow.Add(LeaseTime).ToString("O")
                });
            return MockReply.Of(new
            {
                States = new
                {
                    User = new { AuthStatus = Authorized ? 2 : 0 },
                    SignService = new { ReadyToSign }
                }
            });
        }
        if (frame.Channel == "#Order.Enter.Query")
            return MockReply.Of(new
            {
                ResponseStatus = 0, Value = new
                {
                    ClientOrderNum = Interlocked.Increment(ref _brokerOrderId),
                    NumEDocument = Interlocked.Increment(ref _brokerOrderId), ErrorCode = 0
                }
            });
        if (frame.Channel == "#Order.Cancel.Query")
            return MockReply.Of(new { ResponseStatus = 0, Value = new { ErrorCode = 0 } });
        return MockReply.Of(new { ResponseStatus = 0 });
    }

    public Task ReplyAsync(string id, string channel, object payload, bool fragment = false)
        => SendAsync("response", channel, id, payload, fragment);

    public Task BroadcastAsync(string channel, object payload)
        => SendAsync("broadcast", channel, null, payload, false);

    private async Task SendAsync(string command, string channel, string? id, object payload, bool fragment)
    {
        WebSocket? socket;
        lock (_socketGate) socket = _client;
        if (socket is null || socket.State != WebSocketState.Open) return;
        var envelope = new Dictionary<string, object?>
        {
            ["Command"] = command,
            ["Channel"] = channel,
            ["Payload"] = JsonSerializer.Serialize(payload)
        };
        if (id is not null) envelope["Id"] = id;
        var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(envelope));
        await _sendGate.WaitAsync();
        try
        {
            if (fragment && bytes.Length > 2)
            {
                int mid = bytes.Length / 2;
                await socket.SendAsync(bytes.AsMemory(0, mid), WebSocketMessageType.Text, false, CancellationToken.None);
                await socket.SendAsync(bytes.AsMemory(mid), WebSocketMessageType.Text, true, CancellationToken.None);
            }
            else
                await socket.SendAsync(bytes.AsMemory(), WebSocketMessageType.Text, true, CancellationToken.None);
        }
        finally { _sendGate.Release(); }
    }

    private static async Task<RouterFrame?> ReadFrameAsync(WebSocket socket, CancellationToken ct)
    {
        using var ms = new MemoryStream();
        var buffer = new byte[32768];
        WebSocketReceiveResult part;
        do
        {
            part = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), ct);
            if (part.MessageType == WebSocketMessageType.Close) return null;
            if (part.MessageType != WebSocketMessageType.Text) throw new InvalidDataException("Non-UTF8 router frame");
            ms.Write(buffer, 0, part.Count);
            if (ms.Length > 8 * 1024 * 1024) throw new InvalidDataException("Oversized request");
        } while (!part.EndOfMessage);
        using var document = JsonDocument.Parse(ms.ToArray());
        var root = document.RootElement;
        var raw = root.GetProperty("Payload");
        JsonElement payload = default;
        if (raw.ValueKind == JsonValueKind.String)
        {
            using var inner = JsonDocument.Parse(raw.GetString()!);
            payload = inner.RootElement.Clone();
        }
        return new RouterFrame(root.GetProperty("Command").GetString() ?? "",
            root.GetProperty("Channel").GetString() ?? "",
            root.TryGetProperty("Id", out var id) ? id.GetString() ?? "" : "",
            payload, raw.ValueKind == JsonValueKind.String);
    }

    public void DropConnection()
    {
        lock (_socketGate) _client?.Abort();
    }

    public async ValueTask DisposeAsync()
    {
        DropConnection();
        await _app.StopAsync();
        await _app.DisposeAsync();
        _sendGate.Dispose();
        _received.Writer.TryComplete();
    }
}

internal sealed record RouterFrame(string Command, string Channel, string Id, JsonElement Payload, bool PayloadWasString);
internal sealed record MockBroadcast(string Channel, object Payload);
internal sealed record MockReply(object Payload, bool Suppress = false, TimeSpan Delay = default,
    bool Fragment = false, MockBroadcast[]? Early = null)
{
    public IEnumerable<MockBroadcast> Before => Early ?? Array.Empty<MockBroadcast>();
    public static MockReply Of(object payload) => new(payload);
    public static MockReply Silent() => new(new { }, Suppress: true);
}
