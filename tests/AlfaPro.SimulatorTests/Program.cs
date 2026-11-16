using AlfaPro.Core;
using AlfaPro.SimulatorTests;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text.Json;

var tests = new (string Name, Func<Task> Run)[]
{
    ("reject invalid bearer authentication", RejectBadToken),
    ("route JSON-string payloads and correlate reordered concurrent responses", RouteAndReorder),
    ("read fragmented WebSocket replies", FragmentedReply),
    ("timeout cleans pending request and ignores late reply", TimeoutAndLateReply),
    ("subscription snapshot, renewal and unsubscribe", SubscriptionLifecycle),
    ("concurrent subscribers do not duplicate underlying subscription", ConcurrentSubscriptions),
    ("failed renewal is reported and does not pretend subscription is healthy", FailedRenewal),
    ("session instrument snapshot, order book and public-trade deduplication", SessionMarketData),
    ("unauthorized and unsigned sessions never send orders", TradingGuards),
    ("order submit, working, partial, execution duplicate and full fill", OrderLifecycle),
    ("order event and execution before submission response", EarlyBrokerEvents),
    ("rejected submit stays rejected", BrokerRejection),
    ("lost acknowledgement never causes automatic order resubmission", UnknownSubmission),
    ("cancellation/execution race converges to filled state", CancelFillRace),
    ("version regression cannot reopen a final order", OutOfOrderRevisions),
    ("socket loss suspends trading and fails outstanding request", ConnectionLoss),
    ("200 concurrent requests are all uniquely correlated", ConcurrentStress),
    ("unknown and invalid market payloads fail conservatively", InvalidMarketData),
    ("31st order book subscription is blocked before contacting the broker", MarketSubscriptionLimit),
    ("500 streamed order books arrive intact and in order", MarketDataBurst),
    ("confirmed cancellation after partial fill preserves executed quantity", CancelAfterPartial),
    ("market orders omit the limit price and retain quantity in pieces", MarketOrderParameters),
    ("events from another account cannot mutate an order", CrossAccountIsolation)
};

int passed = 0, failed = 0;
var duration = System.Diagnostics.Stopwatch.StartNew();
foreach (var test in tests)
{
    var sw = System.Diagnostics.Stopwatch.StartNew();
    try
    {
        await test.Run().WaitAsync(TimeSpan.FromSeconds(20));
        Console.WriteLine($"PASS  {test.Name}  ({sw.ElapsedMilliseconds} ms)");
        passed++;
    }
    catch (Exception ex)
    {
        Console.WriteLine($"FAIL  {test.Name}: {ex}");
        failed++;
    }
}
Console.WriteLine($"TOTAL: {passed} passed, {failed} failed, {tests.Length} scenarios ({duration.Elapsed.TotalSeconds:F1}s)");
if (failed != 0) Environment.ExitCode = 1;

static void Check(bool condition, string reason)
{
    if (!condition) throw new Exception(reason);
}
static async Task Eventually(Func<bool> condition, string reason, int ms = 3000)
{
    var started = System.Diagnostics.Stopwatch.StartNew();
    while (!condition() && started.ElapsedMilliseconds < ms)
        await Task.Delay(10);
    Check(condition(), reason);
}
static async Task ExpectError(Func<Task> action, string reason)
{
    try { await action(); }
    catch (Exception) { return; }
    throw new Exception(reason);
}
static AlfaOrderRequest Order(int localId = 1) =>
    new(localId, 1, 2, 3, 123, 4, 7, 1, 10, 100.01m, false, "simulator");
static object BrokerState(bool auth, bool ready) =>
    new { States = new { User = new { AuthStatus = auth ? 2 : 0 }, SignService = new { ReadyToSign = ready } } };
static AlfaProOrders Ready(AlfaProTransport transport, TimeSpan? timeout = null)
    => new(transport, timeout) { TradingEnabled = true, BrokerAuthorized = true, ReadyToSign = true, Reconciled = true };

static async Task RejectBadToken()
{
    await using var mock = await MockProTerminal.StartAsync();
    await using var transport = new AlfaProTransport();
    await ExpectError(() => transport.ConnectAsync(mock.Endpoint, "wrong-token"),
        "invalid token handshake unexpectedly accepted");
    Check(mock.Frames.Count == 0, "requests sent despite failed handshake");
}

static async Task RouteAndReorder()
{
    await using var mock = await MockProTerminal.StartAsync();
    mock.OverrideReply = frame => Task.FromResult<MockReply?>(
        frame.Channel == "#Echo.Query"
            ? new MockReply(new { Index = frame.Payload.Field("Index").Int64() },
                Delay: frame.Payload.Field("Index").Int64() == 1
                    ? TimeSpan.FromMilliseconds(90) : TimeSpan.FromMilliseconds(10))
            : null);
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    var first = transport.RequestAsync("#Echo.Query", new { Index = 1 }, TimeSpan.FromSeconds(2));
    var second = transport.RequestAsync("#Echo.Query", new { Index = 2 }, TimeSpan.FromSeconds(2));
    var fast = await second;
    var slow = await first;
    Check(fast.Field("Index").Int64() == 2 && slow.Field("Index").Int64() == 1,
        "responses attributed to wrong request IDs");
    var frames = mock.Frames.Where(f => f.Channel == "#Echo.Query").ToArray();
    Check(frames.Length == 2 && frames.All(f => f.PayloadWasString), "payload was not JSON string");
    Check(frames[0].Id != frames[1].Id, "routing request IDs collided");
    Check(transport.PendingRequests == 0, "correlation entries leaked");
}

static async Task FragmentedReply()
{
    await using var mock = await MockProTerminal.StartAsync();
    mock.OverrideReply = f => Task.FromResult<MockReply?>(f.Channel == "#Fragment.Query"
        ? new MockReply(new { Text = "ok", Sequence = 17 }, Fragment: true)
        : null);
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    var reply = await transport.RequestAsync("#Fragment.Query", new { X = true }, TimeSpan.FromSeconds(2));
    Check(reply.Field("Sequence").Int64() == 17, "fragmented response not reassembled");
}

static async Task TimeoutAndLateReply()
{
    await using var mock = await MockProTerminal.StartAsync();
    mock.OverrideReply = f => Task.FromResult<MockReply?>(f.Channel == "#Never.Query" ? MockReply.Silent() : null);
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    await ExpectError(() => transport.RequestAsync("#Never.Query", new { Test = 1 },
        TimeSpan.FromMilliseconds(120)), "pending request did not time out");
    Check(transport.PendingRequests == 0, "timed out request remains registered");
    var original = mock.Frames.Single(f => f.Channel == "#Never.Query");
    await mock.ReplyAsync(original.Id, original.Channel, new { Late = true });
    var good = await transport.RequestAsync("#Healthy.Query", new { }, TimeSpan.FromSeconds(2));
    Check(good.Field("ResponseStatus").Int64(-1) == 0, "late reply poisoned subsequent request");
}

static async Task SubscriptionLifecycle()
{
    await using var mock = await MockProTerminal.StartAsync();
    mock.LeaseTime = TimeSpan.FromSeconds(2);
    mock.Snapshot("OrderBookEntity", new { IdFi = 11, Lines = new[] { new { Price = 100, BuyQty = 3, SellQty = 0 } } });
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    var subscriptions = new AlfaProSubscriptions(transport, TimeSpan.FromSeconds(2));
    int snapshots = 0;
    subscriptions.Snapshot += (type, json) =>
    {
        if (type == "OrderBookEntity" && AlfaProJson.Items(json).Count == 1)
            Interlocked.Increment(ref snapshots);
    };
    var lease = await subscriptions.SubscribeAsync("OrderBookEntity", [11]);
    Check(lease.Id > 0 && snapshots == 1, "missing initial data or ID");
    await subscriptions.RenewDueAsync();
    await Eventually(() => mock.Requests("#Data.Query") == 2, "renewal not sent");
    var renew = mock.Frames.Single(f => f.Channel == "#Data.Query" && f.Payload.Field("Id").Int64() == lease.Id);
    Check(renew.Payload.Field("Type").ValueKind == JsonValueKind.Undefined,
        "renewal should identify existing subscription");
    await subscriptions.UnsubscribeAllAsync();
    await Eventually(() => mock.Frames.Any(f =>
        f.Command == "unlisten" && f.Channel == "#Data.Bus.OrderBookEntity"),
        "channel not unlistened");
}

static async Task ConcurrentSubscriptions()
{
    await using var mock = await MockProTerminal.StartAsync();
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    var subscriptions = new AlfaProSubscriptions(transport, TimeSpan.FromSeconds(3));
    var leases = await Task.WhenAll(Enumerable.Range(0, 16)
        .Select(_ => subscriptions.SubscribeAsync("OrderBookEntity", [101])));
    Check(mock.Subscriptions("OrderBookEntity") == 1,
        "concurrent consumers created duplicate underlying broker subscriptions");
    Check(leases.All(x => x.Id == leases[0].Id), "consumers received different leases");
}

static async Task FailedRenewal()
{
    await using var mock = await MockProTerminal.StartAsync();
    mock.LeaseTime = TimeSpan.FromSeconds(1);
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    var subs = new AlfaProSubscriptions(transport, TimeSpan.FromSeconds(2));
    var lease = await subs.SubscribeAsync("OrderBookEntity", [100]);
    mock.OverrideReply = frame => Task.FromResult<MockReply?>(frame.Channel == "#Data.Query"
        && frame.Payload.Field("Id").Int64() == lease.Id
        ? MockReply.Of(new { Error = new { Code = 503, Message = "subscription expired" } })
        : null);
    await ExpectError(() => subs.RenewDueAsync(), "failed renewal silently accepted");
}

static async Task SessionMarketData()
{
    await using var mock = await MockProTerminal.StartAsync();
    mock.Snapshot("AssetInfoEntity",
        new { IdObject = 123, IdObjectGroup = 5, Ticker = "SIM", Name = "Simulator",
            Instruments = new[] { new { IdFi = 101, IdMarketBoard = 4, RCode = "TQBR", IsLiquid = true } } });
    mock.Snapshot("FinInfoParamsEntity", new { IdFi = 101, Lot = 10, PriceStep = 0.01 });
    await using var session = new AlfaProSession();
    var depths = new ConcurrentQueue<AlfaDepth>();
    var trades = new ConcurrentQueue<AlfaPublicTrade>();
    session.DepthUpdated += depths.Enqueue;
    session.PublicTrade += trades.Enqueue;
    await session.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    Check(session.Authorized && session.ReadyToSign, "authorization snapshot missing");
    Check(session.Instruments.Single().FiId == 101, "instrument snapshot missing");
    await session.SubscribeMarketAsync(101);
    Check(session.Parameters(101) == (10m, 0.01m), "instrument parameters missing");
    await mock.BroadcastAsync("#Data.Bus.OrderBookEntity", new { Updated = new[] {
        new { IdFi = 101, Lines = new[] {
            new { Price = 100.01, BuyQty = 10, SellQty = 0 },
            new { Price = 100.02, BuyQty = 0, SellQty = 13 } } } } });
    await Eventually(() => depths.Count == 1, "book event not delivered");
    Check(depths.Single().Levels.Sum(x => x.BidsInLots) == 10, "book unit conversion wrong");
    var trade = new { IdFI = 101, TradeNo = 77, Price = 100.02, Qty = 20, BuySell = 1,
        TradeTime = DateTimeOffset.UtcNow.ToString("O") };
    await mock.BroadcastAsync("#Data.Bus.AllTradeEntity", new { Updated = new[] { trade, trade } });
    await Eventually(() => trades.Count == 1, "public trades not delivered");
    Check(trades.Single().QuantityUnits == 20, "trade unit conversion wrong");
}

static async Task TradingGuards()
{
    await using var mock = await MockProTerminal.StartAsync();
    mock.Authorized = false;
    mock.ReadyToSign = false;
    await using var session = new AlfaProSession();
    await session.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    session.Orders.TradingEnabled = true;
    await ExpectError(() => session.Orders.SubmitAsync(Order()), "unauthorized order was accepted");
    await mock.BroadcastAsync("#ConnectionState.Bus", BrokerState(true, false));
    await Eventually(() => session.Authorized && !session.ReadyToSign, "authorization update ignored");
    await ExpectError(() => session.Orders.SubmitAsync(Order()), "unsigned order was accepted");
    Check(mock.Requests("#Order.Enter.Query") == 0, "guarded order reached broker API");
}

static async Task OrderLifecycle()
{
    await using var mock = await MockProTerminal.StartAsync();
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    var orders = Ready(transport);
    int fills = 0;
    orders.OwnExecution += _ => Interlocked.Increment(ref fills);
    var record = await orders.SubmitAsync(Order());
    Check(record.BrokerId > 0 && record.State == AlfaSubmissionState.ServiceAccepted, "submit acknowledgement lost");
    await mock.BroadcastAsync("#Data.Bus.OrderEntity", new { Updated = new[] {
        new { NumEDocument = record.BrokerId, ClientOrderNum = record.ClientOrderNumber,
            IdOrderStatus = 14, Rest = 10, Version = 1 } } });
    // Subscribe is not necessary for this mock because its router does not filter unsolicited broadcasts.
    transport.Broadcast += (channel, payload) =>
    {
        if (channel == "#Data.Bus.OrderEntity")
            foreach (var item in AlfaProJson.Items(payload.Field("Updated"))) orders.OnBrokerOrder(item);
        else if (channel == "#Data.Bus.ClientOperationEntity")
            foreach (var item in AlfaProJson.Items(payload.Field("Updated"))) orders.OnOwnExecution(item);
    };
    await mock.BroadcastAsync("#Data.Bus.OrderEntity", new { Updated = new[] {
        new { NumEDocument = record.BrokerId, ClientOrderNum = record.ClientOrderNumber,
            IdOrderStatus = 14, Rest = 6, Version = 2 } } });
    await Eventually(() => record.State == AlfaSubmissionState.PartiallyFilled, "partial fill not reflected");
    var own = new { IdAccount = 1, IdOperation = 779, NumEDocument = record.BrokerId,
        Price = 100.01, Quantity = 4, BuySell = 1 };
    await mock.BroadcastAsync("#Data.Bus.ClientOperationEntity", new { Updated = new[] { own, own } });
    await Eventually(() => fills == 1, "own trade missing or duplicated");
    await mock.BroadcastAsync("#Data.Bus.OrderEntity", new { Updated = new[] {
        new { NumEDocument = record.BrokerId, IdOrderStatus = 20, Rest = 0, Version = 3 } } });
    await Eventually(() => record.State == AlfaSubmissionState.Filled, "full fill not reflected");
    Check(record.RemainingUnits == 0 && fills == 1, "executed quantity or deduplication inconsistent");
}

static async Task EarlyBrokerEvents()
{
    await using var mock = await MockProTerminal.StartAsync();
    mock.OverrideReply = frame => Task.FromResult<MockReply?>(frame.Channel == "#Order.Enter.Query"
        ? new MockReply(
            new { ResponseStatus = 0, Value = new { NumEDocument = 9001,
                ClientOrderNum = 9002, ErrorCode = 0 } },
            Early: new[]
            {
                new MockBroadcast("#Data.Bus.OrderEntity",
                    new { Updated = new[] { new { NumEDocument = 9001, ClientOrderNum = 9002,
                        IdOrderStatus = 20, Rest = 0, Version = 3 } } }),
                new MockBroadcast("#Data.Bus.ClientOperationEntity",
                    new { Updated = new[] { new { IdAccount = 1, IdOperation = 9003,
                        NumEDocument = 9001, Price = 100.01, Quantity = 10 } } })
            })
        : null);
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    var orders = Ready(transport);
    int own = 0;
    orders.OwnExecution += _ => Interlocked.Increment(ref own);
    transport.Broadcast += (ch, raw) =>
    {
        if (ch == "#Data.Bus.OrderEntity")
            foreach (var item in AlfaProJson.Items(raw.Field("Updated"))) orders.OnBrokerOrder(item);
        if (ch == "#Data.Bus.ClientOperationEntity")
            foreach (var item in AlfaProJson.Items(raw.Field("Updated"))) orders.OnOwnExecution(item);
    };
    var record = await orders.SubmitAsync(Order());
    Check(record.State == AlfaSubmissionState.Filled, "early filled status overwritten by service acknowledgement");
    Check(record.RemainingUnits == 0 && own == 1, "execution before acknowledgement lost");
}

static async Task BrokerRejection()
{
    await using var mock = await MockProTerminal.StartAsync();
    mock.OverrideReply = frame => Task.FromResult<MockReply?>(frame.Channel == "#Order.Enter.Query"
        ? MockReply.Of(new { ResponseStatus = 1, Message = "rejected", Value = new { ErrorCode = 7, ErrorText = "invalid" } })
        : null);
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    var orders = Ready(transport);
    await ExpectError(() => orders.SubmitAsync(Order()), "broker rejection ignored");
    Check(orders.AllOrders.Single().State == AlfaSubmissionState.Rejected, "rejected order is not terminal");
    Check(mock.Requests("#Order.Enter.Query") == 1, "rejected submit was repeated");
}

static async Task UnknownSubmission()
{
    await using var mock = await MockProTerminal.StartAsync();
    mock.OverrideReply = frame => Task.FromResult<MockReply?>(frame.Channel == "#Order.Enter.Query"
        ? MockReply.Silent() : null);
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    var orders = Ready(transport, TimeSpan.FromMilliseconds(160));
    await ExpectError(() => orders.SubmitAsync(Order()), "lost submit response reported as success");
    Check(orders.AllOrders.Single().State == AlfaSubmissionState.OutcomeUnknown, "unknown outcome treated as failure");
    await ExpectError(() => orders.SubmitAsync(Order()), "duplicate local order was submitted");
    Check(mock.Requests("#Order.Enter.Query") == 1, "unconfirmed order was sent twice");
}

static async Task CancelFillRace()
{
    await using var mock = await MockProTerminal.StartAsync();
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    var orders = Ready(transport);
    transport.Broadcast += (ch, raw) =>
    {
        if (ch == "#Data.Bus.OrderEntity")
            foreach (var item in AlfaProJson.Items(raw.Field("Updated"))) orders.OnBrokerOrder(item);
    };
    var record = await orders.SubmitAsync(Order());
    orders.OnBrokerOrder(Json("""{"NumEDocument":5012,"IdOrderStatus":14,"Rest":10,"Version":1}"""));
    mock.OverrideReply = frame => Task.FromResult<MockReply?>(frame.Channel == "#Order.Cancel.Query"
        ? new MockReply(new { ResponseStatus = 1, Message = "already filled" }, Early: new[]
        {
            new MockBroadcast("#Data.Bus.OrderEntity", new { Updated = new[] {
                new { NumEDocument = record.BrokerId, IdOrderStatus = 20, Rest = 0, Version = 5 } } })
        }) : null);
    await ExpectError(() => orders.CancelAsync(1), "cancel race should be recognized as rejected request");
    await Eventually(() => record.State == AlfaSubmissionState.Filled, "execution/cancel race lost filled state");
    Check(!record.CancelPending, "cancel pending flag remained after confirmed fill");
    Check(mock.Requests("#Order.Enter.Query") == 1, "cancel race resubmitted order");
}

static async Task OutOfOrderRevisions()
{
    await using var mock = await MockProTerminal.StartAsync();
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    var orders = Ready(transport);
    var record = await orders.SubmitAsync(Order());
    orders.OnBrokerOrder(Json($$"""{"NumEDocument":{{record.BrokerId}},"IdOrderStatus":20,"Rest":0,"Version":9}"""));
    orders.OnBrokerOrder(Json($$"""{"NumEDocument":{{record.BrokerId}},"IdOrderStatus":14,"Rest":10,"Version":1}"""));
    Check(record.State == AlfaSubmissionState.Filled && record.RemainingUnits == 0,
        "older revision reopened filled order or restored quantity");
}

static async Task ConnectionLoss()
{
    await using var mock = await MockProTerminal.StartAsync();
    mock.OverrideReply = f => Task.FromResult<MockReply?>(f.Channel == "#Slow.Query" ? MockReply.Silent() : null);
    await using var session = new AlfaProSession();
    await session.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    Check(!session.Orders.Reconciled, "unverified broker snapshots must never enable trading");
    // The low-level request is tested independently with the same disconnect behavior.
    await using var otherMock = await MockProTerminal.StartAsync();
    otherMock.OverrideReply = f => Task.FromResult<MockReply?>(f.Channel == "#Slow.Query"
        ? MockReply.Silent() : null);
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(otherMock.Endpoint, otherMock.ExpectedToken);
    var pending = transport.RequestAsync("#Slow.Query", new { }, TimeSpan.FromSeconds(4));
    await Eventually(() => otherMock.Requests("#Slow.Query") == 1, "slow request not transmitted");
    otherMock.DropConnection();
    await ExpectError(() => pending, "socket loss left pending request blocked");
    await Eventually(() => transport.PendingRequests == 0, "pending request leaked after socket loss");
    mock.DropConnection();
    await Eventually(() => !session.Orders.Reconciled && !session.Authorized && !session.ReadyToSign,
        "session remained trading-ready after dropped socket");
}

static async Task ConcurrentStress()
{
    await using var mock = await MockProTerminal.StartAsync();
    mock.OverrideReply = f => Task.FromResult<MockReply?>(f.Channel == "#Stress.Query"
        ? new MockReply(new { N = f.Payload.Field("N").Int64() },
            Delay: TimeSpan.FromMilliseconds((int)(f.Payload.Field("N").Int64() % 9)))
        : null);
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    var tasks = Enumerable.Range(0, 200)
        .Select(i => transport.RequestAsync("#Stress.Query", new { N = i }, TimeSpan.FromSeconds(8)))
        .ToArray();
    var results = await Task.WhenAll(tasks);
    Check(results.Select(x => x.Field("N").Int64()).Order().SequenceEqual(Enumerable.Range(0, 200).Select(x => (long)x)),
        "correlated stress results are missing or duplicated");
    Check(mock.Requests("#Stress.Query") == 200 && transport.PendingRequests == 0,
        "stress requests lost or leaked");
}

static Task InvalidMarketData()
{
    Check(AlfaProMarketData.ParseDepth(Json("""{"IdFi":7,"Lines":[{"Price":0,"BuyQty":1,"SellQty":0}]}"""))
          is null, "zero price accepted");
    Check(AlfaProMarketData.ParseDepth(Json("""{"IdFi":7,"Lines":[{"Price":10,"BuyQty":-1,"SellQty":0}]}"""))
          is null, "negative book volume accepted");
    Check(AlfaProMarketData.ParseTrade(Json("""{"IdFI":7,"Price":10,"Qty":0}"""))
          is null, "zero-volume public trade accepted");
    Check(AlfaProQuantities.LotsToUnits(3m, 10m) == 30, "lot conversion");
    return Task.CompletedTask;
}

static async Task MarketSubscriptionLimit()
{
    await using var mock = await MockProTerminal.StartAsync();
    var instruments = Enumerable.Range(1, 31).Select(i => (object)new
    {
        IdObject = 1000 + i, IdObjectGroup = 5, Ticker = "SIM" + i,
        Instruments = new[] { new { IdFi = 100 + i, IdMarketBoard = 4,
            RCode = "TQBR", IsLiquid = true } }
    }).ToArray();
    mock.Snapshot("AssetInfoEntity", instruments);
    await using var session = new AlfaProSession();
    await session.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    Check(session.Instruments.Count == 31, "test fixture instruments were not loaded");
    foreach (var id in Enumerable.Range(101, 30))
        await session.SubscribeMarketAsync(id);
    await ExpectError(() => session.SubscribeMarketAsync(131), "31st order book was accepted");
    Check(mock.Subscriptions("OrderBookEntity") == 30, "excess subscription reached the mock broker");
}

static async Task MarketDataBurst()
{
    await using var mock = await MockProTerminal.StartAsync();
    mock.Snapshot("AssetInfoEntity", new
    {
        IdObject = 107, IdObjectGroup = 5, Ticker = "BURST",
        Instruments = new[] { new { IdFi = 107, IdMarketBoard = 4,
            RCode = "TQBR", IsLiquid = true } }
    });
    await using var session = new AlfaProSession();
    int received = 0, lastQty = 0;
    session.DepthUpdated += depth =>
    {
        Volatile.Write(ref lastQty, (int)depth.Levels[0].BidsInLots);
        Interlocked.Increment(ref received);
    };
    await session.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    await session.SubscribeMarketAsync(107);
    for (int n = 1; n <= 500; n++)
        await mock.BroadcastAsync("#Data.Bus.OrderBookEntity", new
        {
            Updated = new[] { new { IdFi = 107,
                Lines = new[] { new { Price = 100.01, BuyQty = n, SellQty = 0 } } } }
        });
    await Eventually(() => Volatile.Read(ref received) == 500,
        "stream lost one or more order-book updates", 7000);
    Check(Volatile.Read(ref lastQty) == 500, "newest book snapshot was not delivered");
}

static async Task CancelAfterPartial()
{
    await using var mock = await MockProTerminal.StartAsync();
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    var orders = Ready(transport);
    transport.Broadcast += (channel, data) =>
    {
        if (channel == "#Data.Bus.OrderEntity")
            foreach (var item in AlfaProJson.Items(data.Field("Updated")))
                orders.OnBrokerOrder(item);
    };
    var order = await orders.SubmitAsync(Order());
    orders.OnBrokerOrder(Json(JsonSerializer.Serialize(new { NumEDocument = order.BrokerId, Rest = 6, IdOrderStatus = 14, Version = 1 })));
    Check(order.State == AlfaSubmissionState.PartiallyFilled, "partial execution not registered");
    await orders.CancelAsync(1);
    Check(order.CancelPending && order.State == AlfaSubmissionState.PartiallyFilled,
        "successful cancel request was confused with actual cancellation");
    await mock.BroadcastAsync("#Data.Bus.OrderEntity", new
    {
        Updated = new[] { new { NumEDocument = order.BrokerId, Rest = 6, IdOrderStatus = 21, Version = 2 } }
    });
    await Eventually(() => order.State == AlfaSubmissionState.Cancelled,
        "cancellation confirmation was not applied");
    Check(order.RemainingUnits == 6 && !order.CancelPending,
        "partial fill was lost or cancel state was not cleared");
    await orders.CancelAsync(1);
    Check(mock.Requests("#Order.Cancel.Query") == 1, "terminal order was cancelled twice");
}

static async Task MarketOrderParameters()
{
    await using var mock = await MockProTerminal.StartAsync();
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    var orders = Ready(transport);
    var marketOrder = Order() with { IsMarket = true, LimitPrice = 0 };
    await orders.SubmitAsync(marketOrder);
    var enter = mock.Frames.Single(x => x.Channel == "#Order.Enter.Query");
    Check(enter.Payload.Field("Quantity").Int64() == 10, "market order quantity was changed");
    Check(enter.Payload.Field("LimitPrice").ValueKind == JsonValueKind.Undefined,
        "market order transmitted a spurious limit price");
}

static async Task CrossAccountIsolation()
{
    await using var mock = await MockProTerminal.StartAsync();
    await using var transport = new AlfaProTransport();
    await transport.ConnectAsync(mock.Endpoint, mock.ExpectedToken);
    var orders = Ready(transport);
    int count = 0;
    orders.OwnExecution += _ => Interlocked.Increment(ref count);
    var order = await orders.SubmitAsync(Order());
    var externalStatus = Json(JsonSerializer.Serialize(new { NumEDocument = order.BrokerId, IdAccount = 999, IdOrderStatus = 20, Rest = 0, Version = 10 }));
    orders.OnBrokerOrder(externalStatus);
    var externalFill = Json(JsonSerializer.Serialize(new { NumEDocument = order.BrokerId, IdAccount = 999, IdOperation = 6543, Quantity = 10 }));
    orders.OnOwnExecution(externalFill);
    Check(order.State == AlfaSubmissionState.ServiceAccepted,
        "an event explicitly associated with a different account modified the order");
    Check(count == 0, "a trade from another account was attributed to this order");
}

static JsonElement Json(string text)
{
    using var doc = JsonDocument.Parse(text);
    return doc.RootElement.Clone();
}
