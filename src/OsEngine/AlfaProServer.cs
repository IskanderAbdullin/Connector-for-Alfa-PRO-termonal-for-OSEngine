using AlfaPro.Core;
using OsEngine.Entity;
using OsEngine.Logging;
using OsEngine.Market.Servers.Entity;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace OsEngine.Market.Servers.AlfaPro
{
    /// <summary>Native OsEngine wrapper around Alfa PRO API v2.2.</summary>
    public sealed class AlfaProServer : AServer
    {
        public AlfaProServer()
        {
            ServerRealization = new AlfaProServerRealization();
            NeedToHideParameters = false;
            CreateParameterString("PRO WebSocket URL", "ws://127.0.0.1:3366/router/");
            CreateParameterPassword("PRO Bearer Token", "");
            CreateParameterBoolean("Allow LIVE trading", false);
        }
    }

    public sealed class AlfaProServerRealization : IServerRealization
    {
        private AlfaProSession _session;
        private CancellationTokenSource _lifetime;
        private readonly ConcurrentDictionary<long, Security> _securities = new ConcurrentDictionary<long, Security>();
        private readonly ConcurrentDictionary<string, Portfolio> _portfolios = new ConcurrentDictionary<string, Portfolio>();
        private readonly ConcurrentDictionary<long, string> _sections = new ConcurrentDictionary<long, string>();
        private readonly ConcurrentDictionary<long, long> _sectionGroups = new ConcurrentDictionary<long, long>();
        private readonly ConcurrentDictionary<long, string> _sectionRcodes = new ConcurrentDictionary<long, string>();
        private readonly ConcurrentDictionary<long, (string Portfolio, string Security, string Class)> _positionIdentifiers =
            new ConcurrentDictionary<long, (string Portfolio, string Security, string Class)>();
        private readonly ConcurrentDictionary<int, Order> _orders = new ConcurrentDictionary<int, Order>();
        private readonly ConcurrentDictionary<string, long> _objectByCode = new ConcurrentDictionary<string, long>();
        private readonly ConcurrentDictionary<string, long> _allowed = new ConcurrentDictionary<string, long>();
        private readonly object _positionLock = new object();

        public ServerType ServerType => ServerType.AlfaPro;
        public ServerConnectStatus ServerStatus { get; set; } = ServerConnectStatus.Disconnect;
        public DateTime ServerTime { get; set; } = DateTime.UtcNow;
        public List<IServerParameter> ServerParameters { get; set; }
        public bool IsCompletelyDeleted { get; set; }
        public event Action ConnectEvent;
        public event Action DisconnectEvent;
        public event Action ForceCheckOrdersAfterReconnectEvent;
        public event Action<List<Security>> SecurityEvent;
        public event Action<List<Portfolio>> PortfolioEvent;
        public event Action<MarketDepth> MarketDepthEvent;
        public event Action<Trade> NewTradesEvent;
        public event Action<Order> MyOrderEvent;
        public event Action<MyTrade> MyTradeEvent;
        public event Action<string, LogMessageType> LogMessageEvent;
        public event Action<News> NewsEvent { add { } remove { } }
        public event Action<Funding> FundingUpdateEvent { add { } remove { } }
        public event Action<SecurityVolumes> Volume24hUpdateEvent { add { } remove { } }
        public event Action<OptionMarketDataForConnector> AdditionalMarketDataEvent { add { } remove { } }

        public void Connect(WebProxy proxy)
        {
            if (_session != null) return;
            var endpoint = Parameter<ServerParameterString>("PRO WebSocket URL")?.Value
                ?? "ws://127.0.0.1:3366/router/";
            var token = Parameter<ServerParameterPassword>("PRO Bearer Token")?.Value;
            if (string.IsNullOrWhiteSpace(token))
            {
                Log("Bearer token is missing", LogMessageType.Error);
                return;
            }
            _lifetime = new CancellationTokenSource();
            var ct = _lifetime.Token;
            _ = Task.Run(async () =>
            {
                try
                {
                    var current = new AlfaProSession();
                    _session = current;
                    current.Error += message => Log(message, LogMessageType.Error);
                    current.Disconnected += () =>
                    {
                        ServerStatus = ServerConnectStatus.Disconnect;
                        DisconnectEvent?.Invoke();
                    };
                    current.EntityUpdated += OnEntity;
                    current.EntityDeleted += OnEntityDeleted;
                    current.DepthUpdated += OnDepth;
                    current.PublicTrade += OnPublicTrade;
                    current.Orders.OrderChanged += OnOrderChanged;
                    current.Orders.OwnExecution += OnOwnTrade;
                    current.Orders.TradingEnabled = Parameter<ServerParameterBool>("Allow LIVE trading")?.Value == true;
                    await current.ConnectAsync(new Uri(endpoint), token, ct).ConfigureAwait(false);
                    if (ct.IsCancellationRequested) return;
                    ServerStatus = ServerConnectStatus.Connect;
                    ConnectEvent?.Invoke();
                    ForceCheckOrdersAfterReconnectEvent?.Invoke();
                    PublishSecurities();
                    PublishPortfolios();
                }
                catch (Exception ex)
                {
                    Log("Alfa PRO connection failed: " + ex.Message, LogMessageType.Error);
                    ServerStatus = ServerConnectStatus.Disconnect;
                }
            }, ct);
        }

        public void Dispose()
        {
            _lifetime?.Cancel();
            var previous = _session;
            _session = null;
            if (previous != null)
            {
                try { previous.DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds(5)); }
                catch (Exception) { }
            }
            _lifetime?.Dispose();
            _lifetime = null;
            if (ServerStatus != ServerConnectStatus.Disconnect)
            {
                ServerStatus = ServerConnectStatus.Disconnect;
                DisconnectEvent?.Invoke();
            }
        }

        private T Parameter<T>(string name) where T : class, IServerParameter
            => ServerParameters?.OfType<T>().FirstOrDefault(x => x.Name == name);

        private void Log(string message, LogMessageType type)
            => LogMessageEvent?.Invoke(message, type);

        public void GetSecurities() => PublishSecurities();
        private void PublishSecurities()
        {
            if (_securities.Count > 0) SecurityEvent?.Invoke(_securities.Values.ToList());
        }

        private void OnEntity(string type, JsonElement item)
        {
            try
            {
                switch (type)
                {
                    case "AssetInfoEntity":
                        foreach (var instrument in AlfaProMarketData.ParseAsset(item))
                        {
                            var sec = _securities.GetOrAdd(instrument.FiId, _ => new Security
                            {
                                Name = instrument.Ticker,
                                NameFull = instrument.Name,
                                NameId = instrument.FiId.ToString(),
                                NameClass = instrument.RCode + "#" + instrument.MarketBoardId,
                                Exchange = instrument.RCode,
                                SecurityType = SecurityType.Stock,
                                // Unknown values remain zero until FinInfoParamsEntity arrives.
                                Lot = 0,
                                PriceStep = 0,
                                VolumeStep = 1
                            });
                            _objectByCode[sec.NameId] = instrument.ObjectId;
                        }
                        break;
                    case "FinInfoParamsEntity":
                    {
                        long fi = item.Field("IdFi").Int64();
                        if (_securities.TryGetValue(fi, out var sec))
                        {
                            decimal lot = item.Field("Lot").Decimal();
                            decimal step = item.Field("PriceStep").Decimal();
                            if (lot > 0 && step > 0)
                            {
                                sec.Lot = lot;
                                sec.PriceStep = step;
                                sec.PriceStepCost = item.Field("PriceStepCost").Decimal();
                                sec.PriceLimitHigh = item.Field("UpPrice").Decimal();
                                sec.PriceLimitLow = item.Field("DownPrice").Decimal();
                                PublishSecurities();
                            }
                        }
                        break;
                    }
                    case "AllowedOrderParamEntity":
                    {
                        long group = item.Field("IdObjectGroup").Int64();
                        long board = item.Field("IdMarketBoard").Int64();
                        long orderType = item.Field("IdOrderType").Int64();
                        long lifetime = item.Field("IdLifeTime").Int64();
                        long paramId = item.Field("IdAllowedOrderParams").Int64();
                        if (item.Field("IdDocumentType").Int64() == 1
                            && item.Field("IdQuantityType").Int64() == 1
                            && item.Field("IdPriceType").Int64() == 1 && paramId > 0)
                            _allowed[group + ":" + board + ":" + orderType + ":" + lifetime] = paramId;
                        break;
                    }
                    case "SubAccountRazdelEntity":
                    {
                        string key = PortfolioKey(item.Field("IdAccount").Int64(),
                            item.Field("IdSubAccount").Int64(), item.Field("IdRazdel").Int64());
                        long section = item.Field("IdRazdel").Int64();
                        if (section <= 0) break;
                        _sections[section] = key;
                        _sectionGroups[section] = item.Field("IdRazdelGroup").Int64();
                        _sectionRcodes[section] = item.Field("RCode").Text();
                        _portfolios.GetOrAdd(key, _ => new Portfolio { Number = key, ServerType = ServerType.AlfaPro });
                        PublishPortfolios();
                        break;
                    }
                    case "ClientBalanceEntity":
                        UpdateBalance(item);
                        break;
                    case "ClientPositionEntity":
                        UpdatePosition(item);
                        break;
                    case "OrderEntity":
                        UpdateBrokerOrder(item);
                        break;
                }
            }
            catch (Exception ex) { Log("Entity " + type + ": " + ex.Message, LogMessageType.Error); }
        }

        private void OnEntityDeleted(string type, JsonElement item)
        {
            if (type != "ClientPositionEntity") return;
            long id = item.Field("IdPosition").Int64();
            if (id <= 0 || !_positionIdentifiers.TryRemove(id, out var mapping) ||
                !_portfolios.TryGetValue(mapping.Portfolio, out var portfolio))
            {
                _session?.Orders.MarkUnreconciled();
                Log("Cannot correlate removed broker position; state must be re-audited",
                    LogMessageType.Error);
                return;
            }
            lock (_positionLock)
            {
                portfolio.SetNewPosition(new PositionOnBoard
                {
                    PortfolioName = mapping.Portfolio,
                    SecurityNameCode = mapping.Security,
                    SecurityNameClass = mapping.Class,
                    ValueCurrent = 0,
                    ValueBlocked = 0
                });
            }
            PublishPortfolios();
        }

        private static string PortfolioKey(long account, long sub, long section)
            => account + "/" + sub + "/" + section;

        public void GetPortfolios() => PublishPortfolios();
        private void PublishPortfolios()
        {
            if (_portfolios.Count > 0) PortfolioEvent?.Invoke(_portfolios.Values.ToList());
        }

        private void UpdateBalance(JsonElement item)
        {
            long account = item.Field("IdAccount").Int64();
            long sub = item.Field("IdSubAccount").Int64();
            long group = item.Field("IdRazdelGroup").Int64();
            if (account <= 0 || sub <= 0 || group <= 0) return;
            // BalanceEntity is scoped to (subaccount, market group), not every section.
            foreach (var pair in _sections)
            {
                long section = pair.Key;
                if (!_sectionGroups.TryGetValue(section, out var currentGroup) ||
                    currentGroup != group ||
                    pair.Value != PortfolioKey(account, sub, section) ||
                    !_portfolios.TryGetValue(pair.Value, out var portfolio)) continue;
                portfolio.ValueBegin = item.Field("PrevBalance").Decimal();
                portfolio.ValueCurrent = item.Field("PortfolioCost").Decimal();
                portfolio.ValueLiquidCash = item.Field("LiquidBalance").Decimal();
                portfolio.UnrealizedPnl = item.Field("NPL").Decimal();
            }
            PublishPortfolios();
        }

        private void UpdatePosition(JsonElement item)
        {
            long objectId = item.Field("IdObject").Int64();
            long section = item.Field("IdRazdel").Int64();
            long account = item.Field("IdAccount").Int64();
            long sub = item.Field("IdSubAccount").Int64();
            long positionId = item.Field("IdPosition").Int64();
            if (positionId <= 0 || objectId <= 0 || account <= 0 || sub <= 0 ||
                !_sections.TryGetValue(section, out var key) ||
                key != PortfolioKey(account, sub, section) ||
                !_portfolios.TryGetValue(key, out var portfolio)) return;

            _sectionRcodes.TryGetValue(section, out var rcode);
            var candidates = _securities.Values.Where(sec =>
                _objectByCode.TryGetValue(sec.NameId, out var id) &&
                id == objectId && sec.Lot > 0 &&
                (string.IsNullOrEmpty(rcode) || sec.Exchange == rcode)).ToList();
            // A position cannot be associated with an arbitrary board when several match.
            if (candidates.Count != 1)
            {
                _session?.Orders.MarkUnreconciled();
                Log("Position maps to zero or multiple instruments: IdObject=" + objectId,
                    LogMessageType.Error);
                return;
            }
            var security = candidates[0];
            lock (_positionLock)
            {
                portfolio.SetNewPosition(new PositionOnBoard
                {
                    PortfolioName = key,
                    SecurityNameCode = security.Name,
                    SecurityNameClass = security.NameClass,
                    ValueCurrent = item.Field("TorgPos").Decimal() / security.Lot,
                    ValueBegin = item.Field("BackPos").Decimal() / security.Lot,
                    ValueBlocked = 0,
                    UnrealizedPnl = item.Field("DailyPL").Decimal()
                });
            }
            _positionIdentifiers[positionId] = (key, security.Name, security.NameClass);
            PublishPortfolios();
        }

        public void Subscribe(Security security)
        {
            if (!long.TryParse(security?.NameId, out var fi)) return;
            var current = _session;
            if (current == null) return;
            _ = Task.Run(async () =>
            {
                try { await current.SubscribeMarketAsync(fi).ConfigureAwait(false); }
                catch (Exception ex) { Log("Market subscription failed: " + ex.Message, LogMessageType.Error); }
            });
        }

        public void Unsubscribe(Security security)
        {
            // Underlying market stream may still serve other robots; keep until disconnect.
        }

        private void OnDepth(AlfaDepth data)
        {
            if (!_securities.TryGetValue(data.FiId, out var sec)) return;
            var book = new MarketDepth { SecurityNameCode = sec.Name, Time = data.ReceivedUtc.UtcDateTime };
            foreach (var row in data.Levels)
            {
                if (row.BidsInLots > 0)
                    book.Bids.Add(new MarketDepthLevel { Price = (double)row.Price, Bid = (double)row.BidsInLots });
                if (row.AsksInLots > 0)
                    book.Asks.Add(new MarketDepthLevel { Price = (double)row.Price, Ask = (double)row.AsksInLots });
            }
            book.Bids.Sort((a, b) => b.Price.CompareTo(a.Price));
            book.Asks.Sort((a, b) => a.Price.CompareTo(b.Price));
            MarketDepthEvent?.Invoke(book);
        }

        private void OnPublicTrade(AlfaPublicTrade data)
        {
            if (!_securities.TryGetValue(data.FiId, out var sec) || sec.Lot <= 0) return;
            NewTradesEvent?.Invoke(new Trade
            {
                SecurityNameCode = sec.Name, Id = data.TradeNumber, Price = data.Price,
                Volume = data.QuantityUnits / sec.Lot, Time = data.Time.UtcDateTime,
                Side = data.Side == 1 ? Side.Buy : data.Side == -1 ? Side.Sell : Side.None
            });
        }

        private static OrderStateType ConvertState(AlfaSubmissionState state) => state switch
        {
            AlfaSubmissionState.Active => OrderStateType.Active,
            AlfaSubmissionState.PartiallyFilled => OrderStateType.Partial,
            AlfaSubmissionState.Filled => OrderStateType.Done,
            AlfaSubmissionState.Cancelled => OrderStateType.Cancel,
            AlfaSubmissionState.Rejected => OrderStateType.Fail,
            _ => OrderStateType.Pending
        };

        private void OnOrderChanged(AlfaOrderRecord record)
        {
            if (!_orders.TryGetValue(record.Request.LocalOrderId, out var order)) return;
            if (record.BrokerId > 0) order.NumberMarket = record.BrokerId.ToString();
            order.VolumeExecute = AlfaProQuantities.UnitsToLots(
                record.Request.QuantityUnits - record.RemainingUnits,
                _securities.Values.FirstOrDefault(s => s.Name == order.SecurityNameCode
                    && s.NameClass == order.SecurityClassCode)?.Lot ?? 1);
            order.State = ConvertState(record.State);
            order.TimeCallBack = DateTime.UtcNow;
            MyOrderEvent?.Invoke(order);
        }

        private void UpdateBrokerOrder(JsonElement item)
        {
            var record = _session?.Orders.Find(item.Field("NumEDocument").Int64(),
                item.Field("ClientOrderNum").Int64());
            if (record != null) OnOrderChanged(record);
        }

        private void OnOwnTrade(JsonElement item)
        {
            var brokerId = item.Field("NumEDocument").Int64();
            var record = _session?.Orders.Find(brokerId, 0);
            if (record == null || !_orders.TryGetValue(record.Request.LocalOrderId, out var order)) return;
            var lot = _securities.Values.FirstOrDefault(s => s.Name == order.SecurityNameCode
                && s.NameClass == order.SecurityClassCode)?.Lot ?? 0;
            if (lot <= 0) return;
            MyTradeEvent?.Invoke(new MyTrade
            {
                NumberTrade = item.Field("IdOperation").Text(),
                NumberOrderParent = order.NumberMarket,
                SecurityNameCode = order.SecurityNameCode,
                Side = item.Field("BuySell").Int64() == 1 ? Side.Buy : Side.Sell,
                Price = item.Field("Price").Decimal(),
                Volume = item.Field("Quantity").Decimal() / lot,
                Time = item.Field("TimeOperation").Date()?.UtcDateTime ?? DateTime.UtcNow
            });
        }

        public void SendOrder(Order order)
        {
            if (order == null) return;
            _orders.TryAdd(order.NumberUser, order);
            _ = Task.Run(async () =>
            {
                try
                {
                    var current = _session ?? throw new InvalidOperationException("Not connected");
                    var instrument = current.Instruments.FirstOrDefault(x =>
                        _securities.TryGetValue(x.FiId, out var sec) && sec.Name == order.SecurityNameCode
                        && sec.NameClass == order.SecurityClassCode);
                    if (instrument == null) throw new InvalidOperationException("Security not found");
                    var parameters = current.Parameters(instrument.FiId);
                    if (parameters == null || parameters.Value.Lot <= 0 || parameters.Value.Step <= 0)
                        throw new InvalidOperationException("Instrument lot/step not verified");
                    if (order.TypeOrder != OrderPriceType.Limit && order.TypeOrder != OrderPriceType.Market)
                        throw new NotSupportedException("Only market and limit orders supported");
                    if (order.TypeOrder == OrderPriceType.Limit
                        && (order.Price <= 0 || order.Price % parameters.Value.Step != 0))
                        throw new InvalidOperationException("Limit price does not match exchange tick size");
                    var segments = (order.PortfolioNumber ?? "").Split('/');
                    if (segments.Length != 3) throw new InvalidOperationException("Select a valid Alfa PRO portfolio");
                    long account = long.Parse(segments[0]), sub = long.Parse(segments[1]),
                        section = long.Parse(segments[2]);
                    string key = instrument.ObjectGroupId + ":" + instrument.MarketBoardId + ":"
                        + (order.TypeOrder == OrderPriceType.Market ? 1 : 2) + ":5";
                    if (!_allowed.TryGetValue(key, out long allowed) || allowed <= 0)
                        throw new InvalidOperationException("AllowedOrderParamEntity combination not available");
                    order.State = OrderStateType.Pending;
                    MyOrderEvent?.Invoke(order);
                    var req = new AlfaOrderRequest(order.NumberUser, account, sub, section,
                        instrument.ObjectId, instrument.MarketBoardId, checked((int)allowed),
                        order.Side == Side.Buy ? 1 : -1,
                        AlfaProQuantities.LotsToUnits(order.Volume, parameters.Value.Lot),
                        order.Price, order.TypeOrder == OrderPriceType.Market,
                        "OSEngine-" + order.NumberUser);
                    await current.Orders.SubmitAsync(req).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    var record = _session?.Orders.AllOrders.FirstOrDefault(x => x.Request.LocalOrderId == order.NumberUser);
                    if (record?.State == AlfaSubmissionState.Rejected)
                    {
                        order.State = OrderStateType.Fail;
                        MyOrderEvent?.Invoke(order);
                    }
                    // Other outcomes are UNKNOWN. Never report them as definitely rejected.
                    Log("Order not confirmed (do not resubmit blindly): " + ex.Message, LogMessageType.Error);
                }
            });
        }

        public bool CancelOrder(Order order)
        {
            if (order == null || _session == null) return false;
            var current = _session;
            _ = Task.Run(async () =>
            {
                try { await current.Orders.CancelAsync(order.NumberUser).ConfigureAwait(false); }
                catch (Exception ex) { Log("Cancel outcome needs reconciliation: " + ex.Message, LogMessageType.Error); }
            });
            return true; // command queued, NOT proof of broker cancellation
        }

        public void ChangeOrderPrice(Order order, decimal newPrice)
            => Log("Alfa PRO does not offer atomic order replacement. Cancel and submit a new order explicitly.",
                LogMessageType.Error);

        public void CancelAllOrders()
        {
            foreach (var order in _orders.Values.Where(x => x.State == OrderStateType.Active
                || x.State == OrderStateType.Partial)) CancelOrder(order);
        }

        public void CancelAllOrdersToSecurity(Security security)
        {
            foreach (var order in _orders.Values.Where(x => x.SecurityNameCode == security.Name
                && (x.State == OrderStateType.Active || x.State == OrderStateType.Partial))) CancelOrder(order);
        }

        public void GetAllActivOrders()
        {
            foreach (var order in GetActiveOrders(0, 100)) MyOrderEvent?.Invoke(order);
        }

        public OrderStateType GetOrderStatus(Order order)
            => _orders.TryGetValue(order.NumberUser, out var known) ? known.State : OrderStateType.None;

        public List<Order> GetActiveOrders(int startIndex, int count) => _orders.Values
            .Where(x => x.State == OrderStateType.Active || x.State == OrderStateType.Partial
                || x.State == OrderStateType.Pending).OrderByDescending(x => x.TimeCreate)
            .Skip(Math.Max(0, startIndex)).Take(Math.Max(0, count)).ToList();

        public List<Order> GetHistoricalOrders(int startIndex, int count) => _orders.Values
            .Where(x => x.State == OrderStateType.Done || x.State == OrderStateType.Cancel
                || x.State == OrderStateType.Fail).OrderByDescending(x => x.TimeCreate)
            .Skip(Math.Max(0, startIndex)).Take(Math.Max(0, count)).ToList();

        public List<Candle> GetLastCandleHistory(Security security, TimeFrameBuilder tf, int count) => null;
        public List<Candle> GetCandleDataToSecurity(Security security, TimeFrameBuilder tf,
            DateTime start, DateTime end, DateTime actual) => null;
        public List<Trade> GetTickDataToSecurity(Security security,
            DateTime start, DateTime end, DateTime actual) => null;
        public void SetLeverage(Security security, decimal leverage)
            => Log("Changing leverage is not supported by Alfa PRO connector", LogMessageType.System);
        public bool SubscribeNews() => false;
    }
}
