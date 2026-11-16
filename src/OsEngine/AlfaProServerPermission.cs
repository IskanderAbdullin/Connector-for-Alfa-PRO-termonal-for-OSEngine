namespace OsEngine.Market.Servers.AlfaPro
{
    /// <summary>Do not advertise functionality that has not been implemented and verified.</summary>
    public sealed class AlfaProServerPermission : IServerPermission
    {
        public ServerType ServerType => ServerType.AlfaPro;
        public bool DataFeedTf1SecondCanLoad => false;
        public bool DataFeedTf2SecondCanLoad => false;
        public bool DataFeedTf5SecondCanLoad => false;
        public bool DataFeedTf10SecondCanLoad => false;
        public bool DataFeedTf15SecondCanLoad => false;
        public bool DataFeedTf20SecondCanLoad => false;
        public bool DataFeedTf30SecondCanLoad => false;
        public bool DataFeedTf1MinuteCanLoad => false;
        public bool DataFeedTf2MinuteCanLoad => false;
        public bool DataFeedTf5MinuteCanLoad => false;
        public bool DataFeedTf10MinuteCanLoad => false;
        public bool DataFeedTf15MinuteCanLoad => false;
        public bool DataFeedTf30MinuteCanLoad => false;
        public bool DataFeedTf1HourCanLoad => false;
        public bool DataFeedTf2HourCanLoad => false;
        public bool DataFeedTf4HourCanLoad => false;
        public bool DataFeedTfDayCanLoad => false;
        public bool DataFeedTfTickCanLoad => false;
        public bool DataFeedTfMarketDepthCanLoad => true;
        public bool MarketOrdersIsSupport => true;
        public bool StopOrdersIsSupport => false;
        public bool IsCanChangeOrderPrice => false;
        public bool IsUseLotToCalculateProfit => true;
        public TimeFramePermission TradeTimeFramePermission => new TimeFramePermission
        {
            TimeFrameSec1IsOn = false,
            TimeFrameSec2IsOn = false,
            TimeFrameSec5IsOn = false,
            TimeFrameSec10IsOn = false,
            TimeFrameSec15IsOn = false,
            TimeFrameSec20IsOn = false,
            TimeFrameSec30IsOn = false,
            TimeFrameMin1IsOn = false,
            TimeFrameMin2IsOn = false,
            TimeFrameMin3IsOn = false,
            TimeFrameMin5IsOn = false,
            TimeFrameMin10IsOn = false,
            TimeFrameMin15IsOn = false,
            TimeFrameMin20IsOn = false,
            TimeFrameMin30IsOn = false,
            TimeFrameMin45IsOn = false,
            TimeFrameHour1IsOn = false,
            TimeFrameHour2IsOn = false,
            TimeFrameHour4IsOn = false,
            TimeFrameDayIsOn = false
        };
        public int WaitTimeSecondsAfterFirstStartToSendOrders => 0;
        public bool UseStandardCandlesStarter => false;
        public bool ManuallyClosePositionOnBoard_IsOn => false;
        public string[] ManuallyClosePositionOnBoard_ValuesForTrimmingName => null;
        public string[] ManuallyClosePositionOnBoard_ExceptionPositionNames => null;
        public bool CanQueryOrdersAfterReconnect => true;
        public bool CanQueryOrderStatus => true;
        public bool CanGetOrderLists => false;
        public bool HaveOnlyMakerLimitsRealization => false;
        public bool IsNewsServer => false;
        public bool IsSupports_CheckDataFeedLogic => false;
        public string[] CheckDataFeedLogic_ExceptionSecuritiesClass => null;
        public int CheckDataFeedLogic_NoDataMinutesToDisconnect => 10;
        public bool IsSupports_MultipleInstances => false;
        public bool IsSupports_ProxyFor_MultipleInstances => false;
        public bool IsSupports_AsyncOrderSending => false;
        public int AsyncOrderSending_RateGateLimitMls => 100;
        public bool IsSupports_AsyncCandlesStarter => false;
        public int AsyncCandlesStarter_RateGateLimitMls => 100;
        public string[] IpAddressServer => null;
        public bool Leverage_IsSupports => false;
        public decimal Leverage_StandardValue => 1;
        public string[] Leverage_SupportClasses => null;
        public bool CanChangeOrderMarketNumber => true;
        public OrderLifeTimePermission OrdersLifeTimeRealization => new OrderLifeTimePermission
        {
            DayIsReady = true,
            GtcIsReady = false,
            SpecifiedIsReady = false
        };
    }
}
