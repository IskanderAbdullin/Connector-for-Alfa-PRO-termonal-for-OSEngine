using AlfaPro.Core;
using System.Text.Json;

static void Check(bool condition, string message)
{
    if (!condition) throw new Exception("FAILED: " + message);
    Console.WriteLine("PASS: " + message);
}

using var json = JsonDocument.Parse("{\"Id\":\"123\",\"Price\":10.25,\"Active\":true}");
Check(json.RootElement.Field("Id").Int64() == 123, "numeric strings");
Check(json.RootElement.Field("Price").Decimal() == 10.25m, "decimal invariant");
Check(json.RootElement.Field("Active").Bool(), "boolean");

using var depthJson = JsonDocument.Parse("""
    {"IdFi":144950,"Lines":[
      {"Price":100.01,"BuyQty":7,"SellQty":0},
      {"Price":100.02,"BuyQty":0,"SellQty":11}
    ]}
    """);
var depth = AlfaProMarketData.ParseDepth(depthJson.RootElement);
Check(depth is { FiId: 144950 } && depth.Levels.Count == 2, "order book parsing");
Check(depth!.Levels[0].BidsInLots == 7 && depth.Levels[1].AsksInLots == 11, "book quantities are lots");

using var invalidDepthJson = JsonDocument.Parse(
    "{\"IdFi\":1,\"Lines\":[{\"Price\":1,\"BuyQty\":-1,\"SellQty\":0}]}");
Check(AlfaProMarketData.ParseDepth(invalidDepthJson.RootElement) is null, "invalid depth rejected");
Check(AlfaProQuantities.LotsToUnits(3, 10) == 30, "lots -> units");
Check(AlfaProQuantities.UnitsToLots(30, 10) == 3, "units -> lots");
try
{
    AlfaProQuantities.LotsToUnits(0.5m, 1);
    throw new Exception("fractional units were accepted");
}
catch (ArgumentException) { Console.WriteLine("PASS: fractional quantity rejected"); }

using var assetJson = JsonDocument.Parse("""
    {"IdObject":12,"IdObjectGroup":2,"Ticker":"SBER","Name":"Sber",
      "Instruments":[{"IdFi":99,"IdMarketBoard":3,"RCode":"TQBR","IsLiquid":true}]}
    """);
Check(AlfaProMarketData.ParseAsset(assetJson.RootElement).Single() is
    { FiId:99, ObjectId:12, MarketBoardId:3 }, "IdObject and IdFi mapping");
Console.WriteLine("All core unit checks passed.");
