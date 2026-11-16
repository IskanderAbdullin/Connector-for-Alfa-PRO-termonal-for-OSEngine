using System.Globalization;
using System.Text.Json;

namespace AlfaPro.Core;

/// <summary>Defensive helpers for the PRO API's inconsistent numeric JSON representations.</summary>
public static class AlfaProJson
{
    public static JsonElement Field(this JsonElement node, string property)
    {
        if (node.ValueKind == JsonValueKind.Object && node.TryGetProperty(property, out var value))
            return value;
        return default;
    }

    public static string Text(this JsonElement node, string fallback = "")
        => node.ValueKind switch
        {
            JsonValueKind.String => node.GetString() ?? fallback,
            JsonValueKind.Number => node.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => fallback
        };

    public static long Int64(this JsonElement node, long fallback = 0)
        => long.TryParse(node.Text(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : fallback;

    public static decimal Decimal(this JsonElement node, decimal fallback = 0)
        => decimal.TryParse(node.Text(), NumberStyles.Float, CultureInfo.InvariantCulture, out var n) ? n : fallback;

    public static bool Bool(this JsonElement node, bool fallback = false)
        => node.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String when bool.TryParse(node.GetString(), out var n) => n,
            _ => fallback
        };

    public static DateTimeOffset? Date(this JsonElement node)
        => DateTimeOffset.TryParse(node.Text(), CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal, out var date) ? date : null;

    public static JsonElement Unwrap(JsonElement element)
    {
        var nested = element.Field("data");
        return nested.ValueKind == JsonValueKind.Object ? nested : element;
    }

    public static IReadOnlyList<JsonElement> Items(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Array) return Array.Empty<JsonElement>();
        var result = new List<JsonElement>(element.GetArrayLength());
        foreach (var item in element.EnumerateArray()) result.Add(item.Clone());
        return result;
    }
}
