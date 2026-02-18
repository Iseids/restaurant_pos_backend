using System.Globalization;
using System.Text;

namespace PosBackend.AspNet.Infrastructure;

public static class SimplePdfBuilder
{
    public static byte[] BuildSinglePageText(IReadOnlyList<string> lines, string title)
    {
        const double pageWidth = 595;
        const double pageHeight = 842;
        const double startX = 40;
        const double startY = 800;
        const double lineStep = 15;

        var content = new StringBuilder();
        content.AppendLine("BT");
        content.AppendLine("/F1 11 Tf");
        content.AppendLine($"{Fmt(startX)} {Fmt(startY)} Td");

        if (!string.IsNullOrWhiteSpace(title))
        {
            content.AppendLine($"({Escape(title)}) Tj");
            content.AppendLine($"0 -{Fmt(lineStep + 4)} Td");
        }

        foreach (var line in lines)
        {
            content.AppendLine($"({Escape(line)}) Tj");
            content.AppendLine($"0 -{Fmt(lineStep)} Td");
        }

        content.AppendLine("ET");

        var streamBytes = Encoding.ASCII.GetBytes(content.ToString());
        var objects = new List<string>
        {
            "<< /Type /Catalog /Pages 2 0 R >>",
            "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            $"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {Fmt(pageWidth)} {Fmt(pageHeight)}] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
            $"<< /Length {streamBytes.Length} >>\nstream\n{Encoding.ASCII.GetString(streamBytes)}endstream",
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        };

        return Build(objects);
    }

    private static byte[] Build(IReadOnlyList<string> objects)
    {
        var sb = new StringBuilder();
        sb.Append("%PDF-1.4\n");

        var offsets = new List<int> { 0 };
        for (var i = 0; i < objects.Count; i++)
        {
            offsets.Add(Encoding.ASCII.GetByteCount(sb.ToString()));
            sb.Append(i + 1).Append(" 0 obj\n");
            sb.Append(objects[i]).Append("\n");
            sb.Append("endobj\n");
        }

        var xrefPos = Encoding.ASCII.GetByteCount(sb.ToString());
        sb.Append("xref\n");
        sb.Append("0 ").Append(objects.Count + 1).Append("\n");
        sb.Append("0000000000 65535 f \n");
        foreach (var off in offsets.Skip(1))
        {
            sb.Append(off.ToString("D10", CultureInfo.InvariantCulture)).Append(" 00000 n \n");
        }

        sb.Append("trailer\n");
        sb.Append("<< /Size ").Append(objects.Count + 1).Append(" /Root 1 0 R >>\n");
        sb.Append("startxref\n");
        sb.Append(xrefPos).Append("\n");
        sb.Append("%%EOF\n");

        return Encoding.ASCII.GetBytes(sb.ToString());
    }

    private static string Escape(string text)
    {
        return (text ?? string.Empty)
            .Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("(", "\\(", StringComparison.Ordinal)
            .Replace(")", "\\)", StringComparison.Ordinal);
    }

    private static string Fmt(double number)
    {
        return number.ToString("0.###", CultureInfo.InvariantCulture);
    }
}
