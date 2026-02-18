using Microsoft.EntityFrameworkCore;
using PosBackend.AspNet.Data;

namespace PosBackend.AspNet.Services;

public sealed class ReportsService(PosDbContext db)
{
    public async Task<byte[]> BuildShiftSummaryPdfBytes(Guid shiftId, string restaurantName, CancellationToken ct)
    {
        var shift = await db.Shifts.AsNoTracking().FirstOrDefaultAsync(x => x.Id == shiftId, ct);
        if (shift is null)
        {
            throw new InvalidOperationException("SHIFT_NOT_FOUND");
        }

        var openedBy = await db.Users.AsNoTracking()
            .Where(x => x.Id == shift.OpenedBy)
            .Select(x => x.Username)
            .FirstOrDefaultAsync(ct);

        var paymentRows = await (
            from p in db.Payments.AsNoTracking()
            join o in db.Orders.AsNoTracking() on p.OrderId equals o.Id
            where o.ShiftId == shiftId
            group p by p.Method into g
            orderby g.Key
            select new { Method = g.Key, Amount = g.Sum(x => x.Amount) })
            .ToListAsync(ct);

        var paidOrders = await db.Orders.AsNoTracking().CountAsync(x => x.ShiftId == shiftId && x.Status == "paid", ct);
        var openOrders = await db.Orders.AsNoTracking().CountAsync(x => x.ShiftId == shiftId && x.Status == "open", ct);

        var voidedItems = await (
            from i in db.OrderItems.AsNoTracking()
            join o in db.Orders.AsNoTracking() on i.OrderId equals o.Id
            where o.ShiftId == shiftId && i.Voided
            select i.Id)
            .CountAsync(ct);

        var paymentsText = paymentRows.Count == 0
            ? "none"
            : string.Join(", ", paymentRows.Select(x => $"{x.Method}:{(double)x.Amount:0.##}"));

        var lines = new[]
        {
            "%PDF-1.1",
            "1 0 obj<< /Type /Catalog /Pages 2 0 R>>endobj",
            "2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1>>endobj",
            "3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 600 300] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>endobj",
            "4 0 obj<< /Length 260 >>stream",
            "BT /F1 11 Tf 24 260 Td",
            $"({EscapePdf(restaurantName)} - Shift Summary) Tj",
            "0 -18 Td",
            $"(Shift: {shiftId}) Tj",
            "0 -16 Td",
            $"(Opened By: {EscapePdf(openedBy ?? "unknown")}) Tj",
            "0 -16 Td",
            $"(Opened At: {shift.OpenedAt:yyyy-MM-dd HH:mm:ss}) Tj",
            "0 -16 Td",
            $"(Closing At: {shift.ClosedAt:yyyy-MM-dd HH:mm:ss}) Tj",
            "0 -16 Td",
            $"(Opening Cash: {(double)shift.OpeningCash:0.##}) Tj",
            "0 -16 Td",
            $"(Closing Cash: {(shift.ClosingCash is null ? "-" : ((double)shift.ClosingCash.Value).ToString("0.##"))}) Tj",
            "0 -16 Td",
            $"(Paid Orders: {paidOrders}  Open Orders: {openOrders}  Voided Items: {voidedItems}) Tj",
            "0 -16 Td",
            $"(Payments: {EscapePdf(paymentsText)}) Tj",
            "ET",
            "endstream endobj",
            "5 0 obj<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>endobj",
            "xref",
            "0 6",
            "0000000000 65535 f ",
            "0000000010 00000 n ",
            "0000000060 00000 n ",
            "0000000117 00000 n ",
            "0000000244 00000 n ",
            "0000000350 00000 n ",
            "trailer<< /Size 6 /Root 1 0 R >>",
            "startxref",
            "420",
            "%%EOF",
        };

        return System.Text.Encoding.ASCII.GetBytes(string.Join("\n", lines));
    }

    private static string EscapePdf(string s)
    {
        return s.Replace("\\", "\\\\").Replace("(", "\\(").Replace(")", "\\)");
    }
}
