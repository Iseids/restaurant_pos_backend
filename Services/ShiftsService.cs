using Microsoft.EntityFrameworkCore;
using ResPosBackend.Data;
using ResPosBackend.Models;

namespace ResPosBackend.Services;

public sealed class ShiftsService(PosDbContext db)
{
    public async Task<object?> GetCurrentShiftSummary(CancellationToken ct)
    {
        var shift = await db.Shifts
            .AsNoTracking()
            .OrderByDescending(x => x.OpenedAt)
            .FirstOrDefaultAsync(x => x.ClosedAt == null, ct);

        if (shift is null)
        {
            return null;
        }

        var totals = await PaymentTotals(shift.OpenedAt, DateTime.UtcNow, ct);
        var expectedCash = (double)shift.OpeningCash + totals.Cash;

        return new
        {
            shift = new
            {
                id = shift.Id,
                openedBy = shift.OpenedBy,
                openedAt = shift.OpenedAt,
                openingCash = (double)shift.OpeningCash,
                note = shift.Note,
                closedAt = shift.ClosedAt,
                closingCash = shift.ClosingCash is null ? null : (double?)shift.ClosingCash.Value,
            },
            totals = new { cash = totals.Cash, card = totals.Card, cheque = totals.Cheque },
            expectedCash,
            difference = (double?)null,
        };
    }

    public async Task<object> OpenShift(Guid userId, double openingCash, string? note, CancellationToken ct)
    {
        var hasOpenShift = await db.Shifts.AnyAsync(x => x.ClosedAt == null, ct);
        if (hasOpenShift)
        {
            throw new InvalidOperationException("SHIFT_ALREADY_OPEN");
        }

        var shift = new PosShift
        {
            Id = Guid.NewGuid(),
            OpenedBy = userId,
            OpenedAt = DateTime.UtcNow,
            OpeningCash = (decimal)openingCash,
            Note = string.IsNullOrWhiteSpace(note) ? null : note.Trim(),
        };

        db.Shifts.Add(shift);
        await db.SaveChangesAsync(ct);

        return new
        {
            shift = new
            {
                id = shift.Id,
                openedBy = shift.OpenedBy,
                openedAt = shift.OpenedAt,
                openingCash = (double)shift.OpeningCash,
                note = shift.Note,
                closedAt = shift.ClosedAt,
                closingCash = shift.ClosingCash,
            },
            totals = new { cash = 0.0, card = 0.0, cheque = 0.0 },
            expectedCash = openingCash,
            difference = (double?)null,
        };
    }

    public async Task<object?> CloseShift(Guid userId, Guid shiftId, double closingCash, string? note, CancellationToken ct)
    {
        var shift = await db.Shifts.FirstOrDefaultAsync(x => x.Id == shiftId && x.ClosedAt == null, ct);
        if (shift is null)
        {
            return null;
        }

        var closedAt = DateTime.UtcNow;
        shift.ClosedBy = userId;
        shift.ClosedAt = closedAt;
        shift.ClosingCash = (decimal)closingCash;
        if (!string.IsNullOrWhiteSpace(note))
        {
            shift.Note = note.Trim();
        }

        await db.SaveChangesAsync(ct);

        var totals = await PaymentTotals(shift.OpenedAt, closedAt, ct);
        var expectedCash = (double)shift.OpeningCash + totals.Cash;
        var difference = closingCash - expectedCash;

        return new
        {
            shift = new
            {
                id = shift.Id,
                openedAt = shift.OpenedAt,
                openingCash = (double)shift.OpeningCash,
                closedAt = shift.ClosedAt,
                closingCash = shift.ClosingCash is null ? null : (double?)shift.ClosingCash.Value,
            },
            totals = new { cash = totals.Cash, card = totals.Card, cheque = totals.Cheque },
            expectedCash,
            difference,
        };
    }

    public async Task<List<object>> ListShifts(int limit, CancellationToken ct)
    {
        var items = await db.Shifts
            .AsNoTracking()
            .OrderByDescending(x => x.OpenedAt)
            .Take(Math.Clamp(limit, 1, 200))
            .ToListAsync(ct);

        return items.Select(x => (object)new
        {
            id = x.Id,
            openedAt = x.OpenedAt,
            openingCash = (double)x.OpeningCash,
            closedAt = x.ClosedAt,
            closingCash = x.ClosingCash is null ? null : (double?)x.ClosingCash.Value,
            note = x.Note,
        }).ToList();
    }

    private async Task<(double Cash, double Card, double Cheque)> PaymentTotals(DateTime from, DateTime to, CancellationToken ct)
    {
        var grouped = await db.Payments
            .AsNoTracking()
            .Where(x => x.CreatedAt >= from && x.CreatedAt <= to)
            .GroupBy(x => x.Method.ToLower())
            .Select(g => new { Method = g.Key, Total = g.Sum(x => x.Amount) })
            .ToListAsync(ct);

        double cash = 0;
        double card = 0;
        double cheque = 0;

        foreach (var row in grouped)
        {
            var v = (double)row.Total;
            switch (row.Method)
            {
                case "cash":
                    cash = v;
                    break;
                case "card":
                    card = v;
                    break;
                case "cheque":
                    cheque = v;
                    break;
            }
        }

        return (cash, card, cheque);
    }
}
