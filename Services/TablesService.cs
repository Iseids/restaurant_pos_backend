using Microsoft.EntityFrameworkCore;
using ResPosBackend.Data;

namespace ResPosBackend.Services;

public sealed class TablesService(PosDbContext db)
{
    public async Task<List<object>> ListActiveWithOpenOrder(CancellationToken ct)
    {
        var activeTables = await db.Tables
            .AsNoTracking()
            .Where(x => x.IsActive)
            .OrderBy(x => x.Name)
            .Select(x => new { x.Id, x.Name })
            .ToListAsync(ct);

        if (activeTables.Count == 0)
        {
            return [];
        }

        var tableIds = activeTables.Select(x => x.Id).ToList();
        var openOrders = await db.Orders
            .AsNoTracking()
            .Where(x => x.Status == "open" && x.TableId.HasValue && tableIds.Contains(x.TableId.Value))
            .OrderByDescending(x => x.CreatedAt)
            .Select(x => new
            {
                TableId = x.TableId!.Value,
                OpenOrderId = (Guid?)x.Id,
                OpenOrderNo = (short?)x.OrderNo,
                OpenPeopleCount = x.PeopleCount,
                OpenCreatedAt = (DateTime?)x.CreatedAt,
            })
            .ToListAsync(ct);

        var latestOpenByTable = openOrders
            .GroupBy(x => x.TableId)
            .ToDictionary(g => g.Key, g => g.First());

        return activeTables.Select(t =>
        {
            latestOpenByTable.TryGetValue(t.Id, out var open);
            return (object)new
            {
                id = t.Id,
                name = t.Name,
                openOrderId = open?.OpenOrderId,
                openOrderNo = open?.OpenOrderNo.HasValue == true ? open.OpenOrderNo.Value.ToString("00") : null,
                openPeopleCount = open?.OpenPeopleCount,
                openCreatedAt = open?.OpenCreatedAt,
            };
        }).ToList();
    }
}
