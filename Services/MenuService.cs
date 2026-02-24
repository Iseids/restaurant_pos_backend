using Microsoft.EntityFrameworkCore;
using ResPosBackend.Data;

namespace ResPosBackend.Services;

public sealed class MenuService(PosDbContext db)
{
    public async Task<List<object>> ListActiveCategories(CancellationToken ct)
    {
        return await db.Categories
            .AsNoTracking()
            .Where(x => x.IsActive)
            .OrderBy(x => x.SortOrder)
            .ThenBy(x => x.Name)
            .Select(x => (object)new
            {
                id = x.Id,
                name = x.Name,
                sortOrder = x.SortOrder,
                printerId = x.PrinterId,
                parentId = x.ParentId,
                imageUrl = x.ImageUrl,
            })
            .ToListAsync(ct);
    }

    public async Task<List<object>> ListActiveItems(Guid categoryId, CancellationToken ct)
    {
        return await db.MenuItems
            .AsNoTracking()
            .Where(x => x.IsActive && x.CategoryId == categoryId)
            .OrderBy(x => x.Name)
            .Select(x => (object)new
            {
                id = x.Id,
                categoryId = x.CategoryId,
                name = x.Name,
                price = (double)x.Price,
                imageUrl = x.ImageUrl,
            })
            .ToListAsync(ct);
    }

    public async Task<List<object>> ListActiveCustomizations(Guid menuItemId, CancellationToken ct)
    {
        var groups = await db.MenuItemOptionGroups
            .AsNoTracking()
            .Where(x => x.MenuItemId == menuItemId && x.IsActive)
            .OrderBy(x => x.SortOrder)
            .ThenBy(x => x.Name)
            .Select(x => new
            {
                x.Id,
                x.Name,
                x.IsRequired,
                x.MinSelect,
                x.MaxSelect,
                x.AllowQuantity,
                x.SortOrder,
            })
            .ToListAsync(ct);

        if (groups.Count == 0)
        {
            return [];
        }

        var groupIds = groups.Select(x => x.Id).ToList();
        var options = await db.MenuItemOptions
            .AsNoTracking()
            .Where(x => x.IsActive && groupIds.Contains(x.GroupId))
            .OrderBy(x => x.SortOrder)
            .ThenBy(x => x.Name)
            .Select(x => new
            {
                x.Id,
                x.GroupId,
                x.Name,
                x.PriceDelta,
                x.MaxQty,
                x.SortOrder,
            })
            .ToListAsync(ct);

        var byGroup = options
            .GroupBy(x => x.GroupId)
            .ToDictionary(
                g => g.Key,
                g => g.Select(x => (object)new
                {
                    id = x.Id,
                    groupId = x.GroupId,
                    name = x.Name,
                    priceDelta = (double)x.PriceDelta,
                    maxQty = x.MaxQty,
                    sortOrder = x.SortOrder,
                }).ToList());

        return groups.Select(g => (object)new
        {
            id = g.Id,
            name = g.Name,
            isRequired = g.IsRequired,
            minSelect = g.MinSelect,
            maxSelect = g.MaxSelect,
            allowQuantity = g.AllowQuantity,
            sortOrder = g.SortOrder,
            options = byGroup.TryGetValue(g.Id, out var list) ? list : new List<object>(),
        }).ToList();
    }
}
