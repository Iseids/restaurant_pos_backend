using BCrypt.Net;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using PosBackend.AspNet.Data;
using PosBackend.AspNet.Infrastructure;
using PosBackend.AspNet.Models;

namespace PosBackend.AspNet.Services;

public sealed class AdminService(PosDbContext db)
{
    public async Task<object> GetPrinterSettings(CancellationToken ct)
    {
        var rows = await db.AppSettings
            .AsNoTracking()
            .Where(x =>
                x.Key == PosSettingKeys.DefaultReceiptPrinterId ||
                x.Key == PosSettingKeys.DefaultInvoicePrinterId)
            .ToListAsync(ct);

        var receipt = rows.FirstOrDefault(x => x.Key == PosSettingKeys.DefaultReceiptPrinterId)?.Value;
        var invoice = rows.FirstOrDefault(x => x.Key == PosSettingKeys.DefaultInvoicePrinterId)?.Value;

        return new
        {
            receiptPrinterId = Guid.TryParse(receipt, out var receiptId) ? receiptId : (Guid?)null,
            invoicePrinterId = Guid.TryParse(invoice, out var invoiceId) ? invoiceId : (Guid?)null,
        };
    }

    public async Task<object> SetPrinterSettings(Guid? receiptPrinterId, Guid? invoicePrinterId, CancellationToken ct)
    {
        if (receiptPrinterId.HasValue)
        {
            var exists = await db.Printers.AsNoTracking()
                .AnyAsync(x =>
                    x.Id == receiptPrinterId.Value &&
                    x.IsActive &&
                    x.Type != null &&
                    x.Type.ToLower() == "network" &&
                    x.Address != null &&
                    x.Address != "",
                    ct);
            if (!exists)
            {
                throw new InvalidOperationException("RECEIPT_PRINTER_NOT_FOUND");
            }
        }

        if (invoicePrinterId.HasValue)
        {
            var exists = await db.Printers.AsNoTracking()
                .AnyAsync(x =>
                    x.Id == invoicePrinterId.Value &&
                    x.IsActive &&
                    x.Type != null &&
                    x.Type.ToLower() == "network" &&
                    x.Address != null &&
                    x.Address != "",
                    ct);
            if (!exists)
            {
                throw new InvalidOperationException("INVOICE_PRINTER_NOT_FOUND");
            }
        }

        await UpsertSetting(PosSettingKeys.DefaultReceiptPrinterId, receiptPrinterId?.ToString(), ct);
        await UpsertSetting(PosSettingKeys.DefaultInvoicePrinterId, invoicePrinterId?.ToString(), ct);
        await db.SaveChangesAsync(ct);

        return new
        {
            receiptPrinterId,
            invoicePrinterId,
        };
    }

    public async Task<List<object>> ListPrinters(CancellationToken ct)
    {
        var items = await db.Printers.AsNoTracking().OrderByDescending(x => x.CreatedAt).ToListAsync(ct);
        return items.Select(x => (object)new
        {
            id = x.Id,
            name = x.Name,
            type = x.Type,
            address = x.Address,
            isActive = x.IsActive,
            createdAt = x.CreatedAt,
        }).ToList();
    }

    public async Task<object> CreatePrinter(string name, string type, string? address, CancellationToken ct)
    {
        var entity = new PosPrinter
        {
            Id = Guid.NewGuid(),
            Name = name,
            Type = string.IsNullOrWhiteSpace(type) ? "network" : type,
            Address = string.IsNullOrWhiteSpace(address) ? null : address.Trim(),
            IsActive = true,
            CreatedAt = DateTime.UtcNow,
        };

        db.Printers.Add(entity);
        await SaveWithConflictHandling(ct);

        return new
        {
            id = entity.Id,
            name = entity.Name,
            type = entity.Type,
            address = entity.Address,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
        };
    }

    public async Task<object?> UpdatePrinter(Guid id, string? name, string? type, string? address, bool hasAddress, bool? isActive, CancellationToken ct)
    {
        var entity = await db.Printers.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(name))
        {
            entity.Name = name.Trim();
        }

        if (!string.IsNullOrWhiteSpace(type))
        {
            entity.Type = type.Trim();
        }

        if (hasAddress)
        {
            entity.Address = string.IsNullOrWhiteSpace(address) ? null : address.Trim();
        }

        if (isActive.HasValue)
        {
            entity.IsActive = isActive.Value;
        }

        await SaveWithConflictHandling(ct);

        return new
        {
            id = entity.Id,
            name = entity.Name,
            type = entity.Type,
            address = entity.Address,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
        };
    }

    public async Task<List<object>> ListPrintQueue(CancellationToken ct)
    {
        var rows = await (
            from q in db.PrintQueue.AsNoTracking()
            join o in db.Orders.AsNoTracking() on q.OrderId equals o.Id
            join p in db.Printers.AsNoTracking() on q.PrinterId equals p.Id
            join t in db.Tables.AsNoTracking() on o.TableId equals t.Id into tables
            from t in tables.DefaultIfEmpty()
            orderby q.UpdatedAt descending
            select new
            {
                q.Id,
                q.OrderId,
                q.PrinterId,
                q.Kind,
                q.Status,
                q.Attempts,
                q.LastError,
                q.CreatedAt,
                q.UpdatedAt,
                o.OrderNo,
                o.BusinessDate,
                o.IsTakeaway,
                TableName = t != null ? t.Name : null,
                PrinterName = p.Name,
                PrinterType = p.Type,
            }).ToListAsync(ct);

        return rows.Select(x => (object)new
        {
            id = x.Id,
            orderId = x.OrderId,
            printerId = x.PrinterId,
            kind = x.Kind,
            status = x.Status,
            attempts = x.Attempts,
            lastError = x.LastError,
            createdAt = x.CreatedAt,
            updatedAt = x.UpdatedAt,
            orderNo = x.OrderNo.ToString("00"),
            businessDate = x.BusinessDate.ToString("yyyy-MM-dd"),
            isTakeaway = x.IsTakeaway,
            tableName = x.TableName,
            printerName = x.PrinterName,
            printerType = string.IsNullOrWhiteSpace(x.PrinterType) ? "network" : x.PrinterType,
        }).ToList();
    }

    public async Task DeletePrintQueueItem(Guid id, CancellationToken ct)
    {
        var entity = await db.PrintQueue.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            throw new InvalidOperationException("QUEUE_ITEM_NOT_FOUND");
        }

        db.PrintQueue.Remove(entity);
        await db.SaveChangesAsync(ct);
    }

    public async Task<List<object>> ListCategories(CancellationToken ct)
    {
        var items = await db.Categories.AsNoTracking().OrderBy(x => x.SortOrder).ThenBy(x => x.Name).ToListAsync(ct);
        return items.Select(x => (object)new
        {
            id = x.Id,
            name = x.Name,
            sortOrder = x.SortOrder,
            printerId = x.PrinterId,
            parentId = x.ParentId,
            isActive = x.IsActive,
            createdAt = x.CreatedAt,
            imageUrl = x.ImageUrl,
        }).ToList();
    }

    public async Task<object> CreateCategory(string name, int sortOrder, Guid? printerId, Guid? parentId, string? imageUrl, CancellationToken ct)
    {
        var entity = new PosCategory
        {
            Id = Guid.NewGuid(),
            Name = name,
            SortOrder = sortOrder,
            PrinterId = printerId,
            ParentId = parentId,
            IsActive = true,
            CreatedAt = DateTime.UtcNow,
            ImageUrl = string.IsNullOrWhiteSpace(imageUrl) ? null : imageUrl.Trim(),
        };

        db.Categories.Add(entity);
        await SaveWithConflictHandling(ct);

        return new
        {
            id = entity.Id,
            name = entity.Name,
            sortOrder = entity.SortOrder,
            printerId = entity.PrinterId,
            parentId = entity.ParentId,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
            imageUrl = entity.ImageUrl,
        };
    }

    public async Task<object?> UpdateCategory(
        Guid id,
        string? name,
        int? sortOrder,
        Guid? printerId,
        bool hasPrinter,
        Guid? parentId,
        bool hasParent,
        string? imageUrl,
        bool hasImage,
        bool? isActive,
        CancellationToken ct)
    {
        var entity = await db.Categories.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(name))
        {
            entity.Name = name.Trim();
        }

        if (sortOrder.HasValue)
        {
            entity.SortOrder = sortOrder.Value;
        }

        if (hasPrinter)
        {
            entity.PrinterId = printerId;
        }

        if (hasParent)
        {
            entity.ParentId = parentId;
        }

        if (hasImage)
        {
            entity.ImageUrl = string.IsNullOrWhiteSpace(imageUrl) ? null : imageUrl.Trim();
        }

        if (isActive.HasValue)
        {
            entity.IsActive = isActive.Value;
        }

        await SaveWithConflictHandling(ct);

        return new
        {
            id = entity.Id,
            name = entity.Name,
            sortOrder = entity.SortOrder,
            printerId = entity.PrinterId,
            parentId = entity.ParentId,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
            imageUrl = entity.ImageUrl,
        };
    }

    public async Task<List<object>> ListMaterials(CancellationToken ct)
    {
        var items = await db.RawMaterials.AsNoTracking().OrderBy(x => x.Name).ToListAsync(ct);
        return items.Select(x => (object)new
        {
            id = x.Id,
            name = x.Name,
            unit = x.Unit,
            stockQty = (double)x.StockQty,
            isActive = x.IsActive,
            createdAt = x.CreatedAt,
        }).ToList();
    }

    public async Task<object> CreateMaterial(string name, string? unit, double stockQty, CancellationToken ct)
    {
        var entity = new PosRawMaterial
        {
            Id = Guid.NewGuid(),
            Name = name,
            Unit = string.IsNullOrWhiteSpace(unit) ? null : unit.Trim(),
            StockQty = (decimal)stockQty,
            IsActive = true,
            CreatedAt = DateTime.UtcNow,
        };

        db.RawMaterials.Add(entity);
        await SaveWithConflictHandling(ct);

        return new
        {
            id = entity.Id,
            name = entity.Name,
            unit = entity.Unit,
            stockQty = (double)entity.StockQty,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
        };
    }

    public async Task<object?> UpdateMaterial(Guid id, string? name, string? unit, double? stockDelta, bool? isActive, CancellationToken ct)
    {
        var entity = await db.RawMaterials.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(name))
        {
            entity.Name = name.Trim();
        }

        if (!string.IsNullOrWhiteSpace(unit))
        {
            entity.Unit = unit.Trim();
        }

        if (stockDelta.HasValue)
        {
            entity.StockQty += (decimal)stockDelta.Value;
        }

        if (isActive.HasValue)
        {
            entity.IsActive = isActive.Value;
        }

        await SaveWithConflictHandling(ct);

        return new
        {
            id = entity.Id,
            name = entity.Name,
            unit = entity.Unit,
            stockQty = (double)entity.StockQty,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
        };
    }

    public async Task<List<object>> ListMenuItems(Guid? categoryId, CancellationToken ct)
    {
        var query = db.MenuItems.AsNoTracking();
        if (categoryId.HasValue)
        {
            query = query.Where(x => x.CategoryId == categoryId.Value);
        }

        var items = await query.OrderBy(x => x.Name).ToListAsync(ct);
        return items.Select(x => (object)new
        {
            id = x.Id,
            categoryId = x.CategoryId,
            name = x.Name,
            price = (double)x.Price,
            stockQty = (double)x.StockQty,
            isActive = x.IsActive,
            createdAt = x.CreatedAt,
            imageUrl = x.ImageUrl,
        }).ToList();
    }

    public async Task<object> CreateMenuItem(Guid categoryId, string name, double price, double stockQty, string? imageUrl, CancellationToken ct)
    {
        var entity = new PosMenuItem
        {
            Id = Guid.NewGuid(),
            CategoryId = categoryId,
            Name = name,
            Price = (decimal)price,
            StockQty = (decimal)stockQty,
            IsActive = true,
            CreatedAt = DateTime.UtcNow,
            ImageUrl = string.IsNullOrWhiteSpace(imageUrl) ? null : imageUrl.Trim(),
        };

        db.MenuItems.Add(entity);
        await SaveWithConflictHandling(ct);

        return new
        {
            id = entity.Id,
            categoryId = entity.CategoryId,
            name = entity.Name,
            price = (double)entity.Price,
            stockQty = (double)entity.StockQty,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
            imageUrl = entity.ImageUrl,
        };
    }

    public async Task<object?> UpdateMenuItem(
        Guid id,
        string? name,
        double? price,
        bool? isActive,
        Guid? categoryId,
        double? stockDelta,
        string? imageUrl,
        bool hasImage,
        CancellationToken ct)
    {
        var entity = await db.MenuItems.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(name))
        {
            entity.Name = name.Trim();
        }

        if (price.HasValue)
        {
            entity.Price = (decimal)price.Value;
        }

        if (stockDelta.HasValue)
        {
            entity.StockQty += (decimal)stockDelta.Value;
        }

        if (isActive.HasValue)
        {
            entity.IsActive = isActive.Value;
        }

        if (categoryId.HasValue)
        {
            entity.CategoryId = categoryId.Value;
        }

        if (hasImage)
        {
            entity.ImageUrl = string.IsNullOrWhiteSpace(imageUrl) ? null : imageUrl.Trim();
        }

        await SaveWithConflictHandling(ct);

        return new
        {
            id = entity.Id,
            categoryId = entity.CategoryId,
            name = entity.Name,
            price = (double)entity.Price,
            stockQty = (double)entity.StockQty,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
            imageUrl = entity.ImageUrl,
        };
    }

    public async Task DeleteMenuItem(Guid id, CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var affectedItems = await db.OrderItems.Where(x => x.MenuItemId == id).ToListAsync(ct);
        foreach (var item in affectedItems)
        {
            item.MenuItemId = null;
        }
        if (affectedItems.Count > 0)
        {
            await db.SaveChangesAsync(ct);
        }

        var entity = await db.MenuItems.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            await tx.RollbackAsync(ct);
            throw new InvalidOperationException("MENU_ITEM_NOT_FOUND");
        }

        db.MenuItems.Remove(entity);
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
    }

    public async Task<List<object>> ListMenuItemMaterials(Guid menuItemId, CancellationToken ct)
    {
        var rows = await (
            from m in db.MenuItemMaterials.AsNoTracking()
            join r in db.RawMaterials.AsNoTracking() on m.MaterialId equals r.Id
            where m.MenuItemId == menuItemId
            orderby r.Name
            select new
            {
                m.MaterialId,
                r.Name,
                r.Unit,
                m.Qty,
            })
            .ToListAsync(ct);

        return rows.Select(x => (object)new
        {
            materialId = x.MaterialId,
            name = x.Name,
            unit = x.Unit,
            qty = (double)x.Qty,
        }).ToList();
    }

    public async Task SetMenuItemMaterials(Guid menuItemId, IReadOnlyList<(Guid MaterialId, double Qty)> items, CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var existing = await db.MenuItemMaterials.Where(x => x.MenuItemId == menuItemId).ToListAsync(ct);
        db.MenuItemMaterials.RemoveRange(existing);

        foreach (var row in items)
        {
            if (row.Qty == 0)
            {
                continue;
            }

            db.MenuItemMaterials.Add(new PosMenuItemMaterial
            {
                MenuItemId = menuItemId,
                MaterialId = row.MaterialId,
                Qty = (decimal)row.Qty,
            });
        }

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
    }

    public async Task<List<object>> ListMenuItemCustomizations(Guid menuItemId, bool includeInactive, CancellationToken ct)
    {
        var groupsQ = db.MenuItemOptionGroups.AsNoTracking().Where(x => x.MenuItemId == menuItemId);
        if (!includeInactive)
        {
            groupsQ = groupsQ.Where(x => x.IsActive);
        }

        var groups = await groupsQ.OrderBy(x => x.SortOrder).ThenBy(x => x.Name).ToListAsync(ct);
        if (groups.Count == 0)
        {
            return [];
        }

        var groupIds = groups.Select(x => x.Id).ToList();
        var optionsQ = db.MenuItemOptions.AsNoTracking().Where(x => groupIds.Contains(x.GroupId));
        if (!includeInactive)
        {
            optionsQ = optionsQ.Where(x => x.IsActive);
        }

        var options = await optionsQ.OrderBy(x => x.SortOrder).ThenBy(x => x.Name).ToListAsync(ct);

        var byGroup = options.GroupBy(x => x.GroupId).ToDictionary(
            g => g.Key,
            g => g.Select(x => (object)new
            {
                id = x.Id,
                groupId = x.GroupId,
                name = x.Name,
                priceDelta = (double)x.PriceDelta,
                maxQty = x.MaxQty,
                sortOrder = x.SortOrder,
                isActive = x.IsActive,
            }).ToList());

        return groups.Select(g => (object)new
        {
            id = g.Id,
            menuItemId = g.MenuItemId,
            name = g.Name,
            isRequired = g.IsRequired,
            minSelect = g.MinSelect,
            maxSelect = g.MaxSelect,
            allowQuantity = g.AllowQuantity,
            sortOrder = g.SortOrder,
            isActive = g.IsActive,
            options = byGroup.TryGetValue(g.Id, out var list) ? list : new List<object>(),
        }).ToList();
    }

    public async Task<object> CreateCustomizationGroup(
        Guid menuItemId,
        string name,
        bool isRequired,
        int minSelect,
        int? maxSelect,
        bool allowQuantity,
        int sortOrder,
        bool isActive,
        CancellationToken ct)
    {
        var entity = new PosMenuItemOptionGroup
        {
            Id = Guid.NewGuid(),
            MenuItemId = menuItemId,
            Name = name,
            IsRequired = isRequired,
            MinSelect = minSelect,
            MaxSelect = maxSelect,
            AllowQuantity = allowQuantity,
            SortOrder = sortOrder,
            IsActive = isActive,
        };

        db.MenuItemOptionGroups.Add(entity);
        await db.SaveChangesAsync(ct);

        return new
        {
            id = entity.Id,
            menuItemId = entity.MenuItemId,
            name = entity.Name,
            isRequired = entity.IsRequired,
            minSelect = entity.MinSelect,
            maxSelect = entity.MaxSelect,
            allowQuantity = entity.AllowQuantity,
            sortOrder = entity.SortOrder,
            isActive = entity.IsActive,
            options = Array.Empty<object>(),
        };
    }

    public async Task<object?> UpdateCustomizationGroup(
        Guid id,
        string? name,
        bool? isRequired,
        int? minSelect,
        int? maxSelect,
        bool hasMaxSelect,
        bool? allowQuantity,
        int? sortOrder,
        bool? isActive,
        CancellationToken ct)
    {
        var entity = await db.MenuItemOptionGroups.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(name))
        {
            entity.Name = name.Trim();
        }

        if (isRequired.HasValue)
        {
            entity.IsRequired = isRequired.Value;
        }

        if (minSelect.HasValue)
        {
            entity.MinSelect = minSelect.Value;
        }

        if (hasMaxSelect)
        {
            entity.MaxSelect = maxSelect;
        }

        if (allowQuantity.HasValue)
        {
            entity.AllowQuantity = allowQuantity.Value;
        }

        if (sortOrder.HasValue)
        {
            entity.SortOrder = sortOrder.Value;
        }

        if (isActive.HasValue)
        {
            entity.IsActive = isActive.Value;
        }

        await db.SaveChangesAsync(ct);

        return new
        {
            id = entity.Id,
            menuItemId = entity.MenuItemId,
            name = entity.Name,
            isRequired = entity.IsRequired,
            minSelect = entity.MinSelect,
            maxSelect = entity.MaxSelect,
            allowQuantity = entity.AllowQuantity,
            sortOrder = entity.SortOrder,
            isActive = entity.IsActive,
            options = Array.Empty<object>(),
        };
    }

    public async Task<object> CreateCustomizationOption(
        Guid groupId,
        string name,
        double priceDelta,
        int? maxQty,
        int sortOrder,
        bool isActive,
        CancellationToken ct)
    {
        var entity = new PosMenuItemOption
        {
            Id = Guid.NewGuid(),
            GroupId = groupId,
            Name = name,
            PriceDelta = (decimal)priceDelta,
            MaxQty = maxQty,
            SortOrder = sortOrder,
            IsActive = isActive,
        };

        db.MenuItemOptions.Add(entity);
        await db.SaveChangesAsync(ct);

        return new
        {
            id = entity.Id,
            groupId = entity.GroupId,
            name = entity.Name,
            priceDelta = (double)entity.PriceDelta,
            maxQty = entity.MaxQty,
            sortOrder = entity.SortOrder,
            isActive = entity.IsActive,
        };
    }

    public async Task<object?> UpdateCustomizationOption(
        Guid id,
        string? name,
        double? priceDelta,
        int? maxQty,
        bool hasMaxQty,
        int? sortOrder,
        bool? isActive,
        CancellationToken ct)
    {
        var entity = await db.MenuItemOptions.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(name))
        {
            entity.Name = name.Trim();
        }

        if (priceDelta.HasValue)
        {
            entity.PriceDelta = (decimal)priceDelta.Value;
        }

        if (hasMaxQty)
        {
            entity.MaxQty = maxQty;
        }

        if (sortOrder.HasValue)
        {
            entity.SortOrder = sortOrder.Value;
        }

        if (isActive.HasValue)
        {
            entity.IsActive = isActive.Value;
        }

        await db.SaveChangesAsync(ct);

        return new
        {
            id = entity.Id,
            groupId = entity.GroupId,
            name = entity.Name,
            priceDelta = (double)entity.PriceDelta,
            maxQty = entity.MaxQty,
            sortOrder = entity.SortOrder,
            isActive = entity.IsActive,
        };
    }

    public async Task<List<object>> ListTables(bool includeInactive, CancellationToken ct)
    {
        var query = db.Tables.AsNoTracking();
        if (!includeInactive)
        {
            query = query.Where(x => x.IsActive);
        }

        var items = await query.OrderBy(x => x.Name).ToListAsync(ct);
        return items.Select(x => (object)new
        {
            id = x.Id,
            name = x.Name,
            isActive = x.IsActive,
            createdAt = x.CreatedAt,
        }).ToList();
    }

    public async Task<object> CreateTable(string name, CancellationToken ct)
    {
        var entity = new PosTable
        {
            Id = Guid.NewGuid(),
            Name = name,
            SortOrder = 0,
            IsActive = true,
            CreatedAt = DateTime.UtcNow,
        };

        db.Tables.Add(entity);
        await SaveWithConflictHandling(ct);

        return new
        {
            id = entity.Id,
            name = entity.Name,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
        };
    }

    public async Task<object?> UpdateTable(Guid id, string? name, bool? isActive, CancellationToken ct)
    {
        var entity = await db.Tables.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(name))
        {
            entity.Name = name.Trim();
        }

        if (isActive.HasValue)
        {
            entity.IsActive = isActive.Value;
        }

        await SaveWithConflictHandling(ct);

        return new
        {
            id = entity.Id,
            name = entity.Name,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
        };
    }

    public async Task<List<object>> ListUsers(CancellationToken ct)
    {
        var items = await db.Users.AsNoTracking().OrderBy(x => x.CreatedAt).ToListAsync(ct);
        return items.Select(x => (object)new
        {
            id = x.Id,
            username = x.Username,
            role = x.Role,
            isActive = x.IsActive,
            createdAt = x.CreatedAt,
        }).ToList();
    }

    public async Task<object> CreateUser(string username, string password, string role, CancellationToken ct)
    {
        var entity = new PosUser
        {
            Id = Guid.NewGuid(),
            Username = username,
            PasswordHash = BCrypt.Net.BCrypt.HashPassword(password),
            Role = string.IsNullOrWhiteSpace(role) ? "service" : role,
            IsActive = true,
            CreatedAt = DateTime.UtcNow,
        };

        db.Users.Add(entity);
        await SaveWithConflictHandling(ct);

        return new
        {
            id = entity.Id,
            username = entity.Username,
            role = entity.Role,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
        };
    }

    public async Task<object?> UpdateUser(Guid id, string? role, bool? isActive, string? password, CancellationToken ct)
    {
        var entity = await db.Users.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(role))
        {
            entity.Role = role.Trim();
        }

        if (isActive.HasValue)
        {
            entity.IsActive = isActive.Value;
        }

        if (!string.IsNullOrWhiteSpace(password))
        {
            entity.PasswordHash = BCrypt.Net.BCrypt.HashPassword(password);
        }

        await SaveWithConflictHandling(ct);

        return new
        {
            id = entity.Id,
            username = entity.Username,
            role = entity.Role,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
        };
    }

    private async Task SaveWithConflictHandling(CancellationToken ct)
    {
        try
        {
            await db.SaveChangesAsync(ct);
        }
        catch (DbUpdateException ex) when (ex.InnerException is PostgresException pg && pg.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            throw new InvalidOperationException("CONFLICT");
        }
    }

    private async Task UpsertSetting(string key, string? value, CancellationToken ct)
    {
        var row = await db.AppSettings.FirstOrDefaultAsync(x => x.Key == key, ct);
        if (row is null)
        {
            db.AppSettings.Add(new PosAppSetting
            {
                Key = key,
                Value = value,
                UpdatedAt = DateTime.UtcNow,
            });
            return;
        }

        row.Value = value;
        row.UpdatedAt = DateTime.UtcNow;
    }
}
