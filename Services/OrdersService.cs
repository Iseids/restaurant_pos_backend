using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using ResPosBackend.Data;
using ResPosBackend.Models;

namespace ResPosBackend.Services;

public sealed class OrdersService(
    PosDbContext db,
    AuditService audit,
    ILogger<OrdersService> logger,
    SystemAccountsService systemAccounts)
{
    public async Task<List<object>> ListOpenOrders(CancellationToken ct)
    {
        var rows = await (
            from o in db.Orders.AsNoTracking()
            join t in db.Tables.AsNoTracking() on o.TableId equals t.Id into tables
            from t in tables.DefaultIfEmpty()
            where o.Status == "open"
            orderby o.CreatedAt descending
            select new
            {
                o.Id,
                o.BusinessDate,
                o.OrderNo,
                o.Status,
                o.Nickname,
                TableName = t != null ? t.Name : null,
                o.PeopleCount,
                o.IsTakeaway,
                o.CreatedAt,
            })
            .Take(200)
            .ToListAsync(ct);

        return rows.Select(r => (object)new
        {
            id = r.Id,
            businessDate = r.BusinessDate.ToString("yyyy-MM-dd"),
            orderNo = r.OrderNo.ToString("00"),
            status = r.Status,
            nickname = r.Nickname,
            tableName = r.TableName,
            peopleCount = r.PeopleCount,
            isTakeaway = r.IsTakeaway,
            createdAt = r.CreatedAt,
        }).ToList();
    }

    public async Task<object?> ListCurrentShiftOrdersHistory(CancellationToken ct)
    {
        var shift = await db.Shifts
            .AsNoTracking()
            .OrderByDescending(x => x.OpenedAt)
            .FirstOrDefaultAsync(x => x.ClosedAt == null, ct);

        if (shift is null)
        {
            return null;
        }

        var orders = await (
            from o in db.Orders.AsNoTracking()
            join t in db.Tables.AsNoTracking() on o.TableId equals t.Id into tables
            from t in tables.DefaultIfEmpty()
            where o.ShiftId == shift.Id
            orderby o.CreatedAt descending
            select new
            {
                o.Id,
                o.OrderNo,
                o.Status,
                o.Nickname,
                o.IsTakeaway,
                TableName = t != null ? t.Name : null,
                o.CreatedAt,
                o.CustomerDiscountPercent,
                o.DiscountAmount,
                o.DiscountPercent,
                o.ServiceFee,
                o.ServiceFeePercent,
            })
            .Take(500)
            .ToListAsync(ct);

        if (orders.Count == 0)
        {
            return new
            {
                shiftId = shift.Id,
                openedAt = shift.OpenedAt,
                items = Array.Empty<object>(),
            };
        }

        var orderIds = orders.Select(x => x.Id).ToList();

        var itemRows = await db.OrderItems
            .AsNoTracking()
            .Where(x => orderIds.Contains(x.OrderId))
            .Select(x => new
            {
                x.OrderId,
                x.Qty,
                x.UnitPrice,
                x.DiscountAmount,
                x.DiscountPercent,
                x.Voided,
            })
            .ToListAsync(ct);

        var paymentRows = await db.Payments
            .AsNoTracking()
            .Where(x => orderIds.Contains(x.OrderId))
            .Select(x => new { x.OrderId, x.Method, x.Amount })
            .ToListAsync(ct);

        var itemsByOrder = itemRows
            .GroupBy(x => x.OrderId)
            .ToDictionary(g => g.Key, g => g.ToList());

        var paymentsByOrder = paymentRows
            .GroupBy(x => x.OrderId)
            .ToDictionary(g => g.Key, g => g.ToList());

        var output = orders.Select(o =>
        {
            var orderPayload = new Dictionary<string, object?>
            {
                ["customerDiscountPercent"] = (double)o.CustomerDiscountPercent,
                ["orderDiscountAmount"] = (double)o.DiscountAmount,
                ["orderDiscountPercent"] = (double)o.DiscountPercent,
                ["serviceFeeAmount"] = (double)o.ServiceFee,
                ["serviceFeePercent"] = (double)o.ServiceFeePercent,
            };

            var orderItems = itemsByOrder.TryGetValue(o.Id, out var rows)
                ? rows.Select(x => (IDictionary<string, object?>)new Dictionary<string, object?>
                {
                    ["qty"] = (double)x.Qty,
                    ["unitPrice"] = (double)x.UnitPrice,
                    ["discountAmount"] = (double)x.DiscountAmount,
                    ["discountPercent"] = (double)x.DiscountPercent,
                    ["voided"] = x.Voided,
                }).ToList()
                : [];

            var orderPayments = paymentsByOrder.TryGetValue(o.Id, out var payments)
                ? payments.Select(x => (object)new Dictionary<string, object?>
                {
                    ["method"] = x.Method,
                    ["amount"] = (double)x.Amount,
                }).ToList()
                : [];

            var totals = ComputeTotals(orderPayload, orderItems, orderPayments);

            var activeItemsCount = rows?.Count(x => !x.Voided) ?? 0;
            return (object)new
            {
                id = o.Id,
                orderNo = o.OrderNo.ToString("00"),
                status = o.Status,
                nickname = o.Nickname,
                tableName = o.TableName,
                isTakeaway = o.IsTakeaway,
                createdAt = o.CreatedAt,
                itemsCount = activeItemsCount,
                total = totals.TryGetValue("total", out var totalObj) ? totalObj : 0d,
                paid = totals.TryGetValue("paid", out var paidObj) ? paidObj : 0d,
                balance = totals.TryGetValue("balance", out var balanceObj) ? balanceObj : 0d,
            };
        }).ToList();

        return new
        {
            shiftId = shift.Id,
            openedAt = shift.OpenedAt,
            items = output,
        };
    }

    public async Task<OrderSummaryResult> CreateOrder(Guid createdBy, Guid? tableId, int? peopleCount, Guid? customerId, bool isTakeaway, CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var openShift = await db.Shifts
            .OrderByDescending(x => x.OpenedAt)
            .FirstOrDefaultAsync(x => x.ClosedAt == null, ct);
        if (openShift is null)
        {
            throw new PosRuleException("SHIFT_REQUIRED");
        }

        if (tableId.HasValue)
        {
            var existing = await FindOpenOrderForTable(tableId.Value, ct);
            if (existing is not null)
            {
                await tx.RollbackAsync(ct);
                return new OrderSummaryResult(
                    existing.Id,
                    existing.BusinessDate.ToString("yyyy-MM-dd"),
                    existing.OrderNo.ToString("00"),
                    existing.Status,
                    true);
            }
        }

        decimal customerDiscount = 0;
        if (customerId.HasValue)
        {
            var c = await db.Customers.AsNoTracking().FirstOrDefaultAsync(x => x.Id == customerId.Value && x.IsActive, ct);
            if (c is not null)
            {
                customerDiscount = c.DiscountPercent;
            }
        }

        var businessDate = DateOnly.FromDateTime(DateTime.UtcNow);
        var orderNo = await NextOrderNo(businessDate, ct);

        var order = new PosOrder
        {
            Id = Guid.NewGuid(),
            BusinessDate = businessDate,
            OrderNo = (short)orderNo,
            Status = "draft",
            TableId = tableId,
            PeopleCount = peopleCount.HasValue ? (short?)peopleCount.Value : null,
            CustomerId = customerId,
            IsTakeaway = isTakeaway,
            CustomerDiscountPercent = customerDiscount,
            DiscountAmount = 0,
            DiscountPercent = 0,
            ServiceFee = 0,
            ServiceFeePercent = 0,
            CreatedBy = createdBy,
            ShiftId = openShift.Id,
            CreatedAt = DateTime.UtcNow,
        };

        db.Orders.Add(order);
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);

        await WriteOrderAuditAsync(
            actorUserId: createdBy,
            method: "POST",
            path: "/api/orders",
            requestBody: new
            {
                orderId = order.Id,
                businessDate = order.BusinessDate.ToString("yyyy-MM-dd"),
                orderNo = order.OrderNo.ToString("00"),
                tableId = order.TableId,
                peopleCount = order.PeopleCount,
                customerId = order.CustomerId,
                isTakeaway = order.IsTakeaway,
            },
            responseBody: new { ok = true, action = "order_created" },
            ct);

        return new OrderSummaryResult(
            order.Id,
            order.BusinessDate.ToString("yyyy-MM-dd"),
            order.OrderNo.ToString("00"),
            order.Status,
            false);
    }

    public async Task<OrderSummaryResult> CreateOrGetOpenOrderForTable(Guid createdBy, Guid tableId, int? peopleCount, CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var openShift = await db.Shifts
            .OrderByDescending(x => x.OpenedAt)
            .FirstOrDefaultAsync(x => x.ClosedAt == null, ct);
        if (openShift is null)
        {
            throw new PosRuleException("SHIFT_REQUIRED");
        }

        var existing = await FindOpenOrderForTable(tableId, ct);
        if (existing is not null)
        {
            if (peopleCount.HasValue)
            {
                existing.PeopleCount = (short)peopleCount.Value;
                await db.SaveChangesAsync(ct);
            }

            await tx.CommitAsync(ct);
            return new OrderSummaryResult(
                existing.Id,
                existing.BusinessDate.ToString("yyyy-MM-dd"),
                existing.OrderNo.ToString("00"),
                existing.Status,
                true);
        }

        var businessDate = DateOnly.FromDateTime(DateTime.UtcNow);
        var orderNo = await NextOrderNo(businessDate, ct);

        var order = new PosOrder
        {
            Id = Guid.NewGuid(),
            BusinessDate = businessDate,
            OrderNo = (short)orderNo,
            Status = "open",
            TableId = tableId,
            PeopleCount = peopleCount.HasValue ? (short?)peopleCount.Value : null,
            CustomerId = null,
            IsTakeaway = false,
            CustomerDiscountPercent = 0,
            DiscountAmount = 0,
            DiscountPercent = 0,
            ServiceFee = 0,
            ServiceFeePercent = 0,
            CreatedBy = createdBy,
            ShiftId = openShift.Id,
            CreatedAt = DateTime.UtcNow,
        };

        db.Orders.Add(order);
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);

        await WriteOrderAuditAsync(
            actorUserId: createdBy,
            method: "POST",
            path: $"/api/tables/{tableId}/open-order",
            requestBody: new
            {
                orderId = order.Id,
                businessDate = order.BusinessDate.ToString("yyyy-MM-dd"),
                orderNo = order.OrderNo.ToString("00"),
                tableId = order.TableId,
                peopleCount = order.PeopleCount,
            },
            responseBody: new { ok = true, action = "order_created" },
            ct);

        return new OrderSummaryResult(
            order.Id,
            order.BusinessDate.ToString("yyyy-MM-dd"),
            order.OrderNo.ToString("00"),
            order.Status,
            false);
    }

    public async Task<object?> GetOrder(Guid orderId, CancellationToken ct)
    {
        var order = await (
            from o in db.Orders.AsNoTracking()
            join t in db.Tables.AsNoTracking() on o.TableId equals t.Id into tables
            from t in tables.DefaultIfEmpty()
            join c in db.Customers.AsNoTracking() on o.CustomerId equals c.Id into customers
            from c in customers.DefaultIfEmpty()
            where o.Id == orderId
            select new
            {
                o.Id,
                o.BusinessDate,
                o.OrderNo,
                o.Status,
                o.Nickname,
                o.TableId,
                TableName = t != null ? t.Name : null,
                o.IsTakeaway,
                o.PeopleCount,
                o.CustomerId,
                CustomerName = c != null ? c.Name : null,
                CustomerPhone = c != null ? c.Phone : null,
                o.CustomerDiscountPercent,
                o.DiscountAmount,
                o.DiscountPercent,
                o.ServiceFee,
                o.ServiceFeePercent,
                o.CreatedAt,
            }).FirstOrDefaultAsync(ct);

        if (order is null)
        {
            return null;
        }

        var itemsRaw = await db.OrderItems
            .AsNoTracking()
            .Where(x => x.OrderId == orderId)
            .OrderBy(x => x.CreatedAt)
            .ToListAsync(ct);

        var voidedByIds = itemsRaw.Where(x => x.VoidedBy.HasValue).Select(x => x.VoidedBy!.Value).Distinct().ToList();
        var voidedByMap = await db.Users
            .AsNoTracking()
            .Where(x => voidedByIds.Contains(x.Id))
            .ToDictionaryAsync(x => x.Id, x => x.Username, ct);

        var customizations = await db.OrderItemCustomizations
            .AsNoTracking()
            .Where(x => itemsRaw.Select(i => i.Id).Contains(x.OrderItemId))
            .OrderBy(x => x.CreatedAt)
            .ToListAsync(ct);

        var customizationsByItem = customizations
            .GroupBy(x => x.OrderItemId)
            .ToDictionary(
                g => g.Key,
                g => g.Select(x => (object)new
                {
                    optionId = x.OptionId,
                    name = x.OptionName,
                    qty = (double)x.Qty,
                }).ToList());

        var items = itemsRaw.Select(x => new Dictionary<string, object?>
        {
            ["id"] = x.Id,
            ["menuItemId"] = x.MenuItemId,
            ["name"] = x.Name,
            ["qty"] = (double)x.Qty,
            ["unitPrice"] = (double)x.UnitPrice,
            ["discountAmount"] = (double)x.DiscountAmount,
            ["discountPercent"] = (double)x.DiscountPercent,
            ["voided"] = x.Voided,
            ["voidReason"] = x.VoidReason,
            ["voidedAt"] = x.VoidedAt,
            ["voidedBy"] = x.VoidedBy.HasValue && voidedByMap.TryGetValue(x.VoidedBy.Value, out var username) ? username : null,
            ["printerId"] = x.PrinterId,
            ["kitchenPrintedAt"] = x.KitchenPrintedAt,
            ["note"] = x.Note,
            ["customizations"] = customizationsByItem.TryGetValue(x.Id, out var list) ? list : new List<object>(),
        }).ToList();

        var paymentsRaw = await db.Payments
            .AsNoTracking()
            .Where(x => x.OrderId == orderId)
            .OrderBy(x => x.CreatedAt)
            .Select(x => new { x.Id, x.Method, x.Amount, x.Reference, x.CreatedAt })
            .ToListAsync(ct);

        var payments = paymentsRaw.Select(x => (object)new Dictionary<string, object?>
        {
            ["id"] = x.Id,
            ["method"] = x.Method,
            ["amount"] = (double)x.Amount,
            ["reference"] = x.Reference,
            ["createdAt"] = x.CreatedAt,
        }).ToList();

        var orderObj = new Dictionary<string, object?>
        {
            ["id"] = order.Id,
            ["businessDate"] = order.BusinessDate.ToString("yyyy-MM-dd"),
            ["orderNo"] = order.OrderNo.ToString("00"),
            ["status"] = order.Status,
            ["nickname"] = order.Nickname,
            ["tableId"] = order.TableId,
            ["tableName"] = order.TableName,
            ["isTakeaway"] = order.IsTakeaway,
            ["peopleCount"] = order.PeopleCount,
            ["customerId"] = order.CustomerId,
            ["customerName"] = order.CustomerName,
            ["customerPhone"] = order.CustomerPhone,
            ["customerDiscountPercent"] = (double)order.CustomerDiscountPercent,
            ["orderDiscountAmount"] = (double)order.DiscountAmount,
            ["orderDiscountPercent"] = (double)order.DiscountPercent,
            ["serviceFeeAmount"] = (double)order.ServiceFee,
            ["serviceFeePercent"] = (double)order.ServiceFeePercent,
            ["createdAt"] = order.CreatedAt,
        };

        var totals = ComputeTotals(
            orderObj,
            items.Cast<IDictionary<string, object?>>().ToList(),
            payments.Cast<object>().ToList());

        return new
        {
            order = orderObj,
            items,
            payments,
            totals,
        };
    }

    public async Task<object> AddItem(Guid orderId, Guid createdBy, Guid menuItemId, double qty, string? note, List<CustomizationSelection>? customizations, CancellationToken ct)
    {
        var menuRow = await (
            from m in db.MenuItems.AsNoTracking()
            join c in db.Categories.AsNoTracking() on m.CategoryId equals c.Id
            where m.Id == menuItemId && m.IsActive
            select new { m.Id, m.Name, m.Price, c.PrinterId })
            .FirstOrDefaultAsync(ct);

        if (menuRow is null)
        {
            throw new PosNotFoundException("MENU_ITEM_NOT_FOUND");
        }

        var cleanNote = string.IsNullOrWhiteSpace(note) ? null : note.Trim();
        var computed = await ComputeCustomizations(menuItemId, customizations ?? [], ct);
        var unitPrice = menuRow.Price + computed.PriceDelta;

        var existing = await db.OrderItems
            .FirstOrDefaultAsync(x => x.OrderId == orderId
                && x.MenuItemId == menuItemId
                && !x.Voided
                && x.KitchenPrintedAt == null
                && x.UnitPrice == unitPrice
                && (x.Note ?? string.Empty) == (cleanNote ?? string.Empty)
                && (x.CustomizationSignature ?? string.Empty) == (computed.Signature ?? string.Empty),
                ct);

        if (existing is not null)
        {
            var previousQty = existing.Qty;
            existing.Qty += (decimal)qty;
            await db.SaveChangesAsync(ct);

            await WriteOrderAuditAsync(
                actorUserId: createdBy,
                method: "POST",
                path: $"/api/orders/{orderId}/items",
                requestBody: new
                {
                    orderId,
                    itemId = existing.Id,
                    menuItemId,
                    name = existing.Name,
                    addedQty = qty,
                    previousQty = (double)previousQty,
                    newQty = (double)existing.Qty,
                    note = cleanNote,
                    customizationsCount = computed.Rows.Count,
                },
                responseBody: new { ok = true, action = "item_added_existing" },
                ct);

            return new { itemId = existing.Id, qty = (double)existing.Qty };
        }

        var item = new PosOrderItem
        {
            Id = Guid.NewGuid(),
            OrderId = orderId,
            MenuItemId = menuItemId,
            Name = menuRow.Name,
            Qty = (decimal)qty,
            UnitPrice = unitPrice,
            DiscountAmount = 0,
            DiscountPercent = 0,
            Voided = false,
            PrinterId = menuRow.PrinterId,
            CreatedBy = createdBy,
            CreatedAt = DateTime.UtcNow,
            Note = cleanNote,
            CustomizationSignature = computed.Signature,
        };

        db.OrderItems.Add(item);
        await db.SaveChangesAsync(ct);

        if (computed.Rows.Count > 0)
        {
            foreach (var row in computed.Rows)
            {
                db.OrderItemCustomizations.Add(new PosOrderItemCustomization
                {
                    Id = Guid.NewGuid(),
                    OrderItemId = item.Id,
                    GroupId = row.GroupId,
                    OptionId = row.OptionId,
                    GroupName = row.GroupName,
                    OptionName = row.OptionName,
                    Qty = (decimal)row.Qty,
                    PriceDelta = row.PriceDelta,
                    CreatedAt = DateTime.UtcNow,
                });
            }
            await db.SaveChangesAsync(ct);
        }

        await WriteOrderAuditAsync(
            actorUserId: createdBy,
            method: "POST",
            path: $"/api/orders/{orderId}/items",
            requestBody: new
            {
                orderId,
                itemId = item.Id,
                menuItemId,
                name = item.Name,
                qty,
                unitPrice = (double)item.UnitPrice,
                note = item.Note,
                customizationsCount = computed.Rows.Count,
            },
            responseBody: new { ok = true, action = "item_added" },
            ct);

        return new { itemId = item.Id, qty };
    }

    public async Task UpdateItem(Guid itemId, Guid userId, ItemPatch patch, CancellationToken ct)
    {
        var item = await db.OrderItems.FirstOrDefaultAsync(x => x.Id == itemId, ct);
        if (item is null)
        {
            throw new PosNotFoundException("ORDER_ITEM_NOT_FOUND");
        }

        var previousQty = item.Qty;
        decimal? newUnitPrice = null;
        string? newSignature = null;

        if (patch.HasCustomizations)
        {
            if (!item.MenuItemId.HasValue)
            {
                throw new PosNotFoundException("MENU_ITEM_NOT_FOUND");
            }

            var menu = await db.MenuItems.AsNoTracking().FirstOrDefaultAsync(x => x.Id == item.MenuItemId.Value, ct);
            if (menu is null)
            {
                throw new PosNotFoundException("MENU_ITEM_NOT_FOUND");
            }

            var computed = await ComputeCustomizations(item.MenuItemId.Value, patch.Customizations ?? [], ct);
            newUnitPrice = menu.Price + computed.PriceDelta;
            newSignature = computed.Signature;

            var existingRows = await db.OrderItemCustomizations.Where(x => x.OrderItemId == itemId).ToListAsync(ct);
            db.OrderItemCustomizations.RemoveRange(existingRows);

            foreach (var row in computed.Rows)
            {
                db.OrderItemCustomizations.Add(new PosOrderItemCustomization
                {
                    Id = Guid.NewGuid(),
                    OrderItemId = itemId,
                    GroupId = row.GroupId,
                    OptionId = row.OptionId,
                    GroupName = row.GroupName,
                    OptionName = row.OptionName,
                    Qty = (decimal)row.Qty,
                    PriceDelta = row.PriceDelta,
                    CreatedAt = DateTime.UtcNow,
                });
            }
        }

        if (patch.Qty.HasValue)
        {
            item.Qty = (decimal)patch.Qty.Value;
        }

        if (patch.DiscountAmount.HasValue)
        {
            item.DiscountAmount = (decimal)patch.DiscountAmount.Value;
        }

        if (patch.DiscountPercent.HasValue)
        {
            item.DiscountPercent = (decimal)patch.DiscountPercent.Value;
        }

        if (patch.HasNote)
        {
            item.Note = string.IsNullOrWhiteSpace(patch.Note) ? null : patch.Note.Trim();
        }

        if (patch.HasCustomizations)
        {
            item.UnitPrice = newUnitPrice ?? item.UnitPrice;
            item.CustomizationSignature = string.IsNullOrWhiteSpace(newSignature) ? null : newSignature;
        }

        await db.SaveChangesAsync(ct);

        if (patch.Qty.HasValue)
        {
            var qtyDelta = (double)(item.Qty - previousQty);
            if (Math.Abs(qtyDelta) > 0.0001d)
            {
                var isAddition = qtyDelta > 0;
                await WriteOrderAuditAsync(
                    actorUserId: userId,
                    method: "PATCH",
                    path: isAddition
                        ? $"/api/orders/items/{itemId}/qty/add"
                        : $"/api/orders/items/{itemId}/qty/remove",
                    requestBody: new
                    {
                        orderId = item.OrderId,
                        itemId = item.Id,
                        name = item.Name,
                        previousQty = (double)previousQty,
                        newQty = (double)item.Qty,
                        qtyDelta,
                    },
                    responseBody: new { ok = true, action = isAddition ? "item_qty_added" : "item_qty_removed" },
                    ct);
            }
        }
    }

    public async Task VoidItem(Guid itemId, Guid userId, string reason, CancellationToken ct)
    {
        var item = await db.OrderItems.FirstOrDefaultAsync(x => x.Id == itemId, ct);
        if (item is null || item.Voided)
        {
            throw new PosNotFoundException("ORDER_ITEM_NOT_FOUND_OR_ALREADY_VOIDED");
        }

        item.Voided = true;
        item.VoidReason = reason;
        item.VoidedBy = userId;
        item.VoidedAt = DateTime.UtcNow;

        await db.SaveChangesAsync(ct);

        await WriteOrderAuditAsync(
            actorUserId: userId,
            method: "POST",
            path: $"/api/orders/items/{itemId}/void",
            requestBody: new
            {
                orderId = item.OrderId,
                itemId = item.Id,
                name = item.Name,
                reason,
                voidedAt = item.VoidedAt,
            },
            responseBody: new { ok = true, action = "item_voided" },
            ct);
    }

    public async Task UpdateOrder(Guid orderId, OrderPatch patch, CancellationToken ct)
    {
        var order = await db.Orders.FirstOrDefaultAsync(x => x.Id == orderId, ct);
        if (order is null)
        {
            throw new PosNotFoundException("ORDER_NOT_FOUND");
        }

        Guid? cleanTableId = null;
        if (patch.HasTableId)
        {
            cleanTableId = patch.TableId;
            if (cleanTableId.HasValue)
            {
                var clash = await db.Orders.AsNoTracking()
                    .AnyAsync(x => x.Status == "open" && x.TableId == cleanTableId && x.Id != orderId, ct);
                if (clash)
                {
                    throw new PosRuleException("TABLE_ALREADY_HAS_OPEN_ORDER");
                }
            }

            order.TableId = cleanTableId;
            if (cleanTableId.HasValue)
            {
                order.IsTakeaway = false;
            }
        }

        if (patch.HasIsTakeaway && !patch.HasTableId)
        {
            order.IsTakeaway = patch.IsTakeaway ?? order.IsTakeaway;
        }

        if (patch.PeopleCount.HasValue)
        {
            order.PeopleCount = (short)patch.PeopleCount.Value;
        }

        if (patch.HasCustomerId)
        {
            order.CustomerId = patch.CustomerId;
            if (patch.CustomerId.HasValue)
            {
                var customer = await db.Customers.AsNoTracking()
                    .FirstOrDefaultAsync(x => x.Id == patch.CustomerId.Value && x.IsActive, ct);
                order.CustomerDiscountPercent = customer?.DiscountPercent ?? 0;
            }
            else
            {
                order.CustomerDiscountPercent = 0;
            }
        }

        if (patch.OrderDiscountAmount.HasValue)
        {
            order.DiscountAmount = (decimal)patch.OrderDiscountAmount.Value;
        }

        if (patch.OrderDiscountPercent.HasValue)
        {
            order.DiscountPercent = (decimal)patch.OrderDiscountPercent.Value;
        }

        if (patch.ServiceFeeAmount.HasValue)
        {
            order.ServiceFee = (decimal)patch.ServiceFeeAmount.Value;
        }

        if (patch.ServiceFeePercent.HasValue)
        {
            order.ServiceFeePercent = (decimal)patch.ServiceFeePercent.Value;
        }

        if (patch.HasNickname)
        {
            order.Nickname = string.IsNullOrWhiteSpace(patch.Nickname)
                ? null
                : patch.Nickname.Trim();
        }

        await db.SaveChangesAsync(ct);
    }

    public async Task AssignOrderDestination(Guid orderId, Guid? tableId, bool isTakeaway, CancellationToken ct)
    {
        var order = await db.Orders.FirstOrDefaultAsync(x => x.Id == orderId, ct);
        if (order is null)
        {
            throw new PosNotFoundException("ORDER_NOT_FOUND");
        }

        if (order.Status != "open" && order.Status != "draft")
        {
            throw new PosRuleException("ORDER_NOT_OPEN");
        }

        if (isTakeaway)
        {
            if (tableId.HasValue)
            {
                throw new PosRuleException("DESTINATION_CONFLICT");
            }
        }
        else
        {
            if (!tableId.HasValue)
            {
                throw new PosRuleException("DESTINATION_REQUIRED");
            }

            var clash = await db.Orders.AsNoTracking()
                .AnyAsync(x => x.Status == "open" && x.TableId == tableId && x.Id != orderId, ct);
            if (clash)
            {
                throw new PosRuleException("TABLE_ALREADY_HAS_OPEN_ORDER");
            }
        }

        order.TableId = isTakeaway ? null : tableId;
        order.IsTakeaway = isTakeaway;
        if (order.Status == "draft")
        {
            order.Status = "open";
        }

        await db.SaveChangesAsync(ct);
    }

    public async Task<Dictionary<string, object>> AddPayment(Guid orderId, Guid userId, string method, double amount, string? reference, CancellationToken ct)
    {
        if (amount <= 0)
        {
            throw new PosRuleException("PAYMENT_AMOUNT_INVALID");
        }

        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var order = await db.Orders.FirstOrDefaultAsync(x => x.Id == orderId, ct);
        if (order is null)
        {
            throw new PosNotFoundException("ORDER_NOT_FOUND");
        }

        if (order.Status != "open")
        {
            if (order.Status == "draft")
            {
                order.Status = "open";
            }
            else
            {
                throw new PosRuleException("ORDER_NOT_OPEN");
            }
        }

        var now = DateTime.UtcNow;
        var payment = new PosPayment
        {
            Id = Guid.NewGuid(),
            OrderId = orderId,
            Method = method,
            Amount = (decimal)amount,
            Reference = string.IsNullOrWhiteSpace(reference) ? null : reference.Trim(),
            CreatedBy = userId,
            CreatedAt = now,
        };
        db.Payments.Add(payment);

        await systemAccounts.EnsureVaultBaseAccounts(now, ct);
        var targetAccount = await systemAccounts.ResolveAccountForPayment(order.ShiftId, method, ct);
        if (targetAccount is not null)
        {
            db.AccountTransactions.Add(new PosAccountTransaction
            {
                Id = Guid.NewGuid(),
                AccountId = targetAccount.Id,
                Direction = "in",
                Amount = (decimal)amount,
                SourceType = "pos_payment",
                SourceId = payment.Id,
                Note = $"POS payment ({SystemAccountsService.NormalizeMethod(method)})",
                CreatedBy = userId,
                CreatedAt = now,
            });
        }

        await db.SaveChangesAsync(ct);

        var totals = await ComputeTotalsForOrder(orderId, ct);
        var balance = GetDouble(totals["balance"]);

        if (balance <= 0.0001)
        {
            order.Status = "paid";
            await db.SaveChangesAsync(ct);
        }

        await tx.CommitAsync(ct);

        await WriteOrderAuditAsync(
            actorUserId: userId,
            method: "POST",
            path: $"/api/orders/{orderId}/payments",
            requestBody: new
            {
                orderId,
                method,
                amount,
                reference = string.IsNullOrWhiteSpace(reference) ? null : reference.Trim(),
                orderStatusAfter = order.Status,
                balanceAfter = balance,
            },
            responseBody: new { ok = true, action = "payment_added" },
            ct);

        return totals;
    }

    public async Task<List<object>> KitchenPendingByPrinter(Guid orderId, CancellationToken ct)
    {
        var rows = await db.OrderItems
            .AsNoTracking()
            .Where(x => x.OrderId == orderId && !x.Voided && x.PrinterId != null && x.KitchenPrintedAt == null)
            .OrderBy(x => x.CreatedAt)
            .Select(x => new
            {
                x.PrinterId,
                x.Id,
                x.Name,
                x.Qty,
                x.Note,
            })
            .ToListAsync(ct);

        var itemIds = rows.Select(x => x.Id).ToList();
        var cust = await db.OrderItemCustomizations
            .AsNoTracking()
            .Where(x => itemIds.Contains(x.OrderItemId))
            .OrderBy(x => x.CreatedAt)
            .ToListAsync(ct);

        var byItem = cust
            .GroupBy(x => x.OrderItemId)
            .ToDictionary(
                g => g.Key,
                g => g.Select(x => (object)new
                {
                    name = x.OptionName,
                    qty = (double)x.Qty,
                }).ToList());

        var byPrinter = rows
            .GroupBy(x => x.PrinterId!.Value)
            .Select(g => (object)new
            {
                printerId = g.Key,
                items = g.Select(x => (object)new
                {
                    itemId = x.Id,
                    name = x.Name,
                    qty = (double)x.Qty,
                    note = x.Note,
                    customizations = byItem.TryGetValue(x.Id, out var list) ? list : new List<object>(),
                }).ToList(),
            })
            .ToList();

        return byPrinter;
    }

    public async Task MarkKitchenPrinted(IReadOnlyList<Guid> itemIds, CancellationToken ct)
    {
        if (itemIds.Count == 0)
        {
            return;
        }

        var rows = await db.OrderItems.Where(x => itemIds.Contains(x.Id)).ToListAsync(ct);
        var now = DateTime.UtcNow;
        foreach (var row in rows)
        {
            row.KitchenPrintedAt = now;
        }

        await db.SaveChangesAsync(ct);
    }

    public async Task<object> PrintKitchenForOrder(Guid orderId, CancellationToken ct)
    {
        var pending = await KitchenPendingByPrinter(orderId, ct);

        var itemIds = await db.OrderItems
            .AsNoTracking()
            .Where(x => x.OrderId == orderId && !x.Voided && x.PrinterId != null && x.KitchenPrintedAt == null)
            .Select(x => x.Id)
            .ToListAsync(ct);

        await MarkKitchenPrinted(itemIds, ct);

        return new
        {
            printed = true,
            printerCount = pending.Count,
            itemCount = itemIds.Count,
        };
    }

    public Dictionary<string, object> ComputeTotals(
        IDictionary<string, object?> order,
        IReadOnlyList<IDictionary<string, object?>> items,
        IReadOnlyList<object> payments)
    {
        var customerDiscPct = GetDouble(order["customerDiscountPercent"]);
        var orderDiscAmt = GetDouble(order["orderDiscountAmount"]);
        var orderDiscPct = GetDouble(order["orderDiscountPercent"]);
        var svcAmt = GetDouble(order["serviceFeeAmount"]);
        var svcPct = GetDouble(order["serviceFeePercent"]);

        var subtotal = 0d;
        var itemDiscountTotal = 0d;

        foreach (var item in items)
        {
            var voided = item.TryGetValue("voided", out var voidedObj) && (voidedObj as bool? ?? false);
            if (voided)
            {
                continue;
            }

            var qty = GetDouble(item["qty"]);
            var unit = GetDouble(item["unitPrice"]);
            var gross = qty * unit;

            var dp = GetDouble(item["discountPercent"]);
            var da = GetDouble(item["discountAmount"]);

            var disc = dp > 0 ? gross * (dp / 100d) : da;
            var net = Math.Max(0, gross - disc);

            subtotal += net;
            itemDiscountTotal += disc;
        }

        var customerDiscount = Math.Max(0, subtotal * (customerDiscPct / 100d));
        var afterCustomer = Math.Max(0, subtotal - customerDiscount);

        var orderDiscount = orderDiscPct > 0 ? afterCustomer * (orderDiscPct / 100d) : orderDiscAmt;
        var afterOrderDiscount = Math.Max(0, afterCustomer - Math.Max(0, orderDiscount));

        var serviceFee = svcPct > 0 ? afterOrderDiscount * (svcPct / 100d) : svcAmt;
        var total = Math.Max(0, afterOrderDiscount + Math.Max(0, serviceFee));

        var paid = 0d;
        var byMethod = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);

        foreach (var pObj in payments)
        {
            if (pObj is not IDictionary<string, object?> p)
            {
                continue;
            }

            var method = (p.TryGetValue("method", out var methodObj) ? methodObj?.ToString() : null)?.ToLowerInvariant() ?? string.Empty;
            var amount = GetDouble(p["amount"]);

            paid += amount;
            byMethod[method] = byMethod.TryGetValue(method, out var cur) ? cur + amount : amount;
        }

        var balance = total - paid;

        return new Dictionary<string, object>
        {
            ["subtotal"] = subtotal,
            ["itemDiscountTotal"] = itemDiscountTotal,
            ["customerDiscount"] = customerDiscount,
            ["orderDiscount"] = Math.Max(0, orderDiscount),
            ["serviceFee"] = Math.Max(0, serviceFee),
            ["total"] = total,
            ["paid"] = paid,
            ["balance"] = balance,
            ["paidByMethod"] = byMethod,
        };
    }

    public async Task<Dictionary<string, object>> ComputeTotalsForOrder(Guid orderId, CancellationToken ct)
    {
        var orderRaw = await db.Orders
            .AsNoTracking()
            .Where(x => x.Id == orderId)
            .Select(x => new
            {
                x.CustomerDiscountPercent,
                x.DiscountAmount,
                x.DiscountPercent,
                x.ServiceFee,
                x.ServiceFeePercent,
            })
            .FirstOrDefaultAsync(ct);

        if (orderRaw is null)
        {
            throw new PosNotFoundException("ORDER_NOT_FOUND");
        }

        var order = new Dictionary<string, object?>
        {
            ["customerDiscountPercent"] = (double)orderRaw.CustomerDiscountPercent,
            ["orderDiscountAmount"] = (double)orderRaw.DiscountAmount,
            ["orderDiscountPercent"] = (double)orderRaw.DiscountPercent,
            ["serviceFeeAmount"] = (double)orderRaw.ServiceFee,
            ["serviceFeePercent"] = (double)orderRaw.ServiceFeePercent,
        };

        var itemsRaw = await db.OrderItems
            .AsNoTracking()
            .Where(x => x.OrderId == orderId)
            .Select(x => new { x.Qty, x.UnitPrice, x.DiscountAmount, x.DiscountPercent, x.Voided })
            .ToListAsync(ct);

        var items = itemsRaw.Select(x => (IDictionary<string, object?>)new Dictionary<string, object?>
        {
            ["qty"] = (double)x.Qty,
            ["unitPrice"] = (double)x.UnitPrice,
            ["discountAmount"] = (double)x.DiscountAmount,
            ["discountPercent"] = (double)x.DiscountPercent,
            ["voided"] = x.Voided,
        }).ToList();

        var paymentsRaw = await db.Payments
            .AsNoTracking()
            .Where(x => x.OrderId == orderId)
            .Select(x => new { x.Method, x.Amount })
            .ToListAsync(ct);

        var payments = paymentsRaw.Select(x => (object)new Dictionary<string, object?>
        {
            ["method"] = x.Method,
            ["amount"] = (double)x.Amount,
        }).ToList();

        return ComputeTotals(order, items, payments);
    }

    public async Task EnsureOrderOpen(Guid orderId, CancellationToken ct)
    {
        var row = await db.Orders.AsNoTracking().FirstOrDefaultAsync(x => x.Id == orderId, ct);
        if (row is null)
        {
            throw new PosNotFoundException("ORDER_NOT_FOUND");
        }

        if (row.Status != "open")
        {
            throw new PosRuleException("ORDER_LOCKED");
        }
    }

    public async Task ReopenOrder(Guid orderId, bool clearPayments, CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var order = await db.Orders.FirstOrDefaultAsync(x => x.Id == orderId, ct);
        if (order is null)
        {
            throw new PosNotFoundException("ORDER_NOT_FOUND");
        }

        if (clearPayments)
        {
            var payments = await db.Payments.Where(x => x.OrderId == orderId).ToListAsync(ct);
            if (payments.Count > 0)
            {
                db.Payments.RemoveRange(payments);
            }
        }

        order.Status = "open";
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
    }

    public async Task DiscardDraftOrder(Guid orderId, Guid userId, CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var order = await db.Orders.FirstOrDefaultAsync(x => x.Id == orderId, ct);
        if (order is null)
        {
            throw new PosNotFoundException("ORDER_NOT_FOUND");
        }

        if (!string.Equals(order.Status, "draft", StringComparison.OrdinalIgnoreCase))
        {
            throw new PosRuleException("ORDER_NOT_DRAFT");
        }

        var items = await db.OrderItems.Where(x => x.OrderId == orderId).ToListAsync(ct);
        if (items.Count > 0)
        {
            var itemIds = items.Select(x => x.Id).ToList();
            var customizations = await db.OrderItemCustomizations
                .Where(x => itemIds.Contains(x.OrderItemId))
                .ToListAsync(ct);
            if (customizations.Count > 0)
            {
                db.OrderItemCustomizations.RemoveRange(customizations);
            }

            db.OrderItems.RemoveRange(items);
        }

        var payments = await db.Payments.Where(x => x.OrderId == orderId).ToListAsync(ct);
        if (payments.Count > 0)
        {
            db.Payments.RemoveRange(payments);
        }

        var queueItems = await db.PrintQueue.Where(x => x.OrderId == orderId).ToListAsync(ct);
        if (queueItems.Count > 0)
        {
            db.PrintQueue.RemoveRange(queueItems);
        }

        db.Orders.Remove(order);
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);

        await WriteOrderAuditAsync(
            actorUserId: userId,
            method: "DELETE",
            path: $"/api/orders/{orderId}",
            requestBody: new
            {
                orderId,
                deletedItems = items.Count,
                deletedPayments = payments.Count,
                deletedPrintQueueItems = queueItems.Count,
            },
            responseBody: new { ok = true, action = "draft_order_discarded" },
            ct);
    }

    public async Task ChangeTable(Guid orderId, Guid? newTableId, CancellationToken ct)
    {
        var order = await db.Orders.FirstOrDefaultAsync(x => x.Id == orderId, ct);
        if (order is null)
        {
            throw new PosNotFoundException("ORDER_NOT_FOUND");
        }

        if (order.Status != "open")
        {
            throw new PosRuleException("ORDER_LOCKED");
        }

        if (newTableId.HasValue)
        {
            var hasClash = await db.Orders.AsNoTracking()
                .AnyAsync(x => x.Status == "open" && x.TableId == newTableId && x.Id != orderId, ct);
            if (hasClash)
            {
                throw new PosRuleException("TABLE_ALREADY_HAS_OPEN_ORDER");
            }
        }

        order.TableId = newTableId;
        order.IsTakeaway = false;
        await db.SaveChangesAsync(ct);
    }

    public async Task<MergeOrdersResult> MergeOrders(
        IReadOnlyList<Guid> orderIds,
        Guid actorUserId,
        Guid? targetOrderId,
        CancellationToken ct)
    {
        var uniqueIds = orderIds
            .Where(x => x != Guid.Empty)
            .Distinct()
            .ToList();

        if (uniqueIds.Count < 2)
        {
            throw new PosRuleException("MERGE_MIN_2");
        }

        var targetId = targetOrderId ?? uniqueIds[0];
        if (!uniqueIds.Contains(targetId))
        {
            throw new PosRuleException("MERGE_TARGET_INVALID");
        }

        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var orders = await db.Orders
            .Where(x => uniqueIds.Contains(x.Id))
            .ToListAsync(ct);

        if (orders.Count != uniqueIds.Count)
        {
            throw new PosNotFoundException("ORDER_NOT_FOUND");
        }

        if (orders.Any(x => x.Status != "open"))
        {
            throw new PosRuleException("ORDER_LOCKED");
        }

        var target = orders.FirstOrDefault(x => x.Id == targetId);
        if (target is null)
        {
            throw new PosRuleException("MERGE_TARGET_INVALID");
        }

        var sourceIds = uniqueIds
            .Where(x => x != targetId)
            .ToList();

        if (sourceIds.Count == 0)
        {
            throw new PosRuleException("MERGE_MIN_2");
        }

        var sourceSet = sourceIds.ToHashSet();

        var sourceItems = await db.OrderItems
            .Where(x => sourceSet.Contains(x.OrderId))
            .ToListAsync(ct);
        foreach (var item in sourceItems)
        {
            item.OrderId = targetId;
        }

        var sourcePayments = await db.Payments
            .Where(x => sourceSet.Contains(x.OrderId))
            .ToListAsync(ct);
        foreach (var payment in sourcePayments)
        {
            payment.OrderId = targetId;
        }

        var sourceQueueItems = await db.PrintQueue
            .Where(x => sourceSet.Contains(x.OrderId))
            .ToListAsync(ct);
        foreach (var queueItem in sourceQueueItems)
        {
            queueItem.OrderId = targetId;
        }

        var sourceOrders = orders.Where(x => sourceSet.Contains(x.Id)).ToList();
        if (sourceOrders.Count > 0)
        {
            db.Orders.RemoveRange(sourceOrders);
        }

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);

        await WriteOrderAuditAsync(
            actorUserId: actorUserId,
            method: "POST",
            path: "/api/orders/merge",
            requestBody: new
            {
                targetOrderId = target.Id,
                targetOrderNo = target.OrderNo.ToString("00"),
                removedOrderIds = sourceIds,
                removedOrdersCount = sourceIds.Count,
                movedItemsCount = sourceItems.Count,
                movedPaymentsCount = sourcePayments.Count,
                movedPrintQueueCount = sourceQueueItems.Count,
            },
            responseBody: new { ok = true, action = "orders_merged" },
            ct);

        return new MergeOrdersResult(
            TargetOrderId: targetId,
            TargetOrderNo: target.OrderNo.ToString("00"),
            MergedOrderIds: sourceIds,
            MergedOrderCount: sourceIds.Count,
            MovedItemsCount: sourceItems.Count,
            MovedPaymentsCount: sourcePayments.Count,
            MovedPrintQueueCount: sourceQueueItems.Count);
    }

    public async Task<List<object>> KitchenPendingItemsForPrinter(Guid orderId, Guid printerId, CancellationToken ct)
    {
        var rows = await db.OrderItems
            .AsNoTracking()
            .Where(x => x.OrderId == orderId && !x.Voided && x.PrinterId == printerId && x.KitchenPrintedAt == null)
            .OrderBy(x => x.CreatedAt)
            .Select(x => new
            {
                x.Id,
                x.Name,
                x.Qty,
                x.Note,
            })
            .ToListAsync(ct);

        if (rows.Count == 0)
        {
            return [];
        }

        var itemIds = rows.Select(x => x.Id).ToList();
        var customizations = await db.OrderItemCustomizations
            .AsNoTracking()
            .Where(x => itemIds.Contains(x.OrderItemId))
            .OrderBy(x => x.CreatedAt)
            .Select(x => new
            {
                x.OrderItemId,
                x.OptionName,
                x.Qty,
            })
            .ToListAsync(ct);

        var byItem = customizations.GroupBy(x => x.OrderItemId).ToDictionary(
            g => g.Key,
            g => g.Select(x => (object)new
            {
                name = x.OptionName,
                qty = (double)x.Qty,
            }).ToList());

        return rows.Select(x => (object)new
        {
            itemId = x.Id,
            name = x.Name,
            qty = (double)x.Qty,
            note = x.Note,
            customizations = byItem.TryGetValue(x.Id, out var list) ? list : new List<object>(),
        }).ToList();
    }

    public async Task<List<Guid>> KitchenPendingItemIdsForPrinter(Guid orderId, Guid printerId, CancellationToken ct)
    {
        return await db.OrderItems
            .AsNoTracking()
            .Where(x => x.OrderId == orderId && !x.Voided && x.PrinterId == printerId && x.KitchenPrintedAt == null)
            .OrderBy(x => x.CreatedAt)
            .Select(x => x.Id)
            .ToListAsync(ct);
    }

    private async Task<PosOrder?> FindOpenOrderForTable(Guid tableId, CancellationToken ct)
    {
        return await db.Orders
            .OrderByDescending(x => x.CreatedAt)
            .FirstOrDefaultAsync(x => x.Status == "open" && x.TableId == tableId, ct);
    }

    private async Task<int> NextOrderNo(DateOnly businessDate, CancellationToken ct)
    {
        var counter = await db.OrderCounters.FirstOrDefaultAsync(x => x.BusinessDate == businessDate, ct);
        if (counter is null)
        {
            counter = new PosOrderCounter
            {
                BusinessDate = businessDate,
                NextNo = 1,
            };
            db.OrderCounters.Add(counter);
            await db.SaveChangesAsync(ct);
        }

        for (var i = 0; i < 99; i++)
        {
            var candidate = counter.NextNo;
            counter.NextNo = candidate >= 99 ? 1 : candidate + 1;
            await db.SaveChangesAsync(ct);

            var exists = await db.Orders.AsNoTracking()
                .AnyAsync(x => x.BusinessDate == businessDate && x.OrderNo == (short)candidate, ct);

            if (!exists)
            {
                return candidate;
            }
        }

        throw new PosRuleException("ORDER_NO_EXHAUSTED");
    }

    private async Task<CustomizationComputeResult> ComputeCustomizations(Guid menuItemId, IReadOnlyList<CustomizationSelection> selections, CancellationToken ct)
    {
        var groups = await db.MenuItemOptionGroups
            .AsNoTracking()
            .Where(x => x.MenuItemId == menuItemId && x.IsActive)
            .ToListAsync(ct);

        if (groups.Count == 0)
        {
            return new CustomizationComputeResult(0, null, []);
        }

        var groupInfo = groups.ToDictionary(
            g => g.Id,
            g => new GroupInfo(g.Id, g.Name, g.IsRequired, g.MinSelect, g.MaxSelect, g.AllowQuantity));

        var optionQty = new Dictionary<Guid, double>();
        foreach (var s in selections)
        {
            if (s.Qty <= 0)
            {
                continue;
            }

            optionQty[s.OptionId] = optionQty.TryGetValue(s.OptionId, out var cur) ? cur + s.Qty : s.Qty;
        }

        if (optionQty.Count == 0)
        {
            ValidateGroupSelection(groupInfo, new Dictionary<Guid, int>());
            return new CustomizationComputeResult(0, null, []);
        }

        var optionIds = optionQty.Keys.ToList();
        var rows = await (
            from o in db.MenuItemOptions.AsNoTracking()
            join g in db.MenuItemOptionGroups.AsNoTracking() on o.GroupId equals g.Id
            where optionIds.Contains(o.Id)
                && g.MenuItemId == menuItemId
                && o.IsActive
                && g.IsActive
            select new
            {
                OptionId = o.Id,
                OptionName = o.Name,
                o.PriceDelta,
                o.MaxQty,
                GroupId = g.Id,
                GroupName = g.Name,
                g.IsRequired,
                g.MinSelect,
                g.MaxSelect,
                g.AllowQuantity,
            })
            .ToListAsync(ct);

        if (rows.Count != optionIds.Count)
        {
            throw new PosRuleException("CUSTOMIZATION_INVALID");
        }

        var selectedCounts = new Dictionary<Guid, int>();
        var outRows = new List<CustomizationRow>();
        decimal priceDelta = 0;

        foreach (var row in rows)
        {
            var qty = optionQty[row.OptionId];
            if (qty <= 0)
            {
                continue;
            }

            if (!row.AllowQuantity && Math.Abs(qty - 1) > 0.0001)
            {
                throw new PosRuleException("CUSTOMIZATION_INVALID");
            }

            if (row.AllowQuantity && row.MaxQty.HasValue && qty > row.MaxQty.Value)
            {
                throw new PosRuleException("CUSTOMIZATION_INVALID");
            }

            selectedCounts[row.GroupId] = selectedCounts.TryGetValue(row.GroupId, out var cur) ? cur + 1 : 1;
            groupInfo[row.GroupId] = new GroupInfo(
                row.GroupId,
                row.GroupName,
                row.IsRequired,
                row.MinSelect,
                row.MaxSelect,
                row.AllowQuantity);

            priceDelta += row.PriceDelta * (decimal)qty;

            outRows.Add(new CustomizationRow(
                row.GroupId,
                row.OptionId,
                row.GroupName,
                row.OptionName,
                qty,
                row.PriceDelta));
        }

        ValidateGroupSelection(groupInfo, selectedCounts);

        var sig = optionQty
            .Where(x => x.Value > 0)
            .Select(x => $"{x.Key}:{QtySig(x.Value)}")
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();

        var signature = sig.Count == 0 ? null : string.Join("|", sig);
        return new CustomizationComputeResult(priceDelta, signature, outRows);
    }

    private static void ValidateGroupSelection(IReadOnlyDictionary<Guid, GroupInfo> groups, IReadOnlyDictionary<Guid, int> selectedCounts)
    {
        foreach (var group in groups.Values)
        {
            var count = selectedCounts.TryGetValue(group.Id, out var c) ? c : 0;
            var minSel = group.IsRequired ? Math.Max(group.MinSelect, 1) : group.MinSelect;
            if (minSel > 0 && count < minSel)
            {
                throw new PosRuleException("CUSTOMIZATION_REQUIRED");
            }

            if (group.MaxSelect.HasValue && count > group.MaxSelect.Value)
            {
                throw new PosRuleException("CUSTOMIZATION_INVALID");
            }
        }
    }

    private static string QtySig(double value)
    {
        var s = value.ToString("0.###", System.Globalization.CultureInfo.InvariantCulture);
        return s;
    }

    private static double GetDouble(object? value)
    {
        return value switch
        {
            null => 0,
            double d => d,
            float f => f,
            decimal m => (double)m,
            int i => i,
            long l => l,
            short s => s,
            string str when double.TryParse(str, out var parsed) => parsed,
            _ => 0,
        };
    }

    private async Task WriteOrderAuditAsync(
        Guid actorUserId,
        string method,
        string path,
        object? requestBody,
        object? responseBody,
        CancellationToken ct)
    {
        try
        {
            var actor = await ResolveAuditActorAsync(actorUserId, ct);
            await audit.WriteAuditLog(
                userId: actor?.Id ?? actorUserId,
                username: actor?.Username,
                role: actor?.Role,
                method: method,
                path: path,
                statusCode: 200,
                requestBody: SerializeAuditBody(requestBody),
                responseBody: SerializeAuditBody(responseBody),
                ct: ct);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Failed to persist order audit log for {Method} {Path} by user {UserId}",
                method,
                path,
                actorUserId);
        }
    }

    private async Task<AuditActor?> ResolveAuditActorAsync(Guid userId, CancellationToken ct)
    {
        if (userId == Guid.Empty)
        {
            return null;
        }

        return await db.Users
            .AsNoTracking()
            .Where(x => x.Id == userId)
            .Select(x => new AuditActor(x.Id, x.Username, x.Role))
            .FirstOrDefaultAsync(ct);
    }

    private static string? SerializeAuditBody(object? value)
    {
        if (value is null)
        {
            return null;
        }

        try
        {
            return JsonSerializer.Serialize(value);
        }
        catch
        {
            return null;
        }
    }

    private sealed record GroupInfo(Guid Id, string Name, bool IsRequired, int MinSelect, int? MaxSelect, bool AllowQuantity);
    private sealed record CustomizationRow(Guid GroupId, Guid OptionId, string GroupName, string OptionName, double Qty, decimal PriceDelta);
    private sealed record CustomizationComputeResult(decimal PriceDelta, string? Signature, IReadOnlyList<CustomizationRow> Rows);
    private sealed record AuditActor(Guid Id, string Username, string Role);
}

public sealed record CustomizationSelection(Guid OptionId, double Qty);

public sealed record OrderSummaryResult(Guid Id, string BusinessDate, string OrderNo, string Status, bool Existing)
{
    public object ToJson()
    {
        return new
        {
            id = Id,
            businessDate = BusinessDate,
            orderNo = OrderNo,
            status = Status,
            existing = Existing,
        };
    }
}

public sealed record ItemPatch(
    double? Qty,
    double? DiscountAmount,
    double? DiscountPercent,
    bool HasNote,
    string? Note,
    bool HasCustomizations,
    IReadOnlyList<CustomizationSelection>? Customizations);

public sealed record OrderPatch(
    bool HasTableId,
    Guid? TableId,
    int? PeopleCount,
    bool HasCustomerId,
    Guid? CustomerId,
    double? OrderDiscountAmount,
    double? OrderDiscountPercent,
    double? ServiceFeeAmount,
    double? ServiceFeePercent,
    bool HasIsTakeaway,
    bool? IsTakeaway,
    bool HasNickname,
    string? Nickname);

public sealed record MergeOrdersResult(
    Guid TargetOrderId,
    string TargetOrderNo,
    IReadOnlyList<Guid> MergedOrderIds,
    int MergedOrderCount,
    int MovedItemsCount,
    int MovedPaymentsCount,
    int MovedPrintQueueCount)
{
    public object ToJson()
    {
        return new
        {
            targetOrderId = TargetOrderId,
            targetOrderNo = TargetOrderNo,
            mergedOrderIds = MergedOrderIds,
            mergedOrderCount = MergedOrderCount,
            movedItemsCount = MovedItemsCount,
            movedPaymentsCount = MovedPaymentsCount,
            movedPrintQueueCount = MovedPrintQueueCount,
        };
    }
}

public sealed class PosRuleException(string code) : Exception(code)
{
    public string Code { get; } = code;
}

public sealed class PosNotFoundException(string code) : Exception(code)
{
    public string Code { get; } = code;
}
