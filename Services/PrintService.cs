using System.Net.Sockets;
using System.Text;
using Microsoft.EntityFrameworkCore;
using PosBackend.AspNet.Data;
using PosBackend.AspNet.Infrastructure;
using PosBackend.AspNet.Models;

namespace PosBackend.AspNet.Services;

public sealed class PrintService(PosDbContext db, OrdersService orders)
{
    private const int Paper80LineWidth = 48;

    public Task<object> PrintKitchenForOrder(Guid orderId, CancellationToken ct)
        => PrintKitchenForOrder(orderId, null, ct);

    public async Task<object> PrintKitchenForOrder(Guid orderId, string? language, CancellationToken ct)
    {
        var pending = await db.OrderItems
            .AsNoTracking()
            .Where(x => x.OrderId == orderId && !x.Voided && x.PrinterId != null && x.KitchenPrintedAt == null)
            .Select(x => new { PrinterId = x.PrinterId!.Value, x.Id })
            .ToListAsync(ct);

        var unassigned = await db.OrderItems
            .AsNoTracking()
            .Where(x => x.OrderId == orderId && !x.Voided && x.PrinterId == null && x.KitchenPrintedAt == null)
            .Select(x => x.Name)
            .ToListAsync(ct);

        var grouped = pending
            .GroupBy(x => x.PrinterId)
            .Select(g => new { PrinterId = g.Key, ItemIds = g.Select(x => x.Id).ToList() })
            .ToList();

        var printersUsed = 0;
        var itemsPrinted = 0;
        var errors = new List<string>();
        var queued = new List<string>();

        foreach (var group in grouped)
        {
            var printer = await db.Printers.AsNoTracking().FirstOrDefaultAsync(x => x.Id == group.PrinterId, ct);
            if (printer is null)
            {
                errors.Add($"Printer not found: {group.PrinterId}");
                continue;
            }

            if (!printer.IsActive)
            {
                await QueuePrint(orderId, printer.Id, "kitchen", "Printer inactive", ct);
                queued.Add($"{printer.Name} (inactive)");
                continue;
            }

            if (!string.Equals(printer.Type, "network", StringComparison.OrdinalIgnoreCase))
            {
                await QueuePrint(orderId, printer.Id, "kitchen", $"Unsupported printer type: {printer.Type}", ct);
                queued.Add($"{printer.Name} ({printer.Type})");
                continue;
            }

            if (string.IsNullOrWhiteSpace(printer.Address))
            {
                await QueuePrint(orderId, printer.Id, "kitchen", "Missing printer address", ct);
                queued.Add($"{printer.Name} (no address)");
                continue;
            }

            try
            {
                var ticket = await BuildKitchenTicketBytes(orderId, group.ItemIds, language, ct);
                await SendToNetworkPrinter(printer.Address!, ticket, ct);
                await orders.MarkKitchenPrinted(group.ItemIds, ct);
                await ClearQueuedPrint(orderId, printer.Id, "kitchen", ct);

                printersUsed += 1;
                itemsPrinted += group.ItemIds.Count;
            }
            catch (Exception ex)
            {
                await QueuePrint(orderId, printer.Id, "kitchen", ex.Message, ct);
                errors.Add($"{printer.Name}: {ex.Message}");
                queued.Add($"{printer.Name} (error)");
            }
        }

        var result = new Dictionary<string, object>
        {
            ["printersUsed"] = printersUsed,
            ["itemsPrinted"] = itemsPrinted,
        };
        if (errors.Count > 0)
        {
            result["errors"] = errors;
        }

        if (queued.Count > 0)
        {
            result["queued"] = queued;
        }

        if (unassigned.Count > 0)
        {
            result["skipped"] = unassigned.Select(x => $"{x} (no assigned printer)").ToList();
        }

        return result;
    }

    public async Task<List<object>> ListActivePrinters(CancellationToken ct)
    {
        return await db.Printers
            .AsNoTracking()
            .Where(x => x.IsActive)
            .OrderBy(x => x.Name)
            .Select(x => (object)new
            {
                id = x.Id,
                name = x.Name,
                type = x.Type,
                address = x.Address,
                isActive = x.IsActive,
            })
            .ToListAsync(ct);
    }

    public async Task<byte[]> BuildInvoicePdf(Guid orderId, CancellationToken ct)
    {
        var order = await (
            from o in db.Orders.AsNoTracking()
            join t in db.Tables.AsNoTracking() on o.TableId equals t.Id into tables
            from t in tables.DefaultIfEmpty()
            where o.Id == orderId
            select new
            {
                o.OrderNo,
                o.IsTakeaway,
                TableName = t != null ? t.Name : null,
                o.CreatedAt,
            }).FirstOrDefaultAsync(ct);

        if (order is null)
        {
            throw new PosNotFoundException("ORDER_NOT_FOUND");
        }

        var items = await db.OrderItems.AsNoTracking()
            .Where(x => x.OrderId == orderId)
            .OrderBy(x => x.CreatedAt)
            .Select(x => new
            {
                x.Name,
                x.Qty,
                x.UnitPrice,
                x.DiscountAmount,
                x.DiscountPercent,
            }).ToListAsync(ct);

        var totals = await orders.ComputeTotalsForOrder(orderId, ct);

        var lines = new List<string>();
        lines.Add($"Invoice #: {order.OrderNo:00}");
        lines.Add($"Table: {(order.IsTakeaway ? "Takeaway" : (string.IsNullOrWhiteSpace(order.TableName) ? "-" : order.TableName))}");
        lines.Add($"Created: {order.CreatedAt:yyyy-MM-dd HH:mm:ss}");
        lines.Add("----------------------------------------");

        foreach (var item in items)
        {
            var name = item.Name;
            var qty = (double)item.Qty;
            var unit = (double)item.UnitPrice;
            var discountAmount = (double)item.DiscountAmount;
            var discountPercent = (double)item.DiscountPercent;
            var gross = qty * unit;
            var discount = discountPercent > 0 ? gross * (discountPercent / 100d) : discountAmount;
            var net = Math.Max(0, gross - discount);
            lines.Add($"{name} x{qty:0.###}  {net:0.##}");
        }

        lines.Add("----------------------------------------");
        lines.Add($"Subtotal: {ToDouble(totals.TryGetValue("subtotal", out var subtotalObj) ? subtotalObj : null):0.##}");
        lines.Add($"Discounts: {ToDouble(totals.TryGetValue("itemDiscountTotal", out var itemDiscObj) ? itemDiscObj : null):0.##}");
        lines.Add($"Service Fee: {ToDouble(totals.TryGetValue("serviceFee", out var serviceFeeObj) ? serviceFeeObj : null):0.##}");
        lines.Add($"Total: {ToDouble(totals.TryGetValue("total", out var totalObj) ? totalObj : null):0.##}");
        lines.Add($"Paid: {ToDouble(totals.TryGetValue("paid", out var paidObj) ? paidObj : null):0.##}");
        lines.Add($"Balance: {ToDouble(totals.TryGetValue("balance", out var balanceObj) ? balanceObj : null):0.##}");

        return SimplePdfBuilder.BuildSinglePageText(lines, "Restaurant POS - Invoice");
    }

    public Task PrintReceipt(Guid orderId, Guid? printerId, CancellationToken ct)
        => PrintReceipt(orderId, printerId, null, ct);

    public async Task PrintReceipt(Guid orderId, Guid? printerId, string? language, CancellationToken ct)
    {
        var printer = await ResolveReceiptPrinter(printerId, ct);
        var ticket = await BuildReceiptTicketBytes(orderId, language, ct);
        await SendToNetworkPrinter(printer.Address!, ticket, ct);
    }

    public Task PrintInvoice(Guid orderId, Guid? printerId, CancellationToken ct)
        => PrintInvoice(orderId, printerId, null, ct);

    public async Task PrintInvoice(Guid orderId, Guid? printerId, string? language, CancellationToken ct)
    {
        var printer = await ResolveInvoicePrinter(printerId, ct);
        var ticket = await BuildInvoiceTicketBytes(orderId, language, ct);
        await SendToNetworkPrinter(printer.Address!, ticket, ct);
    }

    public async Task<Dictionary<string, object>> RetryQueueItem(Guid queueId, CancellationToken ct)
    {
        var row = await db.PrintQueue.FirstOrDefaultAsync(x => x.Id == queueId, ct);
        if (row is null)
        {
            throw new InvalidOperationException("QUEUE_NOT_FOUND");
        }

        var status = await ProcessQueueRow(row, ct);
        return new Dictionary<string, object> { ["status"] = status };
    }

    public async Task<Dictionary<string, int>> RetryPendingQueue(int limit, CancellationToken ct)
    {
        var rows = await db.PrintQueue
            .Where(x => x.Status == "pending")
            .OrderBy(x => x.UpdatedAt)
            .Take(Math.Clamp(limit, 1, 500))
            .ToListAsync(ct);

        var printed = 0;
        var cleared = 0;
        var failed = 0;

        foreach (var row in rows)
        {
            var status = await ProcessQueueRow(row, ct);
            switch (status)
            {
                case "printed":
                    printed++;
                    break;
                case "cleared":
                    cleared++;
                    break;
                default:
                    failed++;
                    break;
            }
        }

        return new Dictionary<string, int>
        {
            ["attempted"] = rows.Count,
            ["printed"] = printed,
            ["cleared"] = cleared,
            ["failed"] = failed,
        };
    }

    private async Task<string> ProcessQueueRow(PosPrintQueue row, CancellationToken ct)
    {
        if (!string.Equals(row.Kind, "kitchen", StringComparison.OrdinalIgnoreCase))
        {
            await TouchQueueFailure(row.Id, $"Unsupported kind: {row.Kind}", ct);
            return "failed";
        }

        var printer = await db.Printers.AsNoTracking().FirstOrDefaultAsync(x => x.Id == row.PrinterId, ct);
        if (printer is null)
        {
            await TouchQueueFailure(row.Id, "Printer not found", ct);
            return "failed";
        }

        if (!printer.IsActive)
        {
            await TouchQueueFailure(row.Id, "Printer inactive", ct);
            return "failed";
        }

        if (!string.Equals(printer.Type, "network", StringComparison.OrdinalIgnoreCase))
        {
            await TouchQueueFailure(row.Id, $"Unsupported printer type: {printer.Type}", ct);
            return "failed";
        }

        if (string.IsNullOrWhiteSpace(printer.Address))
        {
            await TouchQueueFailure(row.Id, "Missing printer address", ct);
            return "failed";
        }

        var pendingItemIds = await orders.KitchenPendingItemIdsForPrinter(row.OrderId, row.PrinterId, ct);
        if (pendingItemIds.Count == 0)
        {
            await DeleteQueueById(row.Id, ct);
            return "cleared";
        }

        try
        {
            var ticket = await BuildKitchenTicketBytes(row.OrderId, pendingItemIds, null, ct);
            await SendToNetworkPrinter(printer.Address!, ticket, ct);
            await orders.MarkKitchenPrinted(pendingItemIds, ct);
            await DeleteQueueById(row.Id, ct);
            return "printed";
        }
        catch (PosNotFoundException)
        {
            await DeleteQueueById(row.Id, ct);
            return "cleared";
        }
        catch (Exception ex)
        {
            await TouchQueueFailure(row.Id, ex.Message, ct);
            return "failed";
        }
    }

    private async Task TouchQueueFailure(Guid queueId, string error, CancellationToken ct)
    {
        var err = string.IsNullOrWhiteSpace(error) ? null : error.Trim();
        var rows = await db.PrintQueue.Where(x => x.Id == queueId).ToListAsync(ct);
        if (rows.Count == 0)
        {
            return;
        }

        var now = DateTime.UtcNow;
        foreach (var row in rows)
        {
            row.Attempts += 1;
            row.LastError = err;
            row.UpdatedAt = now;
        }

        await db.SaveChangesAsync(ct);
    }

    private async Task DeleteQueueById(Guid queueId, CancellationToken ct)
    {
        var rows = await db.PrintQueue.Where(x => x.Id == queueId).ToListAsync(ct);
        if (rows.Count == 0)
        {
            return;
        }

        db.PrintQueue.RemoveRange(rows);
        await db.SaveChangesAsync(ct);
    }

    private async Task QueuePrint(Guid orderId, Guid printerId, string kind, string? error, CancellationToken ct)
    {
        var err = string.IsNullOrWhiteSpace(error) ? null : error.Trim();
        var existing = await db.PrintQueue
            .Where(x => x.OrderId == orderId && x.PrinterId == printerId && x.Kind == kind && x.Status == "pending")
            .ToListAsync(ct);

        var now = DateTime.UtcNow;
        if (existing.Count > 0)
        {
            foreach (var row in existing)
            {
                row.Attempts += 1;
                row.LastError = err;
                row.UpdatedAt = now;
            }

            await db.SaveChangesAsync(ct);
            return;
        }

        db.PrintQueue.Add(new PosPrintQueue
        {
            Id = Guid.NewGuid(),
            OrderId = orderId,
            PrinterId = printerId,
            Kind = kind,
            Status = "pending",
            Attempts = 1,
            LastError = err,
            CreatedAt = now,
            UpdatedAt = now,
        });
        await db.SaveChangesAsync(ct);
    }

    private async Task ClearQueuedPrint(Guid orderId, Guid printerId, string kind, CancellationToken ct)
    {
        var rows = await db.PrintQueue
            .Where(x => x.OrderId == orderId && x.PrinterId == printerId && x.Kind == kind)
            .ToListAsync(ct);
        if (rows.Count == 0)
        {
            return;
        }

        db.PrintQueue.RemoveRange(rows);
        await db.SaveChangesAsync(ct);
    }

    private async Task<byte[]> BuildKitchenTicketBytes(Guid orderId, IReadOnlyList<Guid> itemIds, string? language, CancellationToken ct)
    {
        var order = await (
            from o in db.Orders.AsNoTracking()
            join t in db.Tables.AsNoTracking() on o.TableId equals t.Id into tables
            from t in tables.DefaultIfEmpty()
            where o.Id == orderId
            select new
            {
                o.OrderNo,
                o.IsTakeaway,
                TableName = t != null ? t.Name : null,
                o.PeopleCount,
                o.CreatedAt,
            }).FirstOrDefaultAsync(ct);

        if (order is null)
        {
            throw new PosNotFoundException("ORDER_NOT_FOUND");
        }

        var items = await db.OrderItems.AsNoTracking()
            .Where(x => itemIds.Contains(x.Id))
            .OrderBy(x => x.CreatedAt)
            .Select(x => new
            {
                x.Id,
                x.Name,
                x.Qty,
                x.Note,
            })
            .ToListAsync(ct);

        var customizations = await db.OrderItemCustomizations.AsNoTracking()
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
            g => g.Select(x => $"{x.OptionName} x{(double)x.Qty:0.###}").ToList());

        var isArabic = IsArabicLanguage(language);
        var takeawayLabel = isArabic ? "سفري" : "Takeaway";
        var tableLabel = isArabic ? "الطاولة" : "Table";
        var peopleLabel = isArabic ? "الأشخاص" : "People";
        var timeLabel = isArabic ? "الوقت" : "Time";
        var noteLabel = isArabic ? "ملاحظة" : "Note";
        var title = isArabic ? $"طلب مطبخ #{order.OrderNo:00}" : $"KITCHEN ORDER #{order.OrderNo:00}";
        var tableValue = order.IsTakeaway ? takeawayLabel : (order.TableName ?? "-");

        var sb = new StringBuilder();
        sb.AppendLine(title);
        sb.AppendLine($"{tableLabel}: {tableValue}");
        if (order.PeopleCount.HasValue)
        {
            sb.AppendLine($"{peopleLabel}: {order.PeopleCount}");
        }
        sb.AppendLine($"{timeLabel}: {order.CreatedAt:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine(new string('-', Paper80LineWidth));

        foreach (var item in items)
        {
            sb.AppendLine($"{item.Name} x{(double)item.Qty:0.###}");
            if (!string.IsNullOrWhiteSpace(item.Note))
            {
                sb.AppendLine($"  {noteLabel}: {item.Note}");
            }

            if (byItem.TryGetValue(item.Id, out var list))
            {
                foreach (var c in list)
                {
                    sb.AppendLine($"  + {c}");
                }
            }
        }

        sb.AppendLine();
        sb.AppendLine();

        return WrapEscPosText(sb.ToString());
    }

    private async Task<byte[]> BuildReceiptTicketBytes(Guid orderId, string? language, CancellationToken ct)
    {
        var order = await (
            from o in db.Orders.AsNoTracking()
            join t in db.Tables.AsNoTracking() on o.TableId equals t.Id into tables
            from t in tables.DefaultIfEmpty()
            where o.Id == orderId
            select new
            {
                o.OrderNo,
                o.IsTakeaway,
                TableName = t != null ? t.Name : null,
                o.PeopleCount,
                o.CreatedAt,
            }).FirstOrDefaultAsync(ct);

        if (order is null)
        {
            throw new PosNotFoundException("ORDER_NOT_FOUND");
        }

        var items = await db.OrderItems.AsNoTracking()
            .Where(x => x.OrderId == orderId && !x.Voided)
            .OrderBy(x => x.CreatedAt)
            .Select(x => new
            {
                x.Name,
                x.Qty,
                x.UnitPrice,
                x.DiscountAmount,
                x.DiscountPercent,
            })
            .ToListAsync(ct);

        var payments = await db.Payments.AsNoTracking()
            .Where(x => x.OrderId == orderId)
            .OrderBy(x => x.CreatedAt)
            .Select(x => new { x.Method, x.Amount })
            .ToListAsync(ct);

        var totals = await orders.ComputeTotalsForOrder(orderId, ct);

        var isArabic = IsArabicLanguage(language);
        var title = isArabic ? "نظام نقاط البيع" : "Restaurant POS";
        var receiptLabel = isArabic ? "إيصال" : "Receipt";
        var tableLabel = isArabic ? "الطاولة" : "Table";
        var takeawayLabel = isArabic ? "سفري" : "Takeaway";
        var peopleLabel = isArabic ? "الأشخاص" : "People";
        var timeLabel = isArabic ? "الوقت" : "Time";
        var totalLabel = isArabic ? "الإجمالي" : "Total";
        var paidLabel = isArabic ? "المدفوع" : "Paid";
        var dueLabel = isArabic ? "المتبقي" : "Due";
        var paymentLabel = isArabic ? "المدفوعات" : "Payments";
        var tableValue = order.IsTakeaway ? takeawayLabel : (order.TableName ?? "-");

        var sb = new StringBuilder();
        sb.AppendLine(title);
        sb.AppendLine($"{receiptLabel} #{order.OrderNo:00}");
        sb.AppendLine($"{tableLabel}: {tableValue}");
        if (order.PeopleCount.HasValue)
        {
            sb.AppendLine($"{peopleLabel}: {order.PeopleCount}");
        }
        sb.AppendLine($"{timeLabel}: {order.CreatedAt:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine(new string('-', Paper80LineWidth));

        foreach (var item in items)
        {
            var qty = (double)item.Qty;
            var unit = (double)item.UnitPrice;
            var gross = qty * unit;
            var disc = item.DiscountPercent > 0
                ? gross * ((double)item.DiscountPercent / 100d)
                : (double)item.DiscountAmount;
            var net = Math.Max(0, gross - disc);
            sb.AppendLine($"{item.Name} x{qty:0.###}  {net:0.##}");
        }

        sb.AppendLine(new string('-', Paper80LineWidth));
        sb.AppendLine($"{totalLabel}: {Convert.ToDouble(totals["total"]):0.##}");
        sb.AppendLine($"{paidLabel}: {Convert.ToDouble(totals["paid"]):0.##}");
        sb.AppendLine($"{dueLabel}: {Convert.ToDouble(totals["balance"]):0.##}");
        sb.AppendLine(new string('-', Paper80LineWidth));
        sb.AppendLine(paymentLabel);
        foreach (var pay in payments)
        {
            sb.AppendLine($"{pay.Method}: {(double)pay.Amount:0.##}");
        }
        sb.AppendLine();
        sb.AppendLine();

        return WrapEscPosText(sb.ToString());
    }

    private async Task<byte[]> BuildInvoiceTicketBytes(Guid orderId, string? language, CancellationToken ct)
    {
        var order = await (
            from o in db.Orders.AsNoTracking()
            join t in db.Tables.AsNoTracking() on o.TableId equals t.Id into tables
            from t in tables.DefaultIfEmpty()
            where o.Id == orderId
            select new
            {
                o.OrderNo,
                o.IsTakeaway,
                TableName = t != null ? t.Name : null,
                o.PeopleCount,
                o.CreatedAt,
            }).FirstOrDefaultAsync(ct);

        if (order is null)
        {
            throw new PosNotFoundException("ORDER_NOT_FOUND");
        }

        var items = await db.OrderItems.AsNoTracking()
            .Where(x => x.OrderId == orderId && !x.Voided)
            .OrderBy(x => x.CreatedAt)
            .Select(x => new
            {
                x.Name,
                x.Qty,
                x.UnitPrice,
                x.DiscountAmount,
                x.DiscountPercent,
            })
            .ToListAsync(ct);

        var totals = await orders.ComputeTotalsForOrder(orderId, ct);

        var isArabic = IsArabicLanguage(language);
        var title = isArabic ? "فاتورة" : "Invoice";
        var tableLabel = isArabic ? "الطاولة" : "Table";
        var takeawayLabel = isArabic ? "سفري" : "Takeaway";
        var peopleLabel = isArabic ? "الأشخاص" : "People";
        var timeLabel = isArabic ? "الوقت" : "Time";
        var subtotalLabel = isArabic ? "المجموع الفرعي" : "Subtotal";
        var discountLabel = isArabic ? "الخصم" : "Discounts";
        var serviceLabel = isArabic ? "رسوم الخدمة" : "Service Fee";
        var totalLabel = isArabic ? "الإجمالي" : "Total";
        var paidLabel = isArabic ? "المدفوع" : "Paid";
        var balanceLabel = isArabic ? "المتبقي" : "Balance";
        var tableValue = order.IsTakeaway ? takeawayLabel : (order.TableName ?? "-");

        var sb = new StringBuilder();
        sb.AppendLine($"{title} #{order.OrderNo:00}");
        sb.AppendLine($"{tableLabel}: {tableValue}");
        if (order.PeopleCount.HasValue)
        {
            sb.AppendLine($"{peopleLabel}: {order.PeopleCount}");
        }
        sb.AppendLine($"{timeLabel}: {order.CreatedAt:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine(new string('-', Paper80LineWidth));

        foreach (var item in items)
        {
            var qty = (double)item.Qty;
            var unit = (double)item.UnitPrice;
            var gross = qty * unit;
            var disc = item.DiscountPercent > 0
                ? gross * ((double)item.DiscountPercent / 100d)
                : (double)item.DiscountAmount;
            var net = Math.Max(0, gross - disc);
            sb.AppendLine($"{item.Name} x{qty:0.###}  {net:0.##}");
        }

        sb.AppendLine(new string('-', Paper80LineWidth));
        sb.AppendLine($"{subtotalLabel}: {Convert.ToDouble(totals["subtotal"]):0.##}");
        sb.AppendLine($"{discountLabel}: {Convert.ToDouble(totals["itemDiscountTotal"]):0.##}");
        sb.AppendLine($"{serviceLabel}: {Convert.ToDouble(totals["serviceFee"]):0.##}");
        sb.AppendLine($"{totalLabel}: {Convert.ToDouble(totals["total"]):0.##}");
        sb.AppendLine($"{paidLabel}: {Convert.ToDouble(totals["paid"]):0.##}");
        sb.AppendLine($"{balanceLabel}: {Convert.ToDouble(totals["balance"]):0.##}");
        sb.AppendLine();
        sb.AppendLine();

        return WrapEscPosText(sb.ToString());
    }

    private async Task<PosPrinter> ResolveReceiptPrinter(Guid? printerId, CancellationToken ct)
    {
        if (printerId.HasValue)
        {
            return await ResolveActiveNetworkPrinter(printerId.Value, "Receipt printer not found or inactive", "receipt", ct);
        }

        var defaultReceiptPrinterId = await GetSettingGuid(PosSettingKeys.DefaultReceiptPrinterId, ct);
        if (defaultReceiptPrinterId.HasValue)
        {
            return await ResolveActiveNetworkPrinter(defaultReceiptPrinterId.Value, "Receipt printer not found or inactive", "receipt", ct);
        }

        var fallback = await db.Printers.AsNoTracking()
            .Where(x => x.IsActive)
            .OrderBy(x => x.CreatedAt)
            .FirstOrDefaultAsync(ct);

        if (fallback is null)
        {
            throw new InvalidOperationException("Receipt printer not found or inactive");
        }

        EnsureNetworkPrinter(fallback, "receipt");
        return fallback;
    }

    private async Task<PosPrinter> ResolveInvoicePrinter(Guid? printerId, CancellationToken ct)
    {
        if (printerId.HasValue)
        {
            return await ResolveActiveNetworkPrinter(printerId.Value, "Invoice printer not found or inactive", "invoice", ct);
        }

        var defaultInvoicePrinterId = await GetSettingGuid(PosSettingKeys.DefaultInvoicePrinterId, ct);
        if (defaultInvoicePrinterId.HasValue)
        {
            return await ResolveActiveNetworkPrinter(defaultInvoicePrinterId.Value, "Invoice printer not found or inactive", "invoice", ct);
        }

        var defaultReceiptPrinterId = await GetSettingGuid(PosSettingKeys.DefaultReceiptPrinterId, ct);
        if (defaultReceiptPrinterId.HasValue)
        {
            return await ResolveActiveNetworkPrinter(defaultReceiptPrinterId.Value, "Invoice printer not found or inactive", "invoice", ct);
        }

        var fallback = await db.Printers.AsNoTracking()
            .Where(x => x.IsActive)
            .OrderBy(x => x.CreatedAt)
            .FirstOrDefaultAsync(ct);

        if (fallback is null)
        {
            throw new InvalidOperationException("Invoice printer not found or inactive");
        }

        EnsureNetworkPrinter(fallback, "invoice");
        return fallback;
    }

    private async Task<PosPrinter> ResolveActiveNetworkPrinter(Guid printerId, string notFoundMessage, string kind, CancellationToken ct)
    {
        var printer = await db.Printers.AsNoTracking()
            .FirstOrDefaultAsync(x => x.Id == printerId && x.IsActive, ct);

        if (printer is null)
        {
            throw new InvalidOperationException(notFoundMessage);
        }

        EnsureNetworkPrinter(printer, kind);
        return printer;
    }

    private static void EnsureNetworkPrinter(PosPrinter printer, string kind)
    {
        if (!string.Equals(printer.Type, "network", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Unsupported printer type for {kind}: {printer.Type}");
        }

        if (string.IsNullOrWhiteSpace(printer.Address))
        {
            throw new InvalidOperationException("Missing printer address");
        }
    }

    private async Task<Guid?> GetSettingGuid(string key, CancellationToken ct)
    {
        var value = await db.AppSettings.AsNoTracking()
            .Where(x => x.Key == key)
            .Select(x => x.Value)
            .FirstOrDefaultAsync(ct);

        return Guid.TryParse(value, out var parsed) ? parsed : (Guid?)null;
    }

    private static bool IsArabicLanguage(string? language)
        => !string.IsNullOrWhiteSpace(language) &&
           language.Trim().StartsWith("ar", StringComparison.OrdinalIgnoreCase);

    private static byte[] WrapEscPosText(string text)
    {
        var body = Encoding.UTF8.GetBytes(text);
        var bytes = new List<byte>(body.Length + 6)
        {
            0x1B, 0x40, // initialize
        };
        bytes.AddRange(body);
        bytes.Add(0x0A);
        bytes.Add(0x0A);
        bytes.Add(0x1D);
        bytes.Add(0x56);
        bytes.Add(0x00); // cut
        return bytes.ToArray();
    }

    private static async Task SendToNetworkPrinter(string address, byte[] payload, CancellationToken ct)
    {
        ParsePrinterAddress(address, out var host, out var port);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct);
        linked.CancelAfter(TimeSpan.FromSeconds(5));

        using var client = new TcpClient();
        await client.ConnectAsync(host, port, linked.Token);
        await using var stream = client.GetStream();
        await stream.WriteAsync(payload, linked.Token);
        await stream.FlushAsync(linked.Token);
    }

    private static void ParsePrinterAddress(string address, out string host, out int port)
    {
        var raw = address.Trim();
        if (string.IsNullOrWhiteSpace(raw))
        {
            throw new InvalidOperationException("Missing printer address");
        }

        host = raw;
        port = 9100;

        var idx = raw.LastIndexOf(':');
        if (idx > 0 && idx < raw.Length - 1 && int.TryParse(raw[(idx + 1)..], out var parsedPort))
        {
            host = raw[..idx];
            port = parsedPort;
        }

        if (string.IsNullOrWhiteSpace(host))
        {
            throw new InvalidOperationException("Invalid printer address");
        }
    }

    private static double ToDouble(object? value)
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
            string text when double.TryParse(text, out var parsed) => parsed,
            _ => 0,
        };
    }
}
