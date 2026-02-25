using BCrypt.Net;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using ResPosBackend.Data;
using ResPosBackend.Infrastructure;
using ResPosBackend.Models;
using System.Text.Json;

namespace ResPosBackend.Services;

public sealed class AdminService(PosDbContext db, IHttpClientFactory httpClientFactory)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private sealed record CurrencyEntry(string Code, string Name, string Symbol, List<decimal> Denominations);

    private sealed record CurrencySettingsSnapshot(string DefaultCurrencyCode, List<CurrencyEntry> Currencies);

    private sealed record CashierExpenseSettingsSnapshot(bool EnabledForCashier, decimal? CapAmount);

    private sealed record InvoiceTemplateSettingsSnapshot(
        string BusinessName,
        string? BusinessTagline,
        string? BusinessAddress,
        string? BusinessPhone,
        string? BusinessTaxNumber,
        string? HeaderNote,
        string? FooterNote,
        string InvoiceTitleEn,
        string InvoiceTitleAr,
        string ReceiptTitleEn,
        string ReceiptTitleAr,
        string PrimaryColorHex,
        string AccentColorHex,
        string LayoutVariant,
        bool ShowLogo,
        bool ShowPaymentsSection);

    private sealed class CurrencyEntryPayload
    {
        public string? Code { get; set; }
        public string? Name { get; set; }
        public string? Symbol { get; set; }
        public List<decimal>? Denominations { get; set; }
    }

    private sealed class InvoiceTemplatePayload
    {
        public string? BusinessName { get; set; }
        public string? BusinessTagline { get; set; }
        public string? BusinessAddress { get; set; }
        public string? BusinessPhone { get; set; }
        public string? BusinessTaxNumber { get; set; }
        public string? HeaderNote { get; set; }
        public string? FooterNote { get; set; }
        public string? InvoiceTitleEn { get; set; }
        public string? InvoiceTitleAr { get; set; }
        public string? ReceiptTitleEn { get; set; }
        public string? ReceiptTitleAr { get; set; }
        public string? PrimaryColorHex { get; set; }
        public string? AccentColorHex { get; set; }
        public string? LayoutVariant { get; set; }
        public bool? ShowLogo { get; set; }
        public bool? ShowPaymentsSection { get; set; }
    }

    public async Task<object> GetPrinterSettings(CancellationToken ct)
    {
        var rows = await db.AppSettings
            .AsNoTracking()
            .Where(x =>
                x.Key == PosSettingKeys.DefaultReceiptPrinterId ||
                x.Key == PosSettingKeys.DefaultInvoicePrinterId ||
                x.Key == PosSettingKeys.DefaultCashierDocumentsPrinterId)
            .ToListAsync(ct);

        var receipt = rows.FirstOrDefault(x => x.Key == PosSettingKeys.DefaultReceiptPrinterId)?.Value;
        var invoice = rows.FirstOrDefault(x => x.Key == PosSettingKeys.DefaultInvoicePrinterId)?.Value;
        var cashierDocuments = rows.FirstOrDefault(x => x.Key == PosSettingKeys.DefaultCashierDocumentsPrinterId)?.Value;

        var receiptPrinterId = Guid.TryParse(receipt, out var receiptId) ? receiptId : (Guid?)null;
        var invoicePrinterId = Guid.TryParse(invoice, out var invoiceId) ? invoiceId : (Guid?)null;
        var cashierDocumentsPrinterId = Guid.TryParse(cashierDocuments, out var cashierDocsId)
            ? cashierDocsId
            : (Guid?)null;

        if (!cashierDocumentsPrinterId.HasValue &&
            receiptPrinterId.HasValue &&
            invoicePrinterId.HasValue &&
            receiptPrinterId.Value == invoicePrinterId.Value)
        {
            cashierDocumentsPrinterId = receiptPrinterId.Value;
        }

        return new
        {
            receiptPrinterId,
            invoicePrinterId,
            cashierDocumentsPrinterId,
        };
    }

    public async Task<object> SetPrinterSettings(Guid? receiptPrinterId, Guid? invoicePrinterId, Guid? cashierDocumentsPrinterId, CancellationToken ct)
    {
        async Task EnsureNetworkPrinterExists(Guid printerId, string errorCode)
        {
            var exists = await db.Printers.AsNoTracking()
                .AnyAsync(x =>
                    x.Id == printerId &&
                    x.IsActive &&
                    x.Type != null &&
                    x.Type.ToLower() == "network" &&
                    x.Address != null &&
                    x.Address != "",
                    ct);
            if (!exists)
            {
                throw new InvalidOperationException(errorCode);
            }
        }

        async Task EnsureActivePrinterExists(Guid printerId, string errorCode)
        {
            var exists = await db.Printers.AsNoTracking()
                .AnyAsync(x => x.Id == printerId && x.IsActive, ct);
            if (!exists)
            {
                throw new InvalidOperationException(errorCode);
            }
        }

        if (cashierDocumentsPrinterId.HasValue)
        {
            await EnsureActivePrinterExists(cashierDocumentsPrinterId.Value, "CASHIER_DOCS_PRINTER_NOT_FOUND");
        }

        if (receiptPrinterId.HasValue)
        {
            await EnsureNetworkPrinterExists(receiptPrinterId.Value, "RECEIPT_PRINTER_NOT_FOUND");
        }

        if (invoicePrinterId.HasValue)
        {
            await EnsureNetworkPrinterExists(invoicePrinterId.Value, "INVOICE_PRINTER_NOT_FOUND");
        }

        var effectiveCashierDocumentsPrinterId = cashierDocumentsPrinterId;
        if (!effectiveCashierDocumentsPrinterId.HasValue &&
            receiptPrinterId.HasValue &&
            invoicePrinterId.HasValue &&
            receiptPrinterId.Value == invoicePrinterId.Value)
        {
            effectiveCashierDocumentsPrinterId = receiptPrinterId.Value;
        }

        await UpsertSetting(PosSettingKeys.DefaultReceiptPrinterId, receiptPrinterId?.ToString(), ct);
        await UpsertSetting(PosSettingKeys.DefaultInvoicePrinterId, invoicePrinterId?.ToString(), ct);
        await UpsertSetting(PosSettingKeys.DefaultCashierDocumentsPrinterId, effectiveCashierDocumentsPrinterId?.ToString(), ct);
        await db.SaveChangesAsync(ct);

        return new
        {
            receiptPrinterId,
            invoicePrinterId,
            cashierDocumentsPrinterId = effectiveCashierDocumentsPrinterId,
        };
    }

    public async Task<object> GetCashierExpenseSettings(CancellationToken ct)
    {
        var snapshot = await LoadCashierExpenseSettings(ct);
        return ToCashierExpenseSettingsJson(snapshot);
    }

    public async Task<object> SetCashierExpenseSettings(bool? enabledForCashier, double? capAmount, CancellationToken ct)
    {
        var snapshot = await LoadCashierExpenseSettings(ct);
        var nextEnabled = enabledForCashier ?? snapshot.EnabledForCashier;
        decimal? nextCapAmount = snapshot.CapAmount;

        if (capAmount.HasValue)
        {
            if (capAmount.Value <= 0)
            {
                throw new InvalidOperationException("CASHIER_EXPENSE_CAP_INVALID");
            }

            nextCapAmount = decimal.Round((decimal)capAmount.Value, 2);
        }
        else if (enabledForCashier.HasValue && !enabledForCashier.Value)
        {
            // Keep disabled mode permissive: clear cap to avoid stale validations.
            nextCapAmount = null;
        }

        await UpsertSetting(
            PosSettingKeys.CashierExpensesEnabled,
            nextEnabled ? "true" : "false",
            ct);
        await UpsertSetting(
            PosSettingKeys.CashierExpensesCapAmount,
            nextCapAmount?.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ct);
        await db.SaveChangesAsync(ct);

        return ToCashierExpenseSettingsJson(
            new CashierExpenseSettingsSnapshot(
                EnabledForCashier: nextEnabled,
                CapAmount: nextCapAmount));
    }

    public async Task<object> GetCurrencySettings(CancellationToken ct)
    {
        var snapshot = await LoadCurrencySettings(ct);
        return ToCurrencySettingsJson(snapshot);
    }

    public async Task<object> SetCurrencySettings(string? defaultCurrencyCode, JsonElement currenciesElement, CancellationToken ct)
    {
        var currencies = ParseCurrencies(currenciesElement);
        if (currencies.Count == 0)
        {
            throw new InvalidOperationException("CURRENCIES_REQUIRED");
        }

        var normalizedDefaultCurrency = NormalizeCurrencyCode(defaultCurrencyCode);
        if (normalizedDefaultCurrency is null)
        {
            throw new InvalidOperationException("DEFAULT_CURRENCY_REQUIRED");
        }

        if (!currencies.Any(x => x.Code == normalizedDefaultCurrency))
        {
            throw new InvalidOperationException("DEFAULT_CURRENCY_NOT_FOUND");
        }

        await UpsertSetting(PosSettingKeys.DefaultCurrencyCode, normalizedDefaultCurrency, ct);
        await UpsertSetting(
            PosSettingKeys.CurrenciesConfig,
            JsonSerializer.Serialize(
                currencies.Select(x => new CurrencyEntryPayload
                {
                    Code = x.Code,
                    Name = x.Name,
                    Symbol = x.Symbol,
                    Denominations = x.Denominations,
                }),
                JsonOptions),
            ct);
        await db.SaveChangesAsync(ct);

        return ToCurrencySettingsJson(new CurrencySettingsSnapshot(normalizedDefaultCurrency, currencies));
    }

    public async Task<object> GetCurrencyRates(CancellationToken ct)
    {
        var snapshot = await LoadCurrencySettings(ct);
        var baseCode = snapshot.DefaultCurrencyCode;

        using var client = httpClientFactory.CreateClient();
        using var req = new HttpRequestMessage(HttpMethod.Get, $"https://open.er-api.com/v6/latest/{baseCode}");
        using var response = await client.SendAsync(req, ct);

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException("FX_PROVIDER_FAILED");
        }

        await using var stream = await response.Content.ReadAsStreamAsync(ct);
        using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
        var root = doc.RootElement;

        var result = root.TryGetProperty("result", out var resultEl) ? resultEl.GetString() : null;
        if (!string.Equals(result, "success", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("FX_PROVIDER_FAILED");
        }

        if (!root.TryGetProperty("rates", out var ratesEl) || ratesEl.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException("FX_PROVIDER_FAILED");
        }

        var rates = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
        foreach (var rateRow in ratesEl.EnumerateObject())
        {
            decimal value;
            if (rateRow.Value.ValueKind == JsonValueKind.Number && rateRow.Value.TryGetDecimal(out var d))
            {
                value = d;
            }
            else if (rateRow.Value.ValueKind == JsonValueKind.Number && rateRow.Value.TryGetDouble(out var n))
            {
                value = Convert.ToDecimal(n);
            }
            else
            {
                continue;
            }

            rates[rateRow.Name.ToUpperInvariant()] = value;
        }

        rates[baseCode] = 1m;

        return new
        {
            baseCurrencyCode = baseCode,
            source = "open.er-api.com",
            fetchedAt = DateTime.UtcNow,
            rates,
        };
    }

    public async Task<object> GetInvoiceTemplateSettings(CancellationToken ct)
    {
        var snapshot = await LoadInvoiceTemplateSettings(ct);
        return ToInvoiceTemplateSettingsJson(snapshot);
    }

    public async Task<object> SetInvoiceTemplateSettings(
        string? businessName,
        string? businessTagline,
        string? businessAddress,
        string? businessPhone,
        string? businessTaxNumber,
        string? headerNote,
        string? footerNote,
        string? invoiceTitleEn,
        string? invoiceTitleAr,
        string? receiptTitleEn,
        string? receiptTitleAr,
        string? primaryColorHex,
        string? accentColorHex,
        string? layoutVariant,
        bool? showLogo,
        bool? showPaymentsSection,
        CancellationToken ct)
    {
        var snapshot = NormalizeInvoiceTemplate(new InvoiceTemplatePayload
        {
            BusinessName = businessName,
            BusinessTagline = businessTagline,
            BusinessAddress = businessAddress,
            BusinessPhone = businessPhone,
            BusinessTaxNumber = businessTaxNumber,
            HeaderNote = headerNote,
            FooterNote = footerNote,
            InvoiceTitleEn = invoiceTitleEn,
            InvoiceTitleAr = invoiceTitleAr,
            ReceiptTitleEn = receiptTitleEn,
            ReceiptTitleAr = receiptTitleAr,
            PrimaryColorHex = primaryColorHex,
            AccentColorHex = accentColorHex,
            LayoutVariant = layoutVariant,
            ShowLogo = showLogo,
            ShowPaymentsSection = showPaymentsSection,
        });

        await UpsertSetting(
            PosSettingKeys.InvoiceTemplateConfig,
            JsonSerializer.Serialize(
                new InvoiceTemplatePayload
                {
                    BusinessName = snapshot.BusinessName,
                    BusinessTagline = snapshot.BusinessTagline,
                    BusinessAddress = snapshot.BusinessAddress,
                    BusinessPhone = snapshot.BusinessPhone,
                    BusinessTaxNumber = snapshot.BusinessTaxNumber,
                    HeaderNote = snapshot.HeaderNote,
                    FooterNote = snapshot.FooterNote,
                    InvoiceTitleEn = snapshot.InvoiceTitleEn,
                    InvoiceTitleAr = snapshot.InvoiceTitleAr,
                    ReceiptTitleEn = snapshot.ReceiptTitleEn,
                    ReceiptTitleAr = snapshot.ReceiptTitleAr,
                    PrimaryColorHex = snapshot.PrimaryColorHex,
                    AccentColorHex = snapshot.AccentColorHex,
                    LayoutVariant = snapshot.LayoutVariant,
                    ShowLogo = snapshot.ShowLogo,
                    ShowPaymentsSection = snapshot.ShowPaymentsSection,
                },
                JsonOptions),
            ct);
        await db.SaveChangesAsync(ct);

        return ToInvoiceTemplateSettingsJson(snapshot);
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

    private async Task<InvoiceTemplateSettingsSnapshot> LoadInvoiceTemplateSettings(CancellationToken ct)
    {
        var raw = await db.AppSettings
            .AsNoTracking()
            .Where(x => x.Key == PosSettingKeys.InvoiceTemplateConfig)
            .Select(x => x.Value)
            .FirstOrDefaultAsync(ct);

        if (string.IsNullOrWhiteSpace(raw))
        {
            return DefaultInvoiceTemplateSettings();
        }

        try
        {
            var payload = JsonSerializer.Deserialize<InvoiceTemplatePayload>(raw, JsonOptions);
            return NormalizeInvoiceTemplate(payload);
        }
        catch
        {
            return DefaultInvoiceTemplateSettings();
        }
    }

    private static object ToInvoiceTemplateSettingsJson(InvoiceTemplateSettingsSnapshot snapshot)
    {
        return new
        {
            businessName = snapshot.BusinessName,
            businessTagline = snapshot.BusinessTagline,
            businessAddress = snapshot.BusinessAddress,
            businessPhone = snapshot.BusinessPhone,
            businessTaxNumber = snapshot.BusinessTaxNumber,
            headerNote = snapshot.HeaderNote,
            footerNote = snapshot.FooterNote,
            invoiceTitleEn = snapshot.InvoiceTitleEn,
            invoiceTitleAr = snapshot.InvoiceTitleAr,
            receiptTitleEn = snapshot.ReceiptTitleEn,
            receiptTitleAr = snapshot.ReceiptTitleAr,
            primaryColorHex = snapshot.PrimaryColorHex,
            accentColorHex = snapshot.AccentColorHex,
            layoutVariant = snapshot.LayoutVariant,
            showLogo = snapshot.ShowLogo,
            showPaymentsSection = snapshot.ShowPaymentsSection,
        };
    }

    private static InvoiceTemplateSettingsSnapshot DefaultInvoiceTemplateSettings() => new(
        BusinessName: "Restaurant POS",
        BusinessTagline: "Fresh food and fast service",
        BusinessAddress: null,
        BusinessPhone: null,
        BusinessTaxNumber: null,
        HeaderNote: null,
        FooterNote: "Thank you for your visit",
        InvoiceTitleEn: "Invoice",
        InvoiceTitleAr: "فاتورة",
        ReceiptTitleEn: "Receipt",
        ReceiptTitleAr: "إيصال",
        PrimaryColorHex: "#0F766E",
        AccentColorHex: "#14B8A6",
        LayoutVariant: "premium",
        ShowLogo: true,
        ShowPaymentsSection: true);

    private static InvoiceTemplateSettingsSnapshot NormalizeInvoiceTemplate(InvoiceTemplatePayload? payload)
    {
        var defaults = DefaultInvoiceTemplateSettings();
        if (payload is null)
        {
            return defaults;
        }

        return new InvoiceTemplateSettingsSnapshot(
            BusinessName: NormalizeRequiredText(payload.BusinessName, defaults.BusinessName),
            BusinessTagline: NormalizeOptionalText(payload.BusinessTagline),
            BusinessAddress: NormalizeOptionalText(payload.BusinessAddress),
            BusinessPhone: NormalizeOptionalText(payload.BusinessPhone),
            BusinessTaxNumber: NormalizeOptionalText(payload.BusinessTaxNumber),
            HeaderNote: NormalizeOptionalText(payload.HeaderNote),
            FooterNote: NormalizeOptionalText(payload.FooterNote),
            InvoiceTitleEn: NormalizeRequiredText(payload.InvoiceTitleEn, defaults.InvoiceTitleEn),
            InvoiceTitleAr: NormalizeRequiredText(payload.InvoiceTitleAr, defaults.InvoiceTitleAr),
            ReceiptTitleEn: NormalizeRequiredText(payload.ReceiptTitleEn, defaults.ReceiptTitleEn),
            ReceiptTitleAr: NormalizeRequiredText(payload.ReceiptTitleAr, defaults.ReceiptTitleAr),
            PrimaryColorHex: NormalizeHexColor(payload.PrimaryColorHex, defaults.PrimaryColorHex),
            AccentColorHex: NormalizeHexColor(payload.AccentColorHex, defaults.AccentColorHex),
            LayoutVariant: NormalizeLayoutVariant(payload.LayoutVariant, defaults.LayoutVariant),
            ShowLogo: payload.ShowLogo ?? defaults.ShowLogo,
            ShowPaymentsSection: payload.ShowPaymentsSection ?? defaults.ShowPaymentsSection);
    }

    private static string NormalizeLayoutVariant(string? raw, string fallback)
    {
        var value = raw?.Trim().ToLowerInvariant();
        return value switch
        {
            "compact" => "compact",
            "premium" => "premium",
            _ => fallback,
        };
    }

    private static string NormalizeRequiredText(string? raw, string fallback)
    {
        var text = raw?.Trim();
        return string.IsNullOrWhiteSpace(text) ? fallback : text;
    }

    private static string? NormalizeOptionalText(string? raw)
    {
        var text = raw?.Trim();
        return string.IsNullOrWhiteSpace(text) ? null : text;
    }

    private static string NormalizeHexColor(string? raw, string fallback)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        var value = raw.Trim();
        if (!value.StartsWith("#", StringComparison.Ordinal))
        {
            value = $"#{value}";
        }

        return System.Text.RegularExpressions.Regex.IsMatch(value, "^#[0-9a-fA-F]{6}$")
            ? value.ToUpperInvariant()
            : fallback;
    }

    private async Task<CashierExpenseSettingsSnapshot> LoadCashierExpenseSettings(CancellationToken ct)
    {
        var rows = await db.AppSettings
            .AsNoTracking()
            .Where(x =>
                x.Key == PosSettingKeys.CashierExpensesEnabled ||
                x.Key == PosSettingKeys.CashierExpensesCapAmount)
            .ToListAsync(ct);

        var enabledRaw = rows.FirstOrDefault(x => x.Key == PosSettingKeys.CashierExpensesEnabled)?.Value;
        var capRaw = rows.FirstOrDefault(x => x.Key == PosSettingKeys.CashierExpensesCapAmount)?.Value;

        var enabled = ParseSettingBool(enabledRaw) ?? true;
        var capAmount = ParseSettingDecimal(capRaw);
        if (capAmount.HasValue && capAmount <= 0)
        {
            capAmount = null;
        }

        return new CashierExpenseSettingsSnapshot(
            EnabledForCashier: enabled,
            CapAmount: capAmount);
    }

    private static object ToCashierExpenseSettingsJson(CashierExpenseSettingsSnapshot snapshot)
    {
        return new
        {
            enabledForCashier = snapshot.EnabledForCashier,
            capAmount = snapshot.CapAmount is null ? null : (double?)snapshot.CapAmount.Value,
        };
    }

    private async Task<CurrencySettingsSnapshot> LoadCurrencySettings(CancellationToken ct)
    {
        var rows = await db.AppSettings
            .AsNoTracking()
            .Where(x =>
                x.Key == PosSettingKeys.DefaultCurrencyCode ||
                x.Key == PosSettingKeys.CurrenciesConfig)
            .ToListAsync(ct);

        var defaultCurrencyCode = NormalizeCurrencyCode(
            rows.FirstOrDefault(x => x.Key == PosSettingKeys.DefaultCurrencyCode)?.Value);
        var currenciesJson = rows.FirstOrDefault(x => x.Key == PosSettingKeys.CurrenciesConfig)?.Value;
        var currencies = ParseStoredCurrencies(currenciesJson);

        if (currencies.Count == 0)
        {
            currencies =
            [
                new CurrencyEntry(
                    "ILS",
                    "Israeli Shekel",
                    "NIS",
                    [200m, 100m, 50m, 20m, 10m, 5m, 2m, 1m, 0.5m, 0.1m]),
            ];
        }

        if (defaultCurrencyCode is null || !currencies.Any(x => x.Code == defaultCurrencyCode))
        {
            defaultCurrencyCode = currencies[0].Code;
        }

        return new CurrencySettingsSnapshot(defaultCurrencyCode, currencies);
    }

    private static object ToCurrencySettingsJson(CurrencySettingsSnapshot snapshot)
    {
        return new
        {
            defaultCurrencyCode = snapshot.DefaultCurrencyCode,
            currencies = snapshot.Currencies.Select(x => new
            {
                code = x.Code,
                name = x.Name,
                symbol = x.Symbol,
                denominations = x.Denominations,
            }).ToList(),
        };
    }

    private static List<CurrencyEntry> ParseStoredCurrencies(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return [];
        }

        try
        {
            var payload = JsonSerializer.Deserialize<List<CurrencyEntryPayload>>(raw, JsonOptions) ?? [];
            return NormalizeCurrencyEntries(payload.Select(x => new CurrencyEntry(
                x.Code ?? string.Empty,
                x.Name?.Trim() ?? string.Empty,
                x.Symbol?.Trim() ?? string.Empty,
                x.Denominations ?? [])));
        }
        catch
        {
            return [];
        }
    }

    private static List<CurrencyEntry> ParseCurrencies(JsonElement currenciesElement)
    {
        if (currenciesElement.ValueKind != JsonValueKind.Array)
        {
            return [];
        }

        var list = new List<CurrencyEntry>();
        foreach (var row in currenciesElement.EnumerateArray())
        {
            if (row.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            var code = row.TryGetProperty("code", out var codeEl) ? codeEl.GetString() : null;
            var name = row.TryGetProperty("name", out var nameEl) ? nameEl.GetString() : null;
            var symbol = row.TryGetProperty("symbol", out var symbolEl) ? symbolEl.GetString() : null;
            var denominations = row.TryGetProperty("denominations", out var denomsEl)
                ? ParseDenominations(denomsEl)
                : [];

            list.Add(new CurrencyEntry(
                code ?? string.Empty,
                name?.Trim() ?? string.Empty,
                symbol?.Trim() ?? string.Empty,
                denominations));
        }

        return NormalizeCurrencyEntries(list);
    }

    private static List<CurrencyEntry> NormalizeCurrencyEntries(IEnumerable<CurrencyEntry> entries)
    {
        var result = new List<CurrencyEntry>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var entry in entries)
        {
            var code = NormalizeCurrencyCode(entry.Code);
            if (code is null || !seen.Add(code))
            {
                continue;
            }

            var name = string.IsNullOrWhiteSpace(entry.Name) ? code : entry.Name.Trim();
            var symbol = string.IsNullOrWhiteSpace(entry.Symbol) ? code : entry.Symbol.Trim();

            var denominations = entry.Denominations
                .Where(x => x > 0)
                .Select(x => decimal.Round(x, 4))
                .Distinct()
                .OrderByDescending(x => x)
                .ToList();

            if (denominations.Count == 0)
            {
                denominations = [1m];
            }

            result.Add(new CurrencyEntry(code, name, symbol, denominations));
        }

        return result;
    }

    private static List<decimal> ParseDenominations(JsonElement el)
    {
        if (el.ValueKind != JsonValueKind.Array)
        {
            return [];
        }

        var values = new List<decimal>();
        foreach (var row in el.EnumerateArray())
        {
            if (row.ValueKind == JsonValueKind.Number && row.TryGetDecimal(out var d))
            {
                values.Add(d);
                continue;
            }

            if (row.ValueKind == JsonValueKind.Number && row.TryGetDouble(out var n))
            {
                values.Add(Convert.ToDecimal(n));
                continue;
            }

            if (row.ValueKind == JsonValueKind.String &&
                decimal.TryParse(row.GetString(), out var parsed))
            {
                values.Add(parsed);
            }
        }

        return values;
    }

    private static string? NormalizeCurrencyCode(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        var code = raw.Trim().ToUpperInvariant();
        if (code.Length < 2 || code.Length > 8 || code.Any(x => !char.IsLetter(x)))
        {
            return null;
        }

        return code;
    }

    private static bool? ParseSettingBool(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        if (bool.TryParse(raw.Trim(), out var parsed))
        {
            return parsed;
        }

        if (string.Equals(raw.Trim(), "1", StringComparison.Ordinal))
        {
            return true;
        }

        if (string.Equals(raw.Trim(), "0", StringComparison.Ordinal))
        {
            return false;
        }

        return null;
    }

    private static decimal? ParseSettingDecimal(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        if (decimal.TryParse(
                raw.Trim(),
                System.Globalization.NumberStyles.Number,
                System.Globalization.CultureInfo.InvariantCulture,
                out var parsed))
        {
            return parsed;
        }

        return null;
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
