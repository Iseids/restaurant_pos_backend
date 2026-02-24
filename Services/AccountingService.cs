using Microsoft.EntityFrameworkCore;
using Npgsql;
using ResPosBackend.Data;
using ResPosBackend.Models;

namespace ResPosBackend.Services;

public sealed class AccountingService(PosDbContext db)
{
    public async Task<List<object>> ListAccounts(CancellationToken ct)
    {
        var rows = await (
            from a in db.Accounts.AsNoTracking()
            join t in db.AccountTransactions.AsNoTracking() on a.Id equals t.AccountId into txs
            orderby a.CreatedAt
            select new
            {
                a.Id,
                a.Name,
                a.Type,
                a.Currency,
                a.IsActive,
                a.CreatedAt,
                Balance = txs
                    .Sum(x => x.Direction == "in"
                        ? (decimal?)x.Amount
                        : -(decimal?)x.Amount) ?? 0m,
            })
            .ToListAsync(ct);

        return rows.Select(x => (object)new
        {
            id = x.Id,
            name = x.Name,
            type = x.Type,
            currency = x.Currency,
            isActive = x.IsActive,
            createdAt = x.CreatedAt,
            balance = (double)x.Balance,
        }).ToList();
    }

    public async Task<object> CreateAccount(string name, string type, string currency, CancellationToken ct)
    {
        var entity = new PosAccount
        {
            Id = Guid.NewGuid(),
            Name = name,
            Type = string.IsNullOrWhiteSpace(type) ? "cash" : type,
            Currency = string.IsNullOrWhiteSpace(currency) ? "ILS" : currency,
            IsActive = true,
            CreatedAt = DateTime.UtcNow,
        };

        db.Accounts.Add(entity);
        await SaveWithConflictHandling(ct);

        return new
        {
            id = entity.Id,
            name = entity.Name,
            type = entity.Type,
            currency = entity.Currency,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
            balance = 0d,
        };
    }

    public async Task<object?> UpdateAccount(Guid id, string? name, string? type, string? currency, bool? isActive, CancellationToken ct)
    {
        var entity = await db.Accounts.FirstOrDefaultAsync(x => x.Id == id, ct);
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

        if (!string.IsNullOrWhiteSpace(currency))
        {
            entity.Currency = currency.Trim();
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
            currency = entity.Currency,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
            balance = 0d,
        };
    }

    public async Task<List<object>> ListPaymentMethodAccounts(CancellationToken ct)
    {
        var rows = await (
            from map in db.PaymentMethodAccounts.AsNoTracking()
            join acc in db.Accounts.AsNoTracking() on map.AccountId equals acc.Id
            orderby map.Method
            select new
            {
                map.Method,
                map.AccountId,
                AccountName = acc.Name,
                AccountType = acc.Type,
                acc.Currency,
            }).ToListAsync(ct);

        return rows.Select(x => (object)new
        {
            method = x.Method,
            accountId = x.AccountId,
            accountName = x.AccountName,
            accountType = x.AccountType,
            currency = x.Currency,
        }).ToList();
    }

    public async Task<object> SetPaymentMethodAccount(string method, Guid accountId, CancellationToken ct)
    {
        var existing = await db.PaymentMethodAccounts.FirstOrDefaultAsync(x => x.Method == method, ct);
        if (existing is null)
        {
            db.PaymentMethodAccounts.Add(new PosPaymentMethodAccount
            {
                Method = method,
                AccountId = accountId,
            });
        }
        else
        {
            existing.AccountId = accountId;
        }

        await db.SaveChangesAsync(ct);

        var row = await (
            from map in db.PaymentMethodAccounts.AsNoTracking()
            join acc in db.Accounts.AsNoTracking() on map.AccountId equals acc.Id
            where map.Method == method
            select new
            {
                map.Method,
                map.AccountId,
                AccountName = acc.Name,
                AccountType = acc.Type,
                acc.Currency,
            }).FirstAsync(ct);

        return new
        {
            method = row.Method,
            accountId = row.AccountId,
            accountName = row.AccountName,
            accountType = row.AccountType,
            currency = row.Currency,
        };
    }

    public async Task<List<object>> ListSuppliers(CancellationToken ct)
    {
        var items = await db.Suppliers.AsNoTracking().OrderBy(x => x.Name).ToListAsync(ct);
        return items.Select(x => (object)new
        {
            id = x.Id,
            name = x.Name,
            phone = x.Phone,
            email = x.Email,
            note = x.Note,
            isActive = x.IsActive,
            createdAt = x.CreatedAt,
        }).ToList();
    }

    public async Task<object> CreateSupplier(string name, string? phone, string? email, string? note, CancellationToken ct)
    {
        var entity = new PosSupplier
        {
            Id = Guid.NewGuid(),
            Name = name,
            Phone = string.IsNullOrWhiteSpace(phone) ? null : phone.Trim(),
            Email = string.IsNullOrWhiteSpace(email) ? null : email.Trim(),
            Note = string.IsNullOrWhiteSpace(note) ? null : note.Trim(),
            IsActive = true,
            CreatedAt = DateTime.UtcNow,
        };

        db.Suppliers.Add(entity);
        await SaveWithConflictHandling(ct);

        return new
        {
            id = entity.Id,
            name = entity.Name,
            phone = entity.Phone,
            email = entity.Email,
            note = entity.Note,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
        };
    }

    public async Task<object?> UpdateSupplier(
        Guid id,
        string? name,
        string? phone,
        string? email,
        string? note,
        bool? isActive,
        CancellationToken ct)
    {
        var entity = await db.Suppliers.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(name))
        {
            entity.Name = name.Trim();
        }

        if (!string.IsNullOrWhiteSpace(phone))
        {
            entity.Phone = phone.Trim();
        }

        if (!string.IsNullOrWhiteSpace(email))
        {
            entity.Email = email.Trim();
        }

        if (!string.IsNullOrWhiteSpace(note))
        {
            entity.Note = note.Trim();
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
            phone = entity.Phone,
            email = entity.Email,
            note = entity.Note,
            isActive = entity.IsActive,
            createdAt = entity.CreatedAt,
        };
    }

    public async Task<object> CreateReceipt(
        Guid createdBy,
        double amount,
        string method,
        Guid accountId,
        string? source,
        Guid? supplierId,
        DateOnly? receiptDate,
        string? note,
        CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var now = DateTime.UtcNow;
        var entity = new PosReceipt
        {
            Id = Guid.NewGuid(),
            ReceiptDate = receiptDate ?? DateOnly.FromDateTime(now),
            Source = string.IsNullOrWhiteSpace(source) ? null : source.Trim(),
            SupplierId = supplierId,
            Amount = (decimal)amount,
            Method = method,
            AccountId = accountId,
            Note = string.IsNullOrWhiteSpace(note) ? null : note.Trim(),
            CreatedBy = createdBy,
            CreatedAt = now,
        };

        db.Receipts.Add(entity);
        await db.SaveChangesAsync(ct);

        db.AccountTransactions.Add(new PosAccountTransaction
        {
            Id = Guid.NewGuid(),
            AccountId = accountId,
            Direction = "in",
            Amount = (decimal)amount,
            SourceType = "manual_receipt",
            SourceId = entity.Id,
            Note = entity.Note,
            CreatedBy = createdBy,
            CreatedAt = now,
        });

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);

        return new
        {
            id = entity.Id,
            receiptDate = entity.ReceiptDate.ToString("yyyy-MM-dd"),
            amount = (double)entity.Amount,
            method = entity.Method,
            accountId = entity.AccountId,
            createdAt = entity.CreatedAt,
        };
    }

    public async Task<object> CreateExpense(
        Guid createdBy,
        string category,
        double amount,
        string method,
        Guid accountId,
        Guid? supplierId,
        DateOnly? expenseDate,
        string? attachmentUrl,
        string? note,
        CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var now = DateTime.UtcNow;
        var entity = new PosExpense
        {
            Id = Guid.NewGuid(),
            ExpenseDate = expenseDate ?? DateOnly.FromDateTime(now),
            Category = category,
            SupplierId = supplierId,
            Amount = (decimal)amount,
            Method = method,
            AccountId = accountId,
            AttachmentUrl = string.IsNullOrWhiteSpace(attachmentUrl) ? null : attachmentUrl.Trim(),
            Note = string.IsNullOrWhiteSpace(note) ? null : note.Trim(),
            CreatedBy = createdBy,
            CreatedAt = now,
        };

        db.Expenses.Add(entity);
        await db.SaveChangesAsync(ct);

        db.AccountTransactions.Add(new PosAccountTransaction
        {
            Id = Guid.NewGuid(),
            AccountId = accountId,
            Direction = "out",
            Amount = (decimal)amount,
            SourceType = "expense",
            SourceId = entity.Id,
            Note = entity.Note,
            CreatedBy = createdBy,
            CreatedAt = now,
        });

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);

        return new
        {
            id = entity.Id,
            expenseDate = entity.ExpenseDate.ToString("yyyy-MM-dd"),
            amount = (double)entity.Amount,
            method = entity.Method,
            accountId = entity.AccountId,
            createdAt = entity.CreatedAt,
        };
    }

    public async Task Deposit(Guid createdBy, Guid accountId, double amount, string? note, CancellationToken ct)
    {
        db.AccountTransactions.Add(new PosAccountTransaction
        {
            Id = Guid.NewGuid(),
            AccountId = accountId,
            Direction = "in",
            Amount = (decimal)amount,
            SourceType = "deposit",
            SourceId = null,
            Note = string.IsNullOrWhiteSpace(note) ? null : note.Trim(),
            CreatedBy = createdBy,
            CreatedAt = DateTime.UtcNow,
        });

        await db.SaveChangesAsync(ct);
    }

    public async Task Withdraw(Guid createdBy, Guid accountId, double amount, string? note, CancellationToken ct)
    {
        db.AccountTransactions.Add(new PosAccountTransaction
        {
            Id = Guid.NewGuid(),
            AccountId = accountId,
            Direction = "out",
            Amount = (decimal)amount,
            SourceType = "withdrawal",
            SourceId = null,
            Note = string.IsNullOrWhiteSpace(note) ? null : note.Trim(),
            CreatedBy = createdBy,
            CreatedAt = DateTime.UtcNow,
        });

        await db.SaveChangesAsync(ct);
    }

    public async Task Transfer(Guid createdBy, Guid fromAccountId, Guid toAccountId, double amount, string? note, CancellationToken ct)
    {
        if (fromAccountId == toAccountId)
        {
            throw new InvalidOperationException("TRANSFER_SAME_ACCOUNT");
        }

        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var now = DateTime.UtcNow;
        var transfer = new PosAccountTransfer
        {
            Id = Guid.NewGuid(),
            FromAccountId = fromAccountId,
            ToAccountId = toAccountId,
            Amount = (decimal)amount,
            Note = string.IsNullOrWhiteSpace(note) ? null : note.Trim(),
            CreatedBy = createdBy,
            CreatedAt = now,
        };

        db.AccountTransfers.Add(transfer);

        db.AccountTransactions.Add(new PosAccountTransaction
        {
            Id = Guid.NewGuid(),
            AccountId = fromAccountId,
            Direction = "out",
            Amount = (decimal)amount,
            SourceType = "transfer",
            SourceId = transfer.Id,
            Note = transfer.Note ?? "Transfer to account",
            CreatedBy = createdBy,
            CreatedAt = now,
        });

        db.AccountTransactions.Add(new PosAccountTransaction
        {
            Id = Guid.NewGuid(),
            AccountId = toAccountId,
            Direction = "in",
            Amount = (decimal)amount,
            SourceType = "transfer",
            SourceId = transfer.Id,
            Note = transfer.Note ?? "Transfer from account",
            CreatedBy = createdBy,
            CreatedAt = now,
        });

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
    }

    public async Task<List<object>> ListReceipts(DateOnly? start, DateOnly? end, CancellationToken ct)
    {
        var posRows = await (
            from p in db.Payments.AsNoTracking()
            join o in db.Orders.AsNoTracking() on p.OrderId equals o.Id
            join t in db.Tables.AsNoTracking() on o.TableId equals t.Id into tJoin
            from t in tJoin.DefaultIfEmpty()
            join map in db.PaymentMethodAccounts.AsNoTracking() on p.Method equals map.Method into mapJoin
            from map in mapJoin.DefaultIfEmpty()
            join a in db.Accounts.AsNoTracking() on map.AccountId equals a.Id into aJoin
            from a in aJoin.DefaultIfEmpty()
            where (!start.HasValue || o.BusinessDate >= start.Value)
                  && (!end.HasValue || o.BusinessDate <= end.Value)
            select new
            {
                Kind = "pos",
                Id = p.Id,
                Date = o.BusinessDate,
                Amount = p.Amount,
                Method = p.Method,
                Source = (string?)null,
                SupplierName = (string?)null,
                AccountName = a != null ? a.Name : null,
                OrderNo = (short?)o.OrderNo,
                IsTakeaway = (bool?)o.IsTakeaway,
                TableName = t != null ? t.Name : null,
                CreatedAt = p.CreatedAt,
            }).ToListAsync(ct);

        var manualRows = await (
            from r in db.Receipts.AsNoTracking()
            join s in db.Suppliers.AsNoTracking() on r.SupplierId equals s.Id into sJoin
            from s in sJoin.DefaultIfEmpty()
            join a in db.Accounts.AsNoTracking() on r.AccountId equals a.Id into aJoin
            from a in aJoin.DefaultIfEmpty()
            where (!start.HasValue || r.ReceiptDate >= start.Value)
                  && (!end.HasValue || r.ReceiptDate <= end.Value)
            select new
            {
                Kind = "manual",
                Id = r.Id,
                Date = r.ReceiptDate,
                Amount = r.Amount,
                Method = r.Method,
                Source = r.Source,
                SupplierName = s != null ? s.Name : null,
                AccountName = a != null ? a.Name : null,
                OrderNo = (short?)null,
                IsTakeaway = (bool?)null,
                TableName = (string?)null,
                CreatedAt = r.CreatedAt,
            }).ToListAsync(ct);

        var rows = posRows
            .Concat(manualRows)
            .OrderByDescending(x => x.CreatedAt)
            .ToList();

        return rows.Select(x => (object)new
        {
            kind = x.Kind,
            id = x.Id,
            date = x.Date.ToString("yyyy-MM-dd"),
            amount = (double)x.Amount,
            method = x.Method,
            source = x.Source,
            supplierName = x.SupplierName,
            accountName = x.AccountName,
            orderNo = x.OrderNo.HasValue ? x.OrderNo.Value.ToString("00") : null,
            isTakeaway = x.IsTakeaway,
            tableName = x.TableName,
            createdAt = x.CreatedAt,
        }).ToList();
    }

    public async Task<List<object>> ListExpenses(DateOnly? start, DateOnly? end, CancellationToken ct)
    {
        var rows = await (
            from e in db.Expenses.AsNoTracking()
            join s in db.Suppliers.AsNoTracking() on e.SupplierId equals s.Id into sJoin
            from s in sJoin.DefaultIfEmpty()
            join a in db.Accounts.AsNoTracking() on e.AccountId equals a.Id into aJoin
            from a in aJoin.DefaultIfEmpty()
            where (!start.HasValue || e.ExpenseDate >= start.Value)
                  && (!end.HasValue || e.ExpenseDate <= end.Value)
            orderby e.CreatedAt descending
            select new
            {
                e.Id,
                e.ExpenseDate,
                e.Category,
                e.Amount,
                e.Method,
                SupplierName = s != null ? s.Name : null,
                AccountName = a != null ? a.Name : null,
                e.AttachmentUrl,
                e.Note,
                e.CreatedAt,
            }).ToListAsync(ct);

        return rows.Select(x => (object)new
        {
            id = x.Id,
            date = x.ExpenseDate.ToString("yyyy-MM-dd"),
            category = x.Category,
            amount = (double)x.Amount,
            method = x.Method,
            supplierName = x.SupplierName,
            accountName = x.AccountName,
            attachmentUrl = x.AttachmentUrl,
            note = x.Note,
            createdAt = x.CreatedAt,
        }).ToList();
    }

    public async Task<List<object>> ListLedger(DateTime? start, DateTime? end, CancellationToken ct)
    {
        var transactions = await db.AccountTransactions
            .AsNoTracking()
            .Where(x => (!start.HasValue || x.CreatedAt >= start.Value)
                        && (!end.HasValue || x.CreatedAt <= end.Value))
            .OrderByDescending(x => x.CreatedAt)
            .ToListAsync(ct);

        if (transactions.Count == 0)
        {
            return [];
        }

        var accountIds = transactions.Select(x => x.AccountId).Distinct().ToList();
        var accountById = await db.Accounts
            .AsNoTracking()
            .Where(x => accountIds.Contains(x.Id))
            .ToDictionaryAsync(x => x.Id, x => x, ct);

        var orderIds = transactions
            .Where(x => x.SourceType == "pos_payment" && x.SourceId.HasValue)
            .Select(x => x.SourceId!.Value)
            .Distinct()
            .ToList();
        var receiptIds = transactions
            .Where(x => x.SourceType == "manual_receipt" && x.SourceId.HasValue)
            .Select(x => x.SourceId!.Value)
            .Distinct()
            .ToList();
        var expenseIds = transactions
            .Where(x => x.SourceType == "expense" && x.SourceId.HasValue)
            .Select(x => x.SourceId!.Value)
            .Distinct()
            .ToList();

        var orders = orderIds.Count == 0
            ? []
            : await db.Orders.AsNoTracking().Where(x => orderIds.Contains(x.Id)).ToListAsync(ct);
        var orderById = orders.ToDictionary(x => x.Id, x => x);

        var tableIds = orders
            .Where(x => x.TableId.HasValue)
            .Select(x => x.TableId!.Value)
            .Distinct()
            .ToList();
        var tableById = tableIds.Count == 0
            ? new Dictionary<Guid, PosTable>()
            : await db.Tables
                .AsNoTracking()
                .Where(x => tableIds.Contains(x.Id))
                .ToDictionaryAsync(x => x.Id, x => x, ct);

        var receipts = receiptIds.Count == 0
            ? []
            : await db.Receipts.AsNoTracking().Where(x => receiptIds.Contains(x.Id)).ToListAsync(ct);
        var receiptById = receipts.ToDictionary(x => x.Id, x => x);

        var expenses = expenseIds.Count == 0
            ? []
            : await db.Expenses.AsNoTracking().Where(x => expenseIds.Contains(x.Id)).ToListAsync(ct);
        var expenseById = expenses.ToDictionary(x => x.Id, x => x);

        var supplierIds = receipts
            .Where(x => x.SupplierId.HasValue)
            .Select(x => x.SupplierId!.Value)
            .Concat(expenses.Where(x => x.SupplierId.HasValue).Select(x => x.SupplierId!.Value))
            .Distinct()
            .ToList();
        var supplierById = supplierIds.Count == 0
            ? new Dictionary<Guid, PosSupplier>()
            : await db.Suppliers
                .AsNoTracking()
                .Where(x => supplierIds.Contains(x.Id))
                .ToDictionaryAsync(x => x.Id, x => x, ct);

        return transactions.Select(x =>
        {
            PosOrder? order = null;
            PosReceipt? receipt = null;
            PosExpense? expense = null;
            PosSupplier? supplier = null;

            if (x.SourceType == "pos_payment" && x.SourceId.HasValue)
            {
                orderById.TryGetValue(x.SourceId.Value, out order);
            }
            else if (x.SourceType == "manual_receipt" && x.SourceId.HasValue)
            {
                receiptById.TryGetValue(x.SourceId.Value, out receipt);
            }
            else if (x.SourceType == "expense" && x.SourceId.HasValue)
            {
                expenseById.TryGetValue(x.SourceId.Value, out expense);
            }

            var supplierId = receipt?.SupplierId ?? expense?.SupplierId;
            if (supplierId.HasValue)
            {
                supplierById.TryGetValue(supplierId.Value, out supplier);
            }

            string? tableName = null;
            if (order?.TableId is { } tableId && tableById.TryGetValue(tableId, out var table))
            {
                tableName = table.Name;
            }

            return (object)new
            {
                id = x.Id,
                createdAt = x.CreatedAt,
                direction = x.Direction,
                amount = (double)x.Amount,
                sourceType = x.SourceType,
                note = x.Note,
                accountName = accountById.TryGetValue(x.AccountId, out var account) ? account.Name : null,
                orderNo = order is null ? null : order.OrderNo.ToString("00"),
                isTakeaway = order?.IsTakeaway,
                tableName,
                source = receipt?.Source,
                supplierName = supplier?.Name,
                category = expense?.Category,
            };
        }).ToList();
    }

    public async Task<List<object>> ReportDailySales(DateOnly start, DateOnly end, CancellationToken ct)
    {
        var rows = await (
            from p in db.Payments.AsNoTracking()
            join o in db.Orders.AsNoTracking() on p.OrderId equals o.Id
            where o.BusinessDate >= start && o.BusinessDate <= end
            group p by o.BusinessDate
            into g
            orderby g.Key
            select new
            {
                Date = g.Key,
                Total = g.Sum(x => x.Amount),
            }).ToListAsync(ct);

        return rows.Select(x => (object)new
        {
            date = x.Date.ToString("yyyy-MM-dd"),
            total = (double)x.Total,
        }).ToList();
    }

    public async Task<object> ReportProfitLoss(DateOnly start, DateOnly end, CancellationToken ct)
    {
        var incomeFromPos = await (
            from p in db.Payments.AsNoTracking()
            join o in db.Orders.AsNoTracking() on p.OrderId equals o.Id
            where o.BusinessDate >= start && o.BusinessDate <= end
            select (decimal?)p.Amount).SumAsync(ct) ?? 0m;

        var incomeFromManual = await db.Receipts
            .AsNoTracking()
            .Where(x => x.ReceiptDate >= start && x.ReceiptDate <= end)
            .Select(x => (decimal?)x.Amount)
            .SumAsync(ct) ?? 0m;

        var expensesValue = await db.Expenses
            .AsNoTracking()
            .Where(x => x.ExpenseDate >= start && x.ExpenseDate <= end)
            .Select(x => (decimal?)x.Amount)
            .SumAsync(ct) ?? 0m;

        var incomeValue = (double)(incomeFromPos + incomeFromManual);
        var expenseValue = (double)expensesValue;

        return new
        {
            income = incomeValue,
            expenses = expenseValue,
            net = incomeValue - expenseValue,
        };
    }

    public async Task<List<object>> ReportCashFlow(DateTime start, DateTime end, CancellationToken ct)
    {
        var rows = await db.AccountTransactions
            .AsNoTracking()
            .Where(x => x.CreatedAt >= start && x.CreatedAt <= end)
            .GroupBy(x => x.CreatedAt.Date)
            .OrderBy(x => x.Key)
            .Select(g => new
            {
                Date = DateOnly.FromDateTime(g.Key),
                Inflow = g.Sum(x => x.Direction == "in" ? (decimal?)x.Amount : 0m) ?? 0m,
                Outflow = g.Sum(x => x.Direction == "out" ? (decimal?)x.Amount : 0m) ?? 0m,
            })
            .ToListAsync(ct);

        return rows.Select(x =>
        {
            var inflow = (double)x.Inflow;
            var outflow = (double)x.Outflow;
            return (object)new
            {
                date = x.Date.ToString("yyyy-MM-dd"),
                inflow,
                outflow,
                net = inflow - outflow,
            };
        }).ToList();
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
}
