using Microsoft.EntityFrameworkCore;
using Npgsql;
using ResPosBackend.Data;
using ResPosBackend.Infrastructure;
using ResPosBackend.Models;

namespace ResPosBackend.Services;

public sealed class AccountingService(PosDbContext db, SystemAccountsService systemAccounts)
{
    private sealed record CashierExpenseSettingsSnapshot(bool EnabledForCashier, decimal? CapAmount);

    public async Task<List<object>> ListAccounts(CancellationToken ct)
    {
        var rows = await (
            from a in db.Accounts.AsNoTracking()
            join parent in db.Accounts.AsNoTracking() on a.ParentAccountId equals parent.Id into parentJoin
            from parent in parentJoin.DefaultIfEmpty()
            join t in db.AccountTransactions.AsNoTracking() on a.Id equals t.AccountId into txs
            orderby a.CreatedAt
            select new
            {
                a.Id,
                a.Name,
                a.Type,
                a.Currency,
                a.IsActive,
                a.IsSystem,
                a.IsLocked,
                a.AccountScope,
                a.AccountKey,
                a.ShiftId,
                a.BaseAccountId,
                a.ParentAccountId,
                ParentAccountName = parent != null ? parent.Name : null,
                a.CreatedAt,
                Balance = txs
                    .Sum(x => x.Direction == "in"
                        ? (decimal?)x.Amount
                        : -(decimal?)x.Amount) ?? 0m,
            })
            .ToListAsync(ct);

        var accountIds = rows.Select(x => x.Id).ToList();
        var outgoingRelations = accountIds.Count == 0
            ? []
            : await (
                from rel in db.AccountRelations.AsNoTracking()
                join target in db.Accounts.AsNoTracking() on rel.ToAccountId equals target.Id into targetJoin
                from target in targetJoin.DefaultIfEmpty()
                where accountIds.Contains(rel.FromAccountId)
                select new
                {
                    rel.FromAccountId,
                    rel.ToAccountId,
                    TargetAccountName = target != null ? target.Name : null,
                    rel.Percentage,
                    rel.Kind,
                }).ToListAsync(ct);

        var outgoingByFromAccount = outgoingRelations
            .GroupBy(x => x.FromAccountId)
            .ToDictionary(
                g => g.Key,
                g => g.Select(x => (object)new
                {
                    targetAccountId = x.ToAccountId,
                    targetAccountName = x.TargetAccountName,
                    percentage = (double)x.Percentage,
                    kind = x.Kind,
                }).ToList());

        var subAccountCounts = accountIds.Count == 0
            ? new Dictionary<Guid, int>()
            : await db.Accounts.AsNoTracking()
                .Where(x => x.ParentAccountId.HasValue && accountIds.Contains(x.ParentAccountId.Value))
                .GroupBy(x => x.ParentAccountId!.Value)
                .Select(g => new { ParentAccountId = g.Key, Count = g.Count() })
                .ToDictionaryAsync(x => x.ParentAccountId, x => x.Count, ct);

        return rows.Select(x => (object)new
        {
            id = x.Id,
            name = x.Name,
            type = x.Type,
            currency = x.Currency,
            isActive = x.IsActive,
            isSystem = x.IsSystem,
            isLocked = x.IsLocked,
            accountScope = x.AccountScope,
            accountKey = x.AccountKey,
            shiftId = x.ShiftId,
            baseAccountId = x.BaseAccountId,
            parentAccountId = x.ParentAccountId,
            parentAccountName = x.ParentAccountName,
            createdAt = x.CreatedAt,
            balance = (double)x.Balance,
            subAccountsCount = subAccountCounts.TryGetValue(x.Id, out var count) ? count : 0,
            relations = outgoingByFromAccount.TryGetValue(x.Id, out var rels) ? rels : new List<object>(),
        }).ToList();
    }

    public async Task<object> CreateAccount(
        string name,
        string type,
        string currency,
        Guid? parentAccountId,
        IReadOnlyList<AccountRelationInput>? relations,
        CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var accountId = Guid.NewGuid();
        await ValidateParentAssignment(accountId, parentAccountId, ct);

        var entity = new PosAccount
        {
            Id = accountId,
            Name = name,
            Type = string.IsNullOrWhiteSpace(type) ? "cash" : type,
            Currency = string.IsNullOrWhiteSpace(currency) ? "ILS" : currency,
            IsActive = true,
            AccountScope = "custom",
            AccountKey = null,
            IsSystem = false,
            IsLocked = false,
            ShiftId = null,
            BaseAccountId = null,
            ParentAccountId = parentAccountId,
            CreatedAt = DateTime.UtcNow,
        };

        db.Accounts.Add(entity);
        await SaveWithConflictHandling(ct);

        await ReplaceAccountRelations(entity.Id, relations ?? [], ct);
        await SaveWithConflictHandling(ct);
        await tx.CommitAsync(ct);

        var item = await BuildAccountView(entity.Id, ct);
        if (item is null)
        {
            throw new InvalidOperationException("ACCOUNT_NOT_FOUND");
        }

        return item;
    }

    public async Task<object?> UpdateAccount(
        Guid id,
        string? name,
        string? type,
        string? currency,
        bool? isActive,
        Guid? parentAccountId,
        bool hasParentAccountId,
        IReadOnlyList<AccountRelationInput>? relations,
        bool hasRelations,
        CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var entity = await db.Accounts.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return null;
        }

        if (entity.IsSystem && entity.IsLocked)
        {
            throw new InvalidOperationException("ACCOUNT_LOCKED");
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

        if (hasParentAccountId)
        {
            await ValidateParentAssignment(entity.Id, parentAccountId, ct);
            entity.ParentAccountId = parentAccountId;
        }

        await SaveWithConflictHandling(ct);

        if (hasRelations)
        {
            await ReplaceAccountRelations(entity.Id, relations ?? [], ct);
            await SaveWithConflictHandling(ct);
        }

        await tx.CommitAsync(ct);
        return await BuildAccountView(entity.Id, ct);
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
        var normalizedMethod = SystemAccountsService.NormalizeMethod(method);
        if (string.IsNullOrWhiteSpace(normalizedMethod))
        {
            throw new InvalidOperationException("BAD_METHOD");
        }

        var account = await GetActiveManualAccountForOperations(accountId, ct);

        var existing = await db.PaymentMethodAccounts.FirstOrDefaultAsync(x => x.Method == normalizedMethod, ct);
        if (existing is null)
        {
            db.PaymentMethodAccounts.Add(new PosPaymentMethodAccount
            {
                Method = normalizedMethod,
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
            where map.Method == normalizedMethod
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
        var items = await (
            from s in db.Suppliers.AsNoTracking()
            join a in db.Accounts.AsNoTracking() on s.AccountId equals a.Id into aJoin
            from a in aJoin.DefaultIfEmpty()
            orderby s.Name
            select new
            {
                s.Id,
                s.Name,
                s.Phone,
                s.Email,
                s.Note,
                s.AccountId,
                AccountName = a != null ? a.Name : null,
                AccountType = a != null ? a.Type : null,
                AccountCurrency = a != null ? a.Currency : null,
                s.IsActive,
                s.CreatedAt,
            }).ToListAsync(ct);

        return items.Select(x => (object)new
        {
            id = x.Id,
            name = x.Name,
            phone = x.Phone,
            email = x.Email,
            note = x.Note,
            accountId = x.AccountId,
            accountName = x.AccountName,
            accountType = x.AccountType,
            accountCurrency = x.AccountCurrency,
            isActive = x.IsActive,
            createdAt = x.CreatedAt,
        }).ToList();
    }

    public async Task<object> CreateSupplier(
        string name,
        string? phone,
        string? email,
        string? note,
        Guid? accountId,
        string? createAccountName,
        string? createAccountType,
        string? createAccountCurrency,
        CancellationToken ct)
    {
        var resolvedAccountId = await ResolveOrCreateAccountId(
            accountId,
            createAccountName,
            createAccountType,
            createAccountCurrency,
            "supplier",
            ct);

        var entity = new PosSupplier
        {
            Id = Guid.NewGuid(),
            Name = name,
            Phone = string.IsNullOrWhiteSpace(phone) ? null : phone.Trim(),
            Email = string.IsNullOrWhiteSpace(email) ? null : email.Trim(),
            Note = string.IsNullOrWhiteSpace(note) ? null : note.Trim(),
            AccountId = resolvedAccountId,
            IsActive = true,
            CreatedAt = DateTime.UtcNow,
        };

        db.Suppliers.Add(entity);
        await SaveWithConflictHandling(ct);

        var account = entity.AccountId.HasValue
            ? await db.Accounts.AsNoTracking().FirstOrDefaultAsync(x => x.Id == entity.AccountId.Value, ct)
            : null;

        return new
        {
            id = entity.Id,
            name = entity.Name,
            phone = entity.Phone,
            email = entity.Email,
            note = entity.Note,
            accountId = entity.AccountId,
            accountName = account?.Name,
            accountType = account?.Type,
            accountCurrency = account?.Currency,
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
        Guid? accountId,
        bool hasAccountId,
        string? createAccountName,
        string? createAccountType,
        string? createAccountCurrency,
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

        if (!string.IsNullOrWhiteSpace(createAccountName))
        {
            entity.AccountId = await ResolveOrCreateAccountId(
                null,
                createAccountName,
                createAccountType,
                createAccountCurrency,
                "supplier",
                ct);
        }
        else if (hasAccountId)
        {
            if (accountId.HasValue)
            {
                var accountExists = await db.Accounts.AsNoTracking().AnyAsync(x => x.Id == accountId.Value, ct);
                if (!accountExists)
                {
                    throw new InvalidOperationException("ACCOUNT_NOT_FOUND");
                }
            }

            entity.AccountId = accountId;
        }

        if (isActive.HasValue)
        {
            entity.IsActive = isActive.Value;
        }

        await SaveWithConflictHandling(ct);

        var account = entity.AccountId.HasValue
            ? await db.Accounts.AsNoTracking().FirstOrDefaultAsync(x => x.Id == entity.AccountId.Value, ct)
            : null;

        return new
        {
            id = entity.Id,
            name = entity.Name,
            phone = entity.Phone,
            email = entity.Email,
            note = entity.Note,
            accountId = entity.AccountId,
            accountName = account?.Name,
            accountType = account?.Type,
            accountCurrency = account?.Currency,
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
        await GetActiveManualAccountForOperations(accountId, ct);
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
        Guid? accountId,
        Guid? supplierId,
        Guid? employeeId,
        DateOnly? expenseDate,
        string? attachmentUrl,
        string? note,
        CancellationToken ct)
    {
        var resolvedAccountId = await ResolveReferenceAccountId(accountId, supplierId, employeeId, ct);
        if (!resolvedAccountId.HasValue)
        {
            throw new InvalidOperationException("ACCOUNT_REQUIRED");
        }
        await GetActiveManualAccountForOperations(resolvedAccountId.Value, ct);

        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var now = DateTime.UtcNow;
        var vaultAccounts = await systemAccounts.EnsureVaultBaseAccounts(now, ct);
        if (!vaultAccounts.TryGetValue(SystemAccountsService.ExpensesAccountKey, out var expenseAccount))
        {
            throw new InvalidOperationException("EXPENSE_ACCOUNT_MISSING");
        }

        var entity = new PosExpense
        {
            Id = Guid.NewGuid(),
            ExpenseDate = expenseDate ?? DateOnly.FromDateTime(now),
            Category = category,
            SupplierId = supplierId,
            EmployeeId = employeeId,
            Amount = (decimal)amount,
            Method = method,
            AccountId = expenseAccount.Id,
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
            AccountId = resolvedAccountId.Value,
            Direction = "out",
            Amount = (decimal)amount,
            SourceType = "expense",
            SourceId = entity.Id,
            Note = entity.Note,
            CreatedBy = createdBy,
            CreatedAt = now,
        });

        db.AccountTransactions.Add(new PosAccountTransaction
        {
            Id = Guid.NewGuid(),
            AccountId = expenseAccount.Id,
            Direction = "in",
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
            supplierId = entity.SupplierId,
            employeeId = entity.EmployeeId,
            createdAt = entity.CreatedAt,
        };
    }

    public async Task<object> GetCashierExpenseOverview(CancellationToken ct)
    {
        var settings = await LoadCashierExpenseSettings(ct);
        var suppliers = await db.Suppliers
            .AsNoTracking()
            .Where(x => x.IsActive)
            .OrderBy(x => x.Name)
            .Select(x => new
            {
                x.Id,
                x.Name,
            })
            .ToListAsync(ct);
        var employees = await db.Employees
            .AsNoTracking()
            .Where(x => x.IsActive)
            .OrderBy(x => x.Name)
            .Select(x => new
            {
                x.Id,
                x.Name,
            })
            .ToListAsync(ct);

        var shift = await db.Shifts
            .AsNoTracking()
            .OrderByDescending(x => x.OpenedAt)
            .FirstOrDefaultAsync(x => x.ClosedAt == null, ct);

        if (shift is null)
        {
            return new
            {
                enabledForCashier = settings.EnabledForCashier,
                capAmount = settings.CapAmount is null ? null : (double?)settings.CapAmount.Value,
                spentAmount = 0d,
                remainingAmount = settings.CapAmount is null ? null : (double?)settings.CapAmount.Value,
                shift = (object?)null,
                items = Array.Empty<object>(),
                suppliers = suppliers.Select(x => (object)new
                {
                    id = x.Id,
                    name = x.Name,
                }).ToList(),
                employees = employees.Select(x => (object)new
                {
                    id = x.Id,
                    name = x.Name,
                }).ToList(),
            };
        }

        var shiftAccounts = await systemAccounts.EnsureShiftSessionAccounts(shift, DateTime.UtcNow, ct);
        shiftAccounts.TryGetValue("cash", out var shiftCashAccount);
        shiftAccounts.TryGetValue(SystemAccountsService.ExpensesAccountKey, out var shiftExpenseAccount);

        decimal spent = 0m;
        var expenses = new List<object>();
        if (shiftCashAccount is not null && shiftExpenseAccount is not null)
        {
            spent = await db.AccountTransactions
                .AsNoTracking()
                .Where(x =>
                    x.AccountId == shiftCashAccount.Id &&
                    x.SourceType == "cashier_expense" &&
                    x.Direction == "out")
                .Select(x => (decimal?)x.Amount)
                .SumAsync(ct) ?? 0m;

            var rows = await (
                from e in db.Expenses.AsNoTracking()
                join s in db.Suppliers.AsNoTracking() on e.SupplierId equals s.Id into sJoin
                from s in sJoin.DefaultIfEmpty()
                join emp in db.Employees.AsNoTracking() on e.EmployeeId equals emp.Id into empJoin
                from emp in empJoin.DefaultIfEmpty()
                where (e.AccountId == shiftExpenseAccount.Id || e.AccountId == shiftCashAccount.Id) &&
                      e.CreatedAt >= shift.OpenedAt
                orderby e.CreatedAt descending
                select new
                {
                    e.Id,
                    e.ExpenseDate,
                    e.Category,
                    e.Amount,
                    e.Method,
                    e.AccountId,
                    e.SupplierId,
                    e.EmployeeId,
                    e.Note,
                    e.CreatedAt,
                    SupplierName = s != null ? s.Name : null,
                    EmployeeName = emp != null ? emp.Name : null,
                })
                .Take(200)
                .ToListAsync(ct);

            expenses = rows.Select(x => (object)new
            {
                id = x.Id,
                date = x.ExpenseDate.ToString("yyyy-MM-dd"),
                category = x.Category,
                amount = (double)x.Amount,
                method = x.Method,
                accountId = x.AccountId,
                supplierId = x.SupplierId,
                employeeId = x.EmployeeId,
                supplierName = x.SupplierName,
                employeeName = x.EmployeeName,
                note = x.Note,
                createdAt = x.CreatedAt,
            }).ToList();
        }

        var remaining = settings.CapAmount.HasValue
            ? Math.Max(0m, settings.CapAmount.Value - spent)
            : (decimal?)null;

        return new
        {
            enabledForCashier = settings.EnabledForCashier,
            capAmount = settings.CapAmount is null ? null : (double?)settings.CapAmount.Value,
            spentAmount = (double)spent,
            remainingAmount = remaining is null ? null : (double?)remaining.Value,
            shift = new
            {
                id = shift.Id,
                openedAt = shift.OpenedAt,
                openedBy = shift.OpenedBy,
                cashAccountId = shiftCashAccount?.Id,
                cashAccountName = shiftCashAccount?.Name,
                expenseAccountId = shiftExpenseAccount?.Id,
                expenseAccountName = shiftExpenseAccount?.Name,
            },
            items = expenses,
            suppliers = suppliers.Select(x => (object)new
            {
                id = x.Id,
                name = x.Name,
            }).ToList(),
            employees = employees.Select(x => (object)new
            {
                id = x.Id,
                name = x.Name,
            }).ToList(),
        };
    }

    public async Task<object> CreateCashierExpense(
        Guid createdBy,
        double amount,
        Guid? supplierId,
        Guid? employeeId,
        DateOnly? expenseDate,
        string? note,
        CancellationToken ct)
    {
        var settings = await LoadCashierExpenseSettings(ct);
        if (!settings.EnabledForCashier)
        {
            throw new InvalidOperationException("CASHIER_EXPENSES_DISABLED");
        }

        if (amount <= 0)
        {
            throw new InvalidOperationException("CASHIER_EXPENSE_AMOUNT_INVALID");
        }

        await using var tx = await db.Database.BeginTransactionAsync(ct);
        var (_, shiftCashAccount, shiftExpenseAccount) = await GetOpenShiftCashAndExpenseAccounts(ct);
        var (supplier, employee) = await ResolveCashierExpenseTarget(supplierId, employeeId, ct);

        var requestedAmount = decimal.Round((decimal)amount, 2, MidpointRounding.AwayFromZero);
        var spent = await SumCashierExpenseSpent(shiftCashAccount.Id, null, ct);

        if (settings.CapAmount.HasValue && spent + requestedAmount > settings.CapAmount.Value + 0.0001m)
        {
            throw new InvalidOperationException("CASHIER_EXPENSE_CAP_EXCEEDED");
        }

        var now = DateTime.UtcNow;
        var entity = new PosExpense
        {
            Id = Guid.NewGuid(),
            ExpenseDate = expenseDate ?? DateOnly.FromDateTime(now),
            Category = "cashier_expense",
            SupplierId = supplierId,
            EmployeeId = employeeId,
            Amount = requestedAmount,
            Method = "cash",
            AccountId = shiftExpenseAccount.Id,
            AttachmentUrl = null,
            Note = string.IsNullOrWhiteSpace(note) ? null : note.Trim(),
            CreatedBy = createdBy,
            CreatedAt = now,
        };

        db.Expenses.Add(entity);
        await db.SaveChangesAsync(ct);

        db.AccountTransactions.Add(new PosAccountTransaction
        {
            Id = Guid.NewGuid(),
            AccountId = shiftCashAccount.Id,
            Direction = "out",
            Amount = requestedAmount,
            SourceType = "cashier_expense",
            SourceId = entity.Id,
            Note = entity.Note ?? "Cashier expense",
            CreatedBy = createdBy,
            CreatedAt = now,
        });

        db.AccountTransactions.Add(new PosAccountTransaction
        {
            Id = Guid.NewGuid(),
            AccountId = shiftExpenseAccount.Id,
            Direction = "in",
            Amount = requestedAmount,
            SourceType = "cashier_expense",
            SourceId = entity.Id,
            Note = entity.Note ?? "Cashier expense",
            CreatedBy = createdBy,
            CreatedAt = now,
        });

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);

        var spentAfter = spent + requestedAmount;
        var remainingAfter = settings.CapAmount.HasValue
            ? Math.Max(0m, settings.CapAmount.Value - spentAfter)
            : (decimal?)null;

        return new
        {
            id = entity.Id,
            date = entity.ExpenseDate.ToString("yyyy-MM-dd"),
            category = entity.Category,
            amount = (double)entity.Amount,
            method = entity.Method,
            accountId = entity.AccountId,
            supplierId = entity.SupplierId,
            employeeId = entity.EmployeeId,
            supplierName = supplier?.Name,
            employeeName = employee?.Name,
            note = entity.Note,
            createdAt = entity.CreatedAt,
            capAmount = settings.CapAmount is null ? null : (double?)settings.CapAmount.Value,
            spentAmount = (double)spentAfter,
            remainingAmount = remainingAfter is null ? null : (double?)remainingAfter.Value,
        };
    }

    public async Task<object> UpdateCashierExpense(
        Guid expenseId,
        double amount,
        Guid? supplierId,
        Guid? employeeId,
        DateOnly? expenseDate,
        string? note,
        CancellationToken ct)
    {
        if (amount <= 0)
        {
            throw new InvalidOperationException("CASHIER_EXPENSE_AMOUNT_INVALID");
        }

        var settings = await LoadCashierExpenseSettings(ct);
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var (_, shiftCashAccount, shiftExpenseAccount, expense, expenseOutTx, expenseInTx) =
            await GetCashierExpenseForOpenShift(expenseId, ct);
        var (supplier, employee) = await ResolveCashierExpenseTarget(supplierId, employeeId, ct);

        var requestedAmount = decimal.Round((decimal)amount, 2, MidpointRounding.AwayFromZero);
        var spentExcludingCurrent = await SumCashierExpenseSpent(shiftCashAccount.Id, expense.Id, ct);
        if (settings.CapAmount.HasValue &&
            spentExcludingCurrent + requestedAmount > settings.CapAmount.Value + 0.0001m)
        {
            throw new InvalidOperationException("CASHIER_EXPENSE_CAP_EXCEEDED");
        }

        expense.Amount = requestedAmount;
        expense.SupplierId = supplierId;
        expense.EmployeeId = employeeId;
        expense.AccountId = shiftExpenseAccount.Id;
        expense.Note = string.IsNullOrWhiteSpace(note) ? null : note.Trim();
        if (expenseDate.HasValue)
        {
            expense.ExpenseDate = expenseDate.Value;
        }

        expenseOutTx.Amount = requestedAmount;
        expenseOutTx.Note = expense.Note ?? "Cashier expense";
        if (expenseInTx is null)
        {
            db.AccountTransactions.Add(new PosAccountTransaction
            {
                Id = Guid.NewGuid(),
                AccountId = shiftExpenseAccount.Id,
                Direction = "in",
                Amount = requestedAmount,
                SourceType = "cashier_expense",
                SourceId = expense.Id,
                Note = expense.Note ?? "Cashier expense",
                CreatedBy = expense.CreatedBy,
                CreatedAt = DateTime.UtcNow,
            });
        }
        else
        {
            expenseInTx.AccountId = shiftExpenseAccount.Id;
            expenseInTx.Amount = requestedAmount;
            expenseInTx.Note = expense.Note ?? "Cashier expense";
        }

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);

        var spentAfter = spentExcludingCurrent + requestedAmount;
        var remainingAfter = settings.CapAmount.HasValue
            ? Math.Max(0m, settings.CapAmount.Value - spentAfter)
            : (decimal?)null;

        return new
        {
            id = expense.Id,
            date = expense.ExpenseDate.ToString("yyyy-MM-dd"),
            category = expense.Category,
            amount = (double)expense.Amount,
            method = expense.Method,
            accountId = expense.AccountId,
            supplierId = expense.SupplierId,
            employeeId = expense.EmployeeId,
            supplierName = supplier?.Name,
            employeeName = employee?.Name,
            note = expense.Note,
            createdAt = expense.CreatedAt,
            capAmount = settings.CapAmount is null ? null : (double?)settings.CapAmount.Value,
            spentAmount = (double)spentAfter,
            remainingAmount = remainingAfter is null ? null : (double?)remainingAfter.Value,
        };
    }

    public async Task<object> DeleteCashierExpense(Guid expenseId, CancellationToken ct)
    {
        var settings = await LoadCashierExpenseSettings(ct);
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var (_, shiftCashAccount, _, expense, expenseOutTx, expenseInTx) = await GetCashierExpenseForOpenShift(expenseId, ct);
        db.AccountTransactions.Remove(expenseOutTx);
        if (expenseInTx is not null)
        {
            db.AccountTransactions.Remove(expenseInTx);
        }
        db.Expenses.Remove(expense);
        await db.SaveChangesAsync(ct);

        var spentAfter = await SumCashierExpenseSpent(shiftCashAccount.Id, null, ct);
        var remainingAfter = settings.CapAmount.HasValue
            ? Math.Max(0m, settings.CapAmount.Value - spentAfter)
            : (decimal?)null;

        await tx.CommitAsync(ct);

        return new
        {
            id = expenseId,
            capAmount = settings.CapAmount is null ? null : (double?)settings.CapAmount.Value,
            spentAmount = (double)spentAfter,
            remainingAmount = remainingAfter is null ? null : (double?)remainingAfter.Value,
        };
    }

    private async Task<(PosShift Shift, PosAccount ShiftCashAccount, PosAccount ShiftExpenseAccount)> GetOpenShiftCashAndExpenseAccounts(CancellationToken ct)
    {
        var shift = await db.Shifts
            .OrderByDescending(x => x.OpenedAt)
            .FirstOrDefaultAsync(x => x.ClosedAt == null, ct);
        if (shift is null)
        {
            throw new InvalidOperationException("SHIFT_REQUIRED");
        }

        var shiftAccounts = await systemAccounts.EnsureShiftSessionAccounts(shift, DateTime.UtcNow, ct);
        if (!shiftAccounts.TryGetValue("cash", out var shiftCashAccount) || shiftCashAccount is null)
        {
            throw new InvalidOperationException("SHIFT_CASH_ACCOUNT_NOT_FOUND");
        }

        if (!shiftAccounts.TryGetValue(SystemAccountsService.ExpensesAccountKey, out var shiftExpenseAccount) ||
            shiftExpenseAccount is null)
        {
            throw new InvalidOperationException("SHIFT_EXPENSE_ACCOUNT_NOT_FOUND");
        }

        return (shift, shiftCashAccount, shiftExpenseAccount);
    }

    private async Task<(
        PosShift Shift,
        PosAccount ShiftCashAccount,
        PosAccount ShiftExpenseAccount,
        PosExpense Expense,
        PosAccountTransaction ExpenseOutTx,
        PosAccountTransaction? ExpenseInTx)> GetCashierExpenseForOpenShift(
        Guid expenseId,
        CancellationToken ct)
    {
        var (shift, shiftCashAccount, shiftExpenseAccount) = await GetOpenShiftCashAndExpenseAccounts(ct);
        var expense = await db.Expenses
            .FirstOrDefaultAsync(x =>
                x.Id == expenseId &&
                (x.AccountId == shiftExpenseAccount.Id || x.AccountId == shiftCashAccount.Id),
                ct);
        if (expense is null)
        {
            throw new InvalidOperationException("CASHIER_EXPENSE_NOT_FOUND");
        }

        var expenseOutTx = await db.AccountTransactions
            .FirstOrDefaultAsync(x =>
                x.AccountId == shiftCashAccount.Id &&
                x.SourceType == "cashier_expense" &&
                x.Direction == "out" &&
                x.SourceId == expenseId,
                ct);
        if (expenseOutTx is null)
        {
            throw new InvalidOperationException("CASHIER_EXPENSE_NOT_FOUND");
        }

        var expenseInTx = await db.AccountTransactions
            .FirstOrDefaultAsync(x =>
                x.AccountId == shiftExpenseAccount.Id &&
                x.SourceType == "cashier_expense" &&
                x.Direction == "in" &&
                x.SourceId == expenseId,
                ct);

        return (shift, shiftCashAccount, shiftExpenseAccount, expense, expenseOutTx, expenseInTx);
    }

    private async Task<decimal> SumCashierExpenseSpent(Guid accountId, Guid? excludeExpenseId, CancellationToken ct)
    {
        var query = db.AccountTransactions
            .AsNoTracking()
            .Where(x =>
                x.AccountId == accountId &&
                x.SourceType == "cashier_expense" &&
                x.Direction == "out");
        if (excludeExpenseId.HasValue)
        {
            query = query.Where(x => x.SourceId != excludeExpenseId.Value);
        }

        return await query.Select(x => (decimal?)x.Amount).SumAsync(ct) ?? 0m;
    }

    private async Task<(PosSupplier? Supplier, PosEmployee? Employee)> ResolveCashierExpenseTarget(
        Guid? supplierId,
        Guid? employeeId,
        CancellationToken ct)
    {
        if (supplierId.HasValue && employeeId.HasValue)
        {
            throw new InvalidOperationException("BAD_EXPENSE_TARGET");
        }

        PosSupplier? supplier = null;
        PosEmployee? employee = null;
        if (supplierId.HasValue)
        {
            supplier = await db.Suppliers
                .AsNoTracking()
                .FirstOrDefaultAsync(x => x.Id == supplierId.Value, ct);
            if (supplier is null)
            {
                throw new InvalidOperationException("SUPPLIER_NOT_FOUND");
            }

            if (!supplier.IsActive)
            {
                throw new InvalidOperationException("SUPPLIER_INACTIVE");
            }
        }

        if (employeeId.HasValue)
        {
            employee = await db.Employees
                .AsNoTracking()
                .FirstOrDefaultAsync(x => x.Id == employeeId.Value, ct);
            if (employee is null)
            {
                throw new InvalidOperationException("EMPLOYEE_NOT_FOUND");
            }

            if (!employee.IsActive)
            {
                throw new InvalidOperationException("EMPLOYEE_INACTIVE");
            }
        }

        return (supplier, employee);
    }

    public async Task Deposit(Guid createdBy, Guid accountId, double amount, string? note, CancellationToken ct)
    {
        await GetActiveManualAccountForOperations(accountId, ct);
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
        await GetActiveManualAccountForOperations(accountId, ct);
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

        var fromAccount = await GetActiveManualAccountForOperations(fromAccountId, ct);
        var toAccount = await GetActiveManualAccountForOperations(toAccountId, ct);
        if (!string.Equals(fromAccount.Currency, toAccount.Currency, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("TRANSFER_CURRENCY_MISMATCH");
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
            where (!start.HasValue || o.BusinessDate >= start.Value)
                  && (!end.HasValue || o.BusinessDate <= end.Value)
            select new
            {
                PaymentId = p.Id,
                Date = o.BusinessDate,
                Amount = p.Amount,
                Method = p.Method,
                OrderNo = (short?)o.OrderNo,
                IsTakeaway = (bool?)o.IsTakeaway,
                TableName = t != null ? t.Name : null,
                CreatedAt = p.CreatedAt,
            }).ToListAsync(ct);

        var txAccountByPaymentId = new Dictionary<Guid, Guid>();
        if (posRows.Count > 0)
        {
            var paymentIds = posRows.Select(x => x.PaymentId).ToList();
            var txRows = await db.AccountTransactions
                .AsNoTracking()
                .Where(x => x.SourceType == "pos_payment" && x.SourceId.HasValue && paymentIds.Contains(x.SourceId.Value))
                .OrderByDescending(x => x.CreatedAt)
                .Select(x => new
                {
                    SourceId = x.SourceId!.Value,
                    x.AccountId,
                })
                .ToListAsync(ct);

            txAccountByPaymentId = txRows
                .GroupBy(x => x.SourceId)
                .ToDictionary(x => x.Key, x => x.First().AccountId);
        }

        var accountNameById = txAccountByPaymentId.Count == 0
            ? new Dictionary<Guid, string>()
            : await db.Accounts
                .AsNoTracking()
                .Where(x => txAccountByPaymentId.Values.Contains(x.Id))
                .ToDictionaryAsync(x => x.Id, x => x.Name, ct);

        var posPayload = posRows.Select(x => new
        {
            Kind = "pos",
            Id = x.PaymentId,
            Date = x.Date,
            Amount = x.Amount,
            Method = x.Method,
            Source = (string?)null,
            SupplierName = (string?)null,
            AccountName = txAccountByPaymentId.TryGetValue(x.PaymentId, out var accountId) &&
                          accountNameById.TryGetValue(accountId, out var accountName)
                ? accountName
                : null,
            OrderNo = x.OrderNo,
            IsTakeaway = x.IsTakeaway,
            TableName = x.TableName,
            CreatedAt = x.CreatedAt,
        });

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

        var rows = posPayload
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
            join emp in db.Employees.AsNoTracking() on e.EmployeeId equals emp.Id into empJoin
            from emp in empJoin.DefaultIfEmpty()
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
                e.SupplierId,
                e.EmployeeId,
                e.AccountId,
                SupplierName = s != null ? s.Name : null,
                EmployeeName = emp != null ? emp.Name : null,
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
            supplierId = x.SupplierId,
            employeeId = x.EmployeeId,
            accountId = x.AccountId,
            supplierName = x.SupplierName,
            employeeName = x.EmployeeName,
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

        var posSourceIds = transactions
            .Where(x => x.SourceType == "pos_payment" && x.SourceId.HasValue)
            .Select(x => x.SourceId!.Value)
            .Distinct()
            .ToList();
        var posPayments = posSourceIds.Count == 0
            ? []
            : await db.Payments.AsNoTracking()
                .Where(x => posSourceIds.Contains(x.Id))
                .Select(x => new { x.Id, x.OrderId })
                .ToListAsync(ct);
        var orderIdByPaymentId = posPayments.ToDictionary(x => x.Id, x => x.OrderId);
        var orderIds = posPayments.Select(x => x.OrderId).ToHashSet();
        foreach (var sourceId in posSourceIds)
        {
            if (!orderIdByPaymentId.ContainsKey(sourceId))
            {
                orderIds.Add(sourceId);
            }
        }

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

        var employeeIds = expenses
            .Where(x => x.EmployeeId.HasValue)
            .Select(x => x.EmployeeId!.Value)
            .Distinct()
            .ToList();
        var employeeById = employeeIds.Count == 0
            ? new Dictionary<Guid, PosEmployee>()
            : await db.Employees
                .AsNoTracking()
                .Where(x => employeeIds.Contains(x.Id))
                .ToDictionaryAsync(x => x.Id, x => x, ct);

        return transactions.Select(x =>
        {
            PosOrder? order = null;
            PosReceipt? receipt = null;
            PosExpense? expense = null;
            PosSupplier? supplier = null;
            PosEmployee? employee = null;

            if (x.SourceType == "pos_payment" && x.SourceId.HasValue)
            {
                var orderId = orderIdByPaymentId.TryGetValue(x.SourceId.Value, out var mappedOrderId)
                    ? mappedOrderId
                    : x.SourceId.Value;
                orderById.TryGetValue(orderId, out order);
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

            var employeeId = expense?.EmployeeId;
            if (employeeId.HasValue)
            {
                employeeById.TryGetValue(employeeId.Value, out employee);
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
                employeeName = employee?.Name,
                category = expense?.Category,
            };
        }).ToList();
    }

    private async Task<Guid?> ResolveReferenceAccountId(
        Guid? accountId,
        Guid? supplierId,
        Guid? employeeId,
        CancellationToken ct)
    {
        if (supplierId.HasValue && employeeId.HasValue)
        {
            throw new InvalidOperationException("BAD_EXPENSE_TARGET");
        }

        if (accountId.HasValue)
        {
            await GetActiveManualAccountForOperations(accountId.Value, ct);
            return accountId;
        }

        if (supplierId.HasValue)
        {
            var supplier = await db.Suppliers.AsNoTracking().FirstOrDefaultAsync(x => x.Id == supplierId.Value, ct);
            if (supplier is null)
            {
                throw new InvalidOperationException("SUPPLIER_NOT_FOUND");
            }

            if (supplier.AccountId.HasValue)
            {
                await GetActiveManualAccountForOperations(supplier.AccountId.Value, ct);
            }

            return supplier.AccountId;
        }

        if (employeeId.HasValue)
        {
            var employee = await db.Employees.AsNoTracking().FirstOrDefaultAsync(x => x.Id == employeeId.Value, ct);
            if (employee is null)
            {
                throw new InvalidOperationException("EMPLOYEE_NOT_FOUND");
            }

            if (employee.AccountId.HasValue)
            {
                await GetActiveManualAccountForOperations(employee.AccountId.Value, ct);
            }

            return employee.AccountId;
        }

        return null;
    }

    private async Task<Guid?> ResolveOrCreateAccountId(
        Guid? accountId,
        string? createAccountName,
        string? createAccountType,
        string? createAccountCurrency,
        string defaultType,
        CancellationToken ct)
    {
        var hasCreateAccount = !string.IsNullOrWhiteSpace(createAccountName);
        if (accountId.HasValue && hasCreateAccount)
        {
            throw new InvalidOperationException("BAD_ACCOUNT_SELECTION");
        }

        if (accountId.HasValue)
        {
            await GetActiveManualAccountForOperations(accountId.Value, ct);
            return accountId.Value;
        }

        if (!hasCreateAccount)
        {
            return null;
        }

        var account = new PosAccount
        {
            Id = Guid.NewGuid(),
            Name = createAccountName!.Trim(),
            Type = string.IsNullOrWhiteSpace(createAccountType) ? defaultType : createAccountType.Trim().ToLowerInvariant(),
            Currency = string.IsNullOrWhiteSpace(createAccountCurrency) ? "ILS" : createAccountCurrency.Trim().ToUpperInvariant(),
            IsActive = true,
            AccountScope = "custom",
            AccountKey = null,
            IsSystem = false,
            IsLocked = false,
            ShiftId = null,
            BaseAccountId = null,
            CreatedAt = DateTime.UtcNow,
        };

        db.Accounts.Add(account);
        await SaveWithConflictHandling(ct);
        return account.Id;
    }

    private async Task<object?> BuildAccountView(Guid accountId, CancellationToken ct)
    {
        var row = await (
            from a in db.Accounts.AsNoTracking()
            join parent in db.Accounts.AsNoTracking() on a.ParentAccountId equals parent.Id into parentJoin
            from parent in parentJoin.DefaultIfEmpty()
            where a.Id == accountId
            select new
            {
                a.Id,
                a.Name,
                a.Type,
                a.Currency,
                a.IsActive,
                a.IsSystem,
                a.IsLocked,
                a.AccountScope,
                a.AccountKey,
                a.ShiftId,
                a.BaseAccountId,
                a.ParentAccountId,
                ParentAccountName = parent != null ? parent.Name : null,
                a.CreatedAt,
            }).FirstOrDefaultAsync(ct);
        if (row is null)
        {
            return null;
        }

        var balance = await db.AccountTransactions
            .AsNoTracking()
            .Where(x => x.AccountId == accountId)
            .SumAsync(x => x.Direction == "in" ? x.Amount : -x.Amount, ct);

        var relations = await (
            from rel in db.AccountRelations.AsNoTracking()
            join target in db.Accounts.AsNoTracking() on rel.ToAccountId equals target.Id into targetJoin
            from target in targetJoin.DefaultIfEmpty()
            where rel.FromAccountId == accountId
            orderby rel.Kind, target != null ? target.Name : null
            select new
            {
                rel.ToAccountId,
                TargetAccountName = target != null ? target.Name : null,
                rel.Percentage,
                rel.Kind,
            }).ToListAsync(ct);

        var subAccountsCount = await db.Accounts.AsNoTracking().CountAsync(x => x.ParentAccountId == accountId, ct);

        return new
        {
            id = row.Id,
            name = row.Name,
            type = row.Type,
            currency = row.Currency,
            isActive = row.IsActive,
            isSystem = row.IsSystem,
            isLocked = row.IsLocked,
            accountScope = row.AccountScope,
            accountKey = row.AccountKey,
            shiftId = row.ShiftId,
            baseAccountId = row.BaseAccountId,
            parentAccountId = row.ParentAccountId,
            parentAccountName = row.ParentAccountName,
            createdAt = row.CreatedAt,
            balance = (double)balance,
            subAccountsCount,
            relations = relations.Select(x => (object)new
            {
                targetAccountId = x.ToAccountId,
                targetAccountName = x.TargetAccountName,
                percentage = (double)x.Percentage,
                kind = x.Kind,
            }).ToList(),
        };
    }

    private async Task ValidateParentAssignment(Guid accountId, Guid? parentAccountId, CancellationToken ct)
    {
        if (!parentAccountId.HasValue)
        {
            return;
        }

        if (parentAccountId.Value == accountId)
        {
            throw new InvalidOperationException("ACCOUNT_PARENT_SELF");
        }

        var parent = await db.Accounts.AsNoTracking().FirstOrDefaultAsync(x => x.Id == parentAccountId.Value, ct);
        if (parent is null)
        {
            throw new InvalidOperationException("PARENT_ACCOUNT_NOT_FOUND");
        }

        if (string.Equals(parent.AccountScope, SystemAccountsService.ShiftSessionScope, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("ACCOUNT_MANAGED_BY_SHIFT");
        }

        var cursor = parent.ParentAccountId;
        var guard = 0;
        while (cursor.HasValue && guard < 200)
        {
            if (cursor.Value == accountId)
            {
                throw new InvalidOperationException("ACCOUNT_PARENT_CYCLE");
            }

            var next = await db.Accounts
                .AsNoTracking()
                .Where(x => x.Id == cursor.Value)
                .Select(x => x.ParentAccountId)
                .FirstOrDefaultAsync(ct);

            cursor = next;
            guard++;
        }
    }

    private async Task ReplaceAccountRelations(Guid fromAccountId, IReadOnlyList<AccountRelationInput> relations, CancellationToken ct)
    {
        var existing = await db.AccountRelations.Where(x => x.FromAccountId == fromAccountId).ToListAsync(ct);
        if (existing.Count > 0)
        {
            db.AccountRelations.RemoveRange(existing);
        }

        if (relations.Count == 0)
        {
            return;
        }

        var normalized = new List<(Guid TargetAccountId, decimal Percentage, string Kind)>();
        foreach (var rel in relations)
        {
            if (rel.TargetAccountId == Guid.Empty)
            {
                throw new InvalidOperationException("BAD_ACCOUNT_RELATION");
            }

            if (rel.TargetAccountId == fromAccountId)
            {
                throw new InvalidOperationException("ACCOUNT_RELATION_SELF");
            }

            var percentage = Math.Round(rel.Percentage, 2, MidpointRounding.AwayFromZero);
            if (percentage <= 0 || percentage > 100)
            {
                throw new InvalidOperationException("BAD_ACCOUNT_RELATION_PERCENTAGE");
            }

            var kind = NormalizeRelationKind(rel.Kind);
            normalized.Add((rel.TargetAccountId, percentage, kind));
        }

        if (normalized
            .GroupBy(x => new { x.TargetAccountId, x.Kind })
            .Any(g => g.Count() > 1))
        {
            throw new InvalidOperationException("ACCOUNT_RELATION_DUPLICATE");
        }

        foreach (var group in normalized.GroupBy(x => x.Kind))
        {
            var total = group.Sum(x => x.Percentage);
            if (total > 100)
            {
                throw new InvalidOperationException("ACCOUNT_RELATION_PERCENTAGE_OVER_100");
            }
        }

        var targetIds = normalized.Select(x => x.TargetAccountId).Distinct().ToList();
        var targets = await db.Accounts.AsNoTracking()
            .Where(x => targetIds.Contains(x.Id))
            .ToDictionaryAsync(x => x.Id, x => x, ct);
        if (targets.Count != targetIds.Count)
        {
            throw new InvalidOperationException("RELATION_ACCOUNT_NOT_FOUND");
        }

        foreach (var target in targets.Values)
        {
            if (string.Equals(target.AccountScope, SystemAccountsService.ShiftSessionScope, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("ACCOUNT_MANAGED_BY_SHIFT");
            }
        }

        foreach (var rel in normalized)
        {
            db.AccountRelations.Add(new PosAccountRelation
            {
                Id = Guid.NewGuid(),
                FromAccountId = fromAccountId,
                ToAccountId = rel.TargetAccountId,
                Percentage = rel.Percentage,
                Kind = rel.Kind,
                CreatedAt = DateTime.UtcNow,
            });
        }
    }

    private static string NormalizeRelationKind(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return "allocation";
        }

        return raw.Trim().ToLowerInvariant();
    }

    private async Task<PosAccount> GetActiveManualAccountForOperations(Guid accountId, CancellationToken ct)
    {
        var account = await db.Accounts.FirstOrDefaultAsync(x => x.Id == accountId, ct);
        if (account is null)
        {
            throw new InvalidOperationException("ACCOUNT_NOT_FOUND");
        }

        if (!account.IsActive)
        {
            throw new InvalidOperationException("ACCOUNT_INACTIVE");
        }

        if (string.Equals(account.AccountScope, SystemAccountsService.ShiftSessionScope, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("ACCOUNT_MANAGED_BY_SHIFT");
        }

        return account;
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
        if (capAmount.HasValue && capAmount.Value <= 0)
        {
            capAmount = null;
        }

        return new CashierExpenseSettingsSnapshot(enabled, capAmount);
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

        return raw.Trim() switch
        {
            "1" => true,
            "0" => false,
            _ => null,
        };
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

public sealed record AccountRelationInput(Guid TargetAccountId, decimal Percentage, string? Kind);
