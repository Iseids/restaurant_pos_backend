using Microsoft.EntityFrameworkCore;
using ResPosBackend.Data;
using ResPosBackend.Infrastructure;
using ResPosBackend.Models;

namespace ResPosBackend.Services;

public sealed class SystemAccountsService(PosDbContext db)
{
    public const string VaultBaseScope = "vault_base";
    public const string ShiftSessionScope = "shift_session";
    public const string ShiftSessionMainAccountKey = "session_main";

    private sealed record AccountBlueprint(string Method, string DisplayName, string Type);

    private static readonly AccountBlueprint[] Blueprints =
    [
        new("cash", "Cash", "cash"),
        new("card", "Card", "bank"),
        new("cheque", "Cheque", "bank"),
        new("debt", "Debt", "debt"),
    ];

    public static string NormalizeMethod(string? method)
    {
        var normalized = (method ?? string.Empty).Trim().ToLowerInvariant();
        return normalized switch
        {
            "bank" => "card",
            _ => normalized,
        };
    }

    public async Task<string> GetDefaultCurrencyCode(CancellationToken ct)
    {
        var raw = await db.AppSettings
            .AsNoTracking()
            .Where(x => x.Key == PosSettingKeys.DefaultCurrencyCode)
            .Select(x => x.Value)
            .FirstOrDefaultAsync(ct);

        return NormalizeCurrency(raw) ?? "ILS";
    }

    public async Task<Dictionary<string, PosAccount>> EnsureVaultBaseAccounts(DateTime now, CancellationToken ct)
    {
        var defaultCurrency = await GetDefaultCurrencyCode(ct);
        return await EnsureVaultBaseAccounts(defaultCurrency, now, ct);
    }

    public async Task<Dictionary<string, PosAccount>> EnsureShiftSessionAccounts(PosShift shift, DateTime now, CancellationToken ct)
    {
        var defaultCurrency = await GetDefaultCurrencyCode(ct);
        var vaultByMethod = await EnsureVaultBaseAccounts(defaultCurrency, now, ct);

        var sessionAccounts = await db.Accounts
            .Where(x => x.AccountScope == ShiftSessionScope && x.ShiftId == shift.Id)
            .ToListAsync(ct);

        var shiftMainAccount = sessionAccounts.FirstOrDefault(x =>
            string.Equals(x.AccountKey, ShiftSessionMainAccountKey, StringComparison.OrdinalIgnoreCase));
        if (shiftMainAccount is null)
        {
            shiftMainAccount = new PosAccount
            {
                Id = Guid.NewGuid(),
                Name = $"Shift {shift.OpenedAt:yyyyMMdd-HHmm} Session",
                Type = "cash",
                Currency = defaultCurrency,
                IsActive = true,
                AccountScope = ShiftSessionScope,
                AccountKey = ShiftSessionMainAccountKey,
                IsSystem = true,
                IsLocked = true,
                ShiftId = shift.Id,
                BaseAccountId = null,
                ParentAccountId = null,
                CreatedAt = now,
            };
            db.Accounts.Add(shiftMainAccount);
            sessionAccounts.Add(shiftMainAccount);
        }
        else
        {
            shiftMainAccount.Name = $"Shift {shift.OpenedAt:yyyyMMdd-HHmm} Session";
            shiftMainAccount.Type = "cash";
            shiftMainAccount.Currency = defaultCurrency;
            shiftMainAccount.IsActive = true;
            shiftMainAccount.AccountScope = ShiftSessionScope;
            shiftMainAccount.AccountKey = ShiftSessionMainAccountKey;
            shiftMainAccount.IsSystem = true;
            shiftMainAccount.IsLocked = true;
            shiftMainAccount.ShiftId = shift.Id;
            shiftMainAccount.BaseAccountId = null;
            shiftMainAccount.ParentAccountId = null;
        }

        var byMethod = new Dictionary<string, PosAccount>(StringComparer.OrdinalIgnoreCase);
        foreach (var blueprint in Blueprints)
        {
            var account = sessionAccounts.FirstOrDefault(x => string.Equals(x.AccountKey, blueprint.Method, StringComparison.OrdinalIgnoreCase));
            if (account is null)
            {
                account = new PosAccount
                {
                    Id = Guid.NewGuid(),
                    Name = $"Shift {shift.OpenedAt:yyyyMMdd-HHmm} {blueprint.DisplayName}",
                    Type = blueprint.Type,
                    Currency = defaultCurrency,
                    IsActive = true,
                    AccountScope = ShiftSessionScope,
                    AccountKey = blueprint.Method,
                    IsSystem = true,
                    IsLocked = true,
                    ShiftId = shift.Id,
                    BaseAccountId = vaultByMethod[blueprint.Method].Id,
                    ParentAccountId = shiftMainAccount.Id,
                    CreatedAt = now,
                };
                db.Accounts.Add(account);
                sessionAccounts.Add(account);
            }
            else
            {
                account.Type = blueprint.Type;
                account.Currency = defaultCurrency;
                account.IsActive = true;
                account.AccountScope = ShiftSessionScope;
                account.AccountKey = blueprint.Method;
                account.IsSystem = true;
                account.IsLocked = true;
                account.ShiftId = shift.Id;
                account.BaseAccountId = vaultByMethod[blueprint.Method].Id;
                account.ParentAccountId = shiftMainAccount.Id;
            }

            byMethod[blueprint.Method] = account;
        }

        return byMethod;
    }

    public async Task<PosAccount?> ResolveAccountForPayment(Guid? shiftId, string method, CancellationToken ct)
    {
        var normalizedMethod = NormalizeMethod(method);
        if (string.IsNullOrWhiteSpace(normalizedMethod))
        {
            return null;
        }

        if (shiftId.HasValue)
        {
            var sessionAccount = await db.Accounts
                .FirstOrDefaultAsync(x =>
                    x.AccountScope == ShiftSessionScope &&
                    x.ShiftId == shiftId.Value &&
                    x.IsActive &&
                    x.AccountKey == normalizedMethod,
                    ct);
            if (sessionAccount is not null)
            {
                return sessionAccount;
            }
        }

        var vaultAccount = await db.Accounts
            .FirstOrDefaultAsync(x =>
                x.AccountScope == VaultBaseScope &&
                x.IsActive &&
                x.AccountKey == normalizedMethod,
                ct);
        if (vaultAccount is not null)
        {
            return vaultAccount;
        }

        var mappedAccountId = await db.PaymentMethodAccounts
            .AsNoTracking()
            .Where(x => x.Method.ToLower() == normalizedMethod)
            .Select(x => x.AccountId)
            .FirstOrDefaultAsync(ct);
        if (mappedAccountId == Guid.Empty)
        {
            return null;
        }

        return await db.Accounts.FirstOrDefaultAsync(x => x.Id == mappedAccountId && x.IsActive, ct);
    }

    public async Task<List<ShiftMergeEntry>> MergeShiftAccountsToVault(Guid shiftId, Guid userId, DateTime now, CancellationToken ct)
    {
        var sessionAccounts = await db.Accounts
            .Where(x => x.AccountScope == ShiftSessionScope && x.ShiftId == shiftId)
            .OrderBy(x => x.CreatedAt)
            .ToListAsync(ct);

        if (sessionAccounts.Count == 0)
        {
            return [];
        }

        var vaultByMethod = await EnsureVaultBaseAccounts(now, ct);
        var result = new List<ShiftMergeEntry>();

        foreach (var session in sessionAccounts)
        {
            var method = NormalizeMethod(session.AccountKey);
            if (string.IsNullOrWhiteSpace(method))
            {
                session.IsActive = false;
                continue;
            }

            if (!vaultByMethod.TryGetValue(method, out var vault))
            {
                session.IsActive = false;
                continue;
            }

            var balance = await db.AccountTransactions
                .Where(x => x.AccountId == session.Id)
                .SumAsync(x => x.Direction == "in" ? x.Amount : -x.Amount, ct);

            if (Math.Abs(balance) < 0.0001m)
            {
                session.IsActive = false;
                continue;
            }

            var amount = Math.Abs(balance);
            var fromAccount = balance >= 0 ? session : vault;
            var toAccount = balance >= 0 ? vault : session;

            var transfer = new PosAccountTransfer
            {
                Id = Guid.NewGuid(),
                FromAccountId = fromAccount.Id,
                ToAccountId = toAccount.Id,
                Amount = amount,
                Note = $"Shift close merge ({method})",
                CreatedBy = userId,
                CreatedAt = now,
            };
            db.AccountTransfers.Add(transfer);

            db.AccountTransactions.Add(new PosAccountTransaction
            {
                Id = Guid.NewGuid(),
                AccountId = fromAccount.Id,
                Direction = "out",
                Amount = amount,
                SourceType = "shift_close_merge",
                SourceId = transfer.Id,
                Note = $"Shift close merge ({method})",
                CreatedBy = userId,
                CreatedAt = now,
            });

            db.AccountTransactions.Add(new PosAccountTransaction
            {
                Id = Guid.NewGuid(),
                AccountId = toAccount.Id,
                Direction = "in",
                Amount = amount,
                SourceType = "shift_close_merge",
                SourceId = transfer.Id,
                Note = $"Shift close merge ({method})",
                CreatedBy = userId,
                CreatedAt = now,
            });

            session.IsActive = false;

            result.Add(new ShiftMergeEntry(method, fromAccount.Id, toAccount.Id, amount));
        }

        return result;
    }

    private async Task<Dictionary<string, PosAccount>> EnsureVaultBaseAccounts(string currency, DateTime now, CancellationToken ct)
    {
        var vaultAccounts = await db.Accounts
            .Where(x => x.AccountScope == VaultBaseScope)
            .ToListAsync(ct);

        var byMethod = new Dictionary<string, PosAccount>(StringComparer.OrdinalIgnoreCase);
        foreach (var blueprint in Blueprints)
        {
            var account = vaultAccounts.FirstOrDefault(x => string.Equals(x.AccountKey, blueprint.Method, StringComparison.OrdinalIgnoreCase));
            if (account is null)
            {
                account = new PosAccount
                {
                    Id = Guid.NewGuid(),
                    Name = $"Vault {blueprint.DisplayName}",
                    Type = blueprint.Type,
                    Currency = currency,
                    IsActive = true,
                    AccountScope = VaultBaseScope,
                    AccountKey = blueprint.Method,
                    IsSystem = true,
                    IsLocked = true,
                    ShiftId = null,
                    BaseAccountId = null,
                    CreatedAt = now,
                };
                db.Accounts.Add(account);
                vaultAccounts.Add(account);
            }
            else
            {
                account.Name = $"Vault {blueprint.DisplayName}";
                account.Type = blueprint.Type;
                account.Currency = currency;
                account.IsActive = true;
                account.AccountScope = VaultBaseScope;
                account.AccountKey = blueprint.Method;
                account.IsSystem = true;
                account.IsLocked = true;
                account.ShiftId = null;
                account.BaseAccountId = null;
            }

            byMethod[blueprint.Method] = account;
        }

        return byMethod;
    }

    private static string? NormalizeCurrency(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        return raw.Trim().ToUpperInvariant();
    }
}

public sealed record ShiftMergeEntry(string Method, Guid FromAccountId, Guid ToAccountId, decimal Amount);
