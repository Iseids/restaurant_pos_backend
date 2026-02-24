using Microsoft.EntityFrameworkCore;
using ResPosBackend.Models;

namespace ResPosBackend.Data;

public sealed class DevelopmentDataSeeder(
    PosDbContext db,
    IConfiguration config,
    ILogger<DevelopmentDataSeeder> logger)
{
    public async Task SeedAsync(CancellationToken ct)
    {
        await db.Database.MigrateAsync(ct);

        var now = DateTime.UtcNow;
        var changed = false;

        changed |= await SeedUsers(now, ct);
        changed |= await SeedTables(now, ct);

        var kitchenPrinter = await EnsureKitchenPrinter(now, ct);
        changed |= kitchenPrinter.Created;

        var categories = await EnsureCategories(kitchenPrinter.Entity.Id, now, ct);
        changed |= categories.Created;

        changed |= await SeedMenuItems(categories.Entities, now, ct);
        changed |= await SeedAccountsAndPaymentMappings(now, ct);

        if (changed)
        {
            await db.SaveChangesAsync(ct);
            logger.LogInformation("Development seed applied successfully.");
        }
        else
        {
            logger.LogInformation("Development seed skipped (data already present).");
        }
    }

    private async Task<bool> SeedUsers(DateTime now, CancellationToken ct)
    {
        var changed = false;
        var users = await db.Users.ToListAsync(ct);

        var adminUsername = (config["POS_SEED_ADMIN_USERNAME"] ?? "admin").Trim();
        if (string.IsNullOrWhiteSpace(adminUsername))
        {
            adminUsername = "admin";
        }

        var adminPassword = config["POS_SEED_ADMIN_PASSWORD"] ?? "admin123";

        changed |= EnsureUser(users, adminUsername, adminPassword, "admin", now);
        changed |= EnsureUser(users, "cashier", "cashier123", "cashier", now);
        changed |= EnsureUser(users, "service", "service123", "service", now);

        return changed;
    }

    private bool EnsureUser(List<PosUser> users, string username, string password, string role, DateTime now)
    {
        var existing = users.FirstOrDefault(x => string.Equals(x.Username, username, StringComparison.OrdinalIgnoreCase));
        if (existing is not null)
        {
            if (!existing.IsActive)
            {
                existing.IsActive = true;
                return true;
            }

            return false;
        }

        users.Add(new PosUser
        {
            Id = Guid.NewGuid(),
            Username = username,
            PasswordHash = BCrypt.Net.BCrypt.HashPassword(password),
            Role = role,
            IsActive = true,
            CreatedAt = now,
        });

        db.Users.Add(users[^1]);
        return true;
    }

    private async Task<bool> SeedTables(DateTime now, CancellationToken ct)
    {
        var changed = false;
        var tables = await db.Tables.ToListAsync(ct);

        for (var i = 1; i <= 10; i++)
        {
            var name = $"T{i}";
            var existing = tables.FirstOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase));
            if (existing is not null)
            {
                continue;
            }

            var entity = new PosTable
            {
                Id = Guid.NewGuid(),
                Name = name,
                SortOrder = i,
                IsActive = true,
                CreatedAt = now,
            };

            db.Tables.Add(entity);
            tables.Add(entity);
            changed = true;
        }

        return changed;
    }

    private async Task<(PosPrinter Entity, bool Created)> EnsureKitchenPrinter(DateTime now, CancellationToken ct)
    {
        var printers = await db.Printers.ToListAsync(ct);
        var existing = printers.FirstOrDefault(x => string.Equals(x.Name, "Kitchen Printer", StringComparison.OrdinalIgnoreCase));
        if (existing is not null)
        {
            return (existing, false);
        }

        var entity = new PosPrinter
        {
            Id = Guid.NewGuid(),
            Name = "Kitchen Printer",
            Type = "network",
            Address = "127.0.0.1:9100",
            IsActive = true,
            CreatedAt = now,
        };

        db.Printers.Add(entity);
        return (entity, true);
    }

    private async Task<(List<PosCategory> Entities, bool Created)> EnsureCategories(Guid kitchenPrinterId, DateTime now, CancellationToken ct)
    {
        var categories = await db.Categories.ToListAsync(ct);
        var created = false;

        created |= EnsureCategory(categories, "Starters", 10, kitchenPrinterId, now);
        created |= EnsureCategory(categories, "Main Course", 20, kitchenPrinterId, now);
        created |= EnsureCategory(categories, "Drinks", 30, null, now);

        return (categories, created);
    }

    private bool EnsureCategory(List<PosCategory> categories, string name, int sortOrder, Guid? printerId, DateTime now)
    {
        var existing = categories.FirstOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase));
        if (existing is not null)
        {
            return false;
        }

        var entity = new PosCategory
        {
            Id = Guid.NewGuid(),
            Name = name,
            SortOrder = sortOrder,
            PrinterId = printerId,
            ParentId = null,
            ImageUrl = null,
            IsActive = true,
            CreatedAt = now,
        };

        db.Categories.Add(entity);
        categories.Add(entity);
        return true;
    }

    private async Task<bool> SeedMenuItems(List<PosCategory> categories, DateTime now, CancellationToken ct)
    {
        var changed = false;
        var menuItems = await db.MenuItems.ToListAsync(ct);

        var starters = categories.FirstOrDefault(x => string.Equals(x.Name, "Starters", StringComparison.OrdinalIgnoreCase));
        var mains = categories.FirstOrDefault(x => string.Equals(x.Name, "Main Course", StringComparison.OrdinalIgnoreCase));
        var drinks = categories.FirstOrDefault(x => string.Equals(x.Name, "Drinks", StringComparison.OrdinalIgnoreCase));

        if (starters is not null)
        {
            changed |= EnsureMenuItem(menuItems, starters.Id, "French Fries", 3.50m, now);
        }

        if (mains is not null)
        {
            changed |= EnsureMenuItem(menuItems, mains.Id, "Classic Burger", 8.90m, now);
        }

        if (drinks is not null)
        {
            changed |= EnsureMenuItem(menuItems, drinks.Id, "Water", 1.00m, now);
            changed |= EnsureMenuItem(menuItems, drinks.Id, "Cola", 2.50m, now);
        }

        return changed;
    }

    private bool EnsureMenuItem(List<PosMenuItem> menuItems, Guid categoryId, string name, decimal price, DateTime now)
    {
        var existing = menuItems.FirstOrDefault(x =>
            x.CategoryId == categoryId &&
            string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase));

        if (existing is not null)
        {
            return false;
        }

        var entity = new PosMenuItem
        {
            Id = Guid.NewGuid(),
            CategoryId = categoryId,
            Name = name,
            Price = price,
            StockQty = 100m,
            ImageUrl = null,
            IsActive = true,
            CreatedAt = now,
        };

        db.MenuItems.Add(entity);
        menuItems.Add(entity);
        return true;
    }

    private async Task<bool> SeedAccountsAndPaymentMappings(DateTime now, CancellationToken ct)
    {
        var changed = false;
        var accounts = await db.Accounts.ToListAsync(ct);

        var cashAccount = accounts.FirstOrDefault(x => string.Equals(x.Name, "Cash Register", StringComparison.OrdinalIgnoreCase));
        if (cashAccount is null)
        {
            cashAccount = new PosAccount
            {
                Id = Guid.NewGuid(),
                Name = "Cash Register",
                Type = "cash",
                Currency = "ILS",
                IsActive = true,
                CreatedAt = now,
            };
            db.Accounts.Add(cashAccount);
            accounts.Add(cashAccount);
            changed = true;
        }

        var cardAccount = accounts.FirstOrDefault(x => string.Equals(x.Name, "Card Terminal", StringComparison.OrdinalIgnoreCase));
        if (cardAccount is null)
        {
            cardAccount = new PosAccount
            {
                Id = Guid.NewGuid(),
                Name = "Card Terminal",
                Type = "bank",
                Currency = "ILS",
                IsActive = true,
                CreatedAt = now,
            };
            db.Accounts.Add(cardAccount);
            accounts.Add(cardAccount);
            changed = true;
        }

        var mappings = await db.PaymentMethodAccounts.ToListAsync(ct);
        changed |= EnsurePaymentMethodMapping(mappings, "cash", cashAccount.Id);
        changed |= EnsurePaymentMethodMapping(mappings, "card", cardAccount.Id);
        changed |= EnsurePaymentMethodMapping(mappings, "bank", cardAccount.Id);

        return changed;
    }

    private bool EnsurePaymentMethodMapping(List<PosPaymentMethodAccount> mappings, string method, Guid accountId)
    {
        var existing = mappings.FirstOrDefault(x => string.Equals(x.Method, method, StringComparison.OrdinalIgnoreCase));
        if (existing is not null)
        {
            if (existing.AccountId != accountId)
            {
                existing.AccountId = accountId;
                return true;
            }

            return false;
        }

        var entity = new PosPaymentMethodAccount
        {
            Method = method,
            AccountId = accountId,
        };

        db.PaymentMethodAccounts.Add(entity);
        mappings.Add(entity);
        return true;
    }
}
