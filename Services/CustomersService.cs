using Microsoft.EntityFrameworkCore;
using Npgsql;
using PosBackend.AspNet.Data;
using PosBackend.AspNet.Models;

namespace PosBackend.AspNet.Services;

public sealed class CustomersService(PosDbContext db)
{
    public async Task<List<object>> ListCustomers(string? query, int limit, CancellationToken ct)
    {
        var q = (query ?? string.Empty).Trim().ToLowerInvariant();

        var items = await db.Customers
            .AsNoTracking()
            .Where(x => string.IsNullOrEmpty(q)
                || x.Name.ToLower().Contains(q)
                || (x.Phone ?? string.Empty).ToLower().Contains(q))
            .OrderBy(x => x.Name)
            .Take(Math.Clamp(limit, 1, 200))
            .ToListAsync(ct);

        return items.Select(ToDto).ToList();
    }

    public async Task<object> CreateCustomer(string name, string? phone, CancellationToken ct)
    {
        var customer = new PosCustomer
        {
            Id = Guid.NewGuid(),
            Name = name,
            Phone = string.IsNullOrWhiteSpace(phone) ? null : phone.Trim(),
            DiscountPercent = 0,
            IsActive = true,
        };

        db.Customers.Add(customer);
        try
        {
            await db.SaveChangesAsync(ct);
        }
        catch (DbUpdateException ex) when (ex.InnerException is PostgresException pg && pg.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            throw new InvalidOperationException("CONFLICT");
        }

        return ToDto(customer);
    }

    public async Task<object?> UpdateBasic(Guid id, string? name, string? phone, bool? isActive, CancellationToken ct)
    {
        var customer = await db.Customers.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (customer is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(name))
        {
            customer.Name = name.Trim();
        }

        if (phone is not null)
        {
            customer.Phone = string.IsNullOrWhiteSpace(phone) ? null : phone.Trim();
        }

        if (isActive.HasValue)
        {
            customer.IsActive = isActive.Value;
        }

        try
        {
            await db.SaveChangesAsync(ct);
        }
        catch (DbUpdateException ex) when (ex.InnerException is PostgresException pg && pg.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            throw new InvalidOperationException("CONFLICT");
        }

        return ToDto(customer);
    }

    public async Task<object?> SetDiscount(Guid id, double discountPercent, CancellationToken ct)
    {
        if (discountPercent < 0 || discountPercent > 100)
        {
            throw new InvalidOperationException("DISCOUNT_OUT_OF_RANGE");
        }

        var customer = await db.Customers.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (customer is null)
        {
            return null;
        }

        customer.DiscountPercent = (decimal)discountPercent;
        await db.SaveChangesAsync(ct);

        return ToDto(customer);
    }

    private static object ToDto(PosCustomer x)
    {
        return new
        {
            id = x.Id,
            name = x.Name,
            phone = x.Phone,
            discountPercent = (double)x.DiscountPercent,
            isActive = x.IsActive,
        };
    }
}
