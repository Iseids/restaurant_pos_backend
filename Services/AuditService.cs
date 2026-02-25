using Microsoft.EntityFrameworkCore;
using ResPosBackend.Data;

namespace ResPosBackend.Services;

public sealed class AuditService(PosDbContext db)
{
    public async Task WriteAuditLog(
        Guid? userId,
        string? username,
        string? role,
        string method,
        string path,
        int statusCode,
        string? requestBody,
        string? responseBody,
        CancellationToken ct)
    {
        var entity = new Models.PosAuditLog
        {
            Id = Guid.NewGuid(),
            UserId = userId,
            Username = string.IsNullOrWhiteSpace(username) ? null : username.Trim(),
            Role = string.IsNullOrWhiteSpace(role) ? null : role.Trim(),
            Method = method.Trim().ToUpperInvariant(),
            Path = path.Trim(),
            StatusCode = statusCode,
            RequestBody = NormalizeBody(requestBody),
            ResponseBody = NormalizeBody(responseBody),
            CreatedAt = DateTime.UtcNow,
        };

        db.AuditLogs.Add(entity);
        await db.SaveChangesAsync(ct);
    }

    public async Task<List<object>> ListAuditLogs(int limit, CancellationToken ct)
    {
        var safeLimit = Math.Clamp(limit, 1, 500);
        var rows = await db.AuditLogs
            .AsNoTracking()
            .OrderByDescending(x => x.CreatedAt)
            .Take(safeLimit)
            .ToListAsync(ct);

        return rows.Select(x => (object)new
        {
            id = x.Id,
            userId = x.UserId,
            username = x.Username,
            role = x.Role,
            method = x.Method,
            path = x.Path,
            statusCode = x.StatusCode,
            requestBody = x.RequestBody,
            responseBody = x.ResponseBody,
            createdAt = x.CreatedAt,
        }).ToList();
    }

    private static string? NormalizeBody(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var trimmed = value.Trim();
        if (trimmed == "<empty>")
        {
            return null;
        }

        return trimmed;
    }
}
