using Microsoft.EntityFrameworkCore;
using PosBackend.AspNet.Data;
using PosBackend.AspNet.Models;
using System.Security.Cryptography;

namespace PosBackend.AspNet.Services;

public sealed class AuthService(PosDbContext db)
{
    public async Task<(string Token, DateTime ExpiresAt, AuthedUser User)?> LoginAsync(string username, string password, CancellationToken ct)
    {
        var user = await db.Users
            .AsNoTracking()
            .FirstOrDefaultAsync(x => x.Username == username && x.IsActive, ct);

        if (user is null)
        {
            return null;
        }

        if (!BCrypt.Net.BCrypt.Verify(password, user.PasswordHash))
        {
            return null;
        }

        var token = CreateToken();
        var expiresAt = DateTime.UtcNow.AddHours(12);

        db.Sessions.Add(new PosSession
        {
            Token = token,
            UserId = user.Id,
            ExpiresAt = expiresAt,
        });
        await db.SaveChangesAsync(ct);

        return (token, expiresAt, new AuthedUser(user.Id, user.Username, PosRoleExtensions.Parse(user.Role)));
    }

    public async Task<AuthedUser?> AuthenticateTokenAsync(string token, CancellationToken ct)
    {
        var session = await db.Sessions
            .AsNoTracking()
            .Include(x => x.User)
            .FirstOrDefaultAsync(x => x.Token == token && x.ExpiresAt > DateTime.UtcNow, ct);

        if (session?.User is null || !session.User.IsActive)
        {
            return null;
        }

        return new AuthedUser(session.User.Id, session.User.Username, PosRoleExtensions.Parse(session.User.Role));
    }

    private static string CreateToken()
    {
        var bytes = RandomNumberGenerator.GetBytes(32);
        return Convert.ToBase64String(bytes)
            .Replace('+', '-')
            .Replace('/', '_')
            .TrimEnd('=');
    }
}
