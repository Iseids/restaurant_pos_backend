namespace ResPosBackend.Models;

public enum PosRole
{
    Service = 1,
    Cashier = 2,
    Admin = 3,
}

public static class PosRoleExtensions
{
    public static PosRole Parse(string? raw)
    {
        return raw?.Trim().ToLowerInvariant() switch
        {
            "service" => PosRole.Service,
            "cashier" => PosRole.Cashier,
            "admin" => PosRole.Admin,
            _ => PosRole.Service,
        };
    }

    public static string DbValue(this PosRole role)
    {
        return role switch
        {
            PosRole.Service => "service",
            PosRole.Cashier => "cashier",
            PosRole.Admin => "admin",
            _ => "service",
        };
    }
}

public sealed record AuthedUser(Guid Id, string Username, PosRole Role)
{
    public object ToJson()
    {
        return new
        {
            id = Id,
            username = Username,
            role = Role.DbValue(),
            roleLevel = (int)Role,
        };
    }
}
