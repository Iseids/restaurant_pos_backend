using ResPosBackend.Models;

namespace ResPosBackend.Infrastructure;

public static class AuthContextExtensions
{
    private const string UserKey = "auth_user";

    public static void SetAuthedUser(this HttpContext ctx, AuthedUser user)
    {
        ctx.Items[UserKey] = user;
    }

    public static AuthedUser? GetAuthedUser(this HttpContext ctx)
    {
        return ctx.Items.TryGetValue(UserKey, out var value) ? value as AuthedUser : null;
    }

    public static IResult? RequireMinRole(this HttpContext ctx, PosRole role, out AuthedUser? user)
    {
        user = ctx.GetAuthedUser();
        if (user is null)
        {
            return ApiResults.Error(StatusCodes.Status401Unauthorized, "UNAUTHORIZED", "Missing or invalid token");
        }

        if ((int)user.Role < (int)role)
        {
            return ApiResults.Error(StatusCodes.Status403Forbidden, "FORBIDDEN", "Insufficient role");
        }

        return null;
    }
}
