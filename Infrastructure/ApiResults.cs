namespace ResPosBackend.Infrastructure;

public static class ApiResults
{
    public static IResult Ok(object body, int statusCode = StatusCodes.Status200OK)
    {
        return Results.Json(body, statusCode: statusCode);
    }

    public static IResult Error(int statusCode, string code, string message)
    {
        return Results.Json(new
        {
            error = new
            {
                code,
                message,
            },
        }, statusCode: statusCode);
    }
}
