using Npgsql;

namespace ResPosBackend.Infrastructure;

public static class DatabaseConnectionString
{
    public static string Build(IConfiguration config)
    {
        var url = config["POS_DATABASE_URL"];
        if (!string.IsNullOrWhiteSpace(url))
        {
            return BuildFromUrl(url!);
        }

        var host = config["POS_PG_HOST"] ?? "localhost";
        var port = int.TryParse(config["POS_PG_PORT"], out var parsedPort) ? parsedPort : 5432;
        var database = config["POS_PG_DB"] ?? "restaurant_pos";
        var username = config["POS_PG_USER"] ?? "postgres";
        var password = config["POS_PG_PASS"] ?? "dev";

        var b = new NpgsqlConnectionStringBuilder
        {
            Host = host,
            Port = port,
            Database = database,
            Username = username,
            Password = password,
            Pooling = true,
            MaxPoolSize = int.TryParse(config["POS_PG_MAXCONN"], out var maxConn) ? maxConn : 20,
        };

        return b.ConnectionString;
    }

    private static string BuildFromUrl(string url)
    {
        var uri = new Uri(url);

        var userInfo = uri.UserInfo.Split(':', 2);
        var username = userInfo.Length > 0 ? Uri.UnescapeDataString(userInfo[0]) : "postgres";
        var password = userInfo.Length > 1 ? Uri.UnescapeDataString(userInfo[1]) : string.Empty;

        var database = uri.AbsolutePath.Trim('/');
        if (string.IsNullOrWhiteSpace(database))
        {
            database = "restaurant_pos";
        }

        var sslMode = (uri.Query ?? string.Empty).Contains("sslmode=require", StringComparison.OrdinalIgnoreCase)
            ? SslMode.Require
            : SslMode.Disable;

        var b = new NpgsqlConnectionStringBuilder
        {
            Host = string.IsNullOrWhiteSpace(uri.Host) ? "localhost" : uri.Host,
            Port = uri.IsDefaultPort ? 5432 : uri.Port,
            Database = database,
            Username = username,
            Password = password,
            Pooling = true,
            SslMode = sslMode,
        };

        return b.ConnectionString;
    }
}
