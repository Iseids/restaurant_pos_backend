using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using ResPosBackend.Data;
using ResPosBackend.Infrastructure;
using ResPosBackend.Models;
using ResPosBackend.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration.AddEnvironmentVariables();

var connectionString = DatabaseConnectionString.Build(builder.Configuration);

builder.Services.AddDbContext<PosDbContext>(opt => opt.UseNpgsql(connectionString));
builder.Services.AddHttpClient();
builder.Services.AddScoped<AuthService>();
builder.Services.AddScoped<MenuService>();
builder.Services.AddScoped<TablesService>();
builder.Services.AddScoped<OrdersService>();
builder.Services.AddScoped<CustomersService>();
builder.Services.AddScoped<ShiftsService>();
builder.Services.AddScoped<PrintService>();
builder.Services.AddScoped<AdminService>();
builder.Services.AddScoped<AccountingService>();
builder.Services.AddScoped<ReportsService>();
builder.Services.AddScoped<PayrollService>();
builder.Services.AddScoped<AuditService>();
builder.Services.AddScoped<SystemAccountsService>();
builder.Services.AddScoped<DevelopmentDataSeeder>();

builder.Services.AddCors(opt =>
{
    opt.AddPolicy("pos", p => p
        .AllowAnyOrigin()
        .AllowAnyMethod()
        .AllowAnyHeader());
});

var app = builder.Build();

await EnsureOrderNicknameColumnAsync(app, app.Lifetime.ApplicationStopping);
await EnsureAuditLogsTableAsync(app, app.Lifetime.ApplicationStopping);
await EnsureAccountingLinksColumnsAsync(app, app.Lifetime.ApplicationStopping);
await EnsureSystemAccountsColumnsAsync(app, app.Lifetime.ApplicationStopping);
await EnsureSystemAccountsBootstrapAsync(app, app.Lifetime.ApplicationStopping);

if (app.Environment.IsDevelopment())
{
    using var scope = app.Services.CreateScope();
    var seeder = scope.ServiceProvider.GetRequiredService<DevelopmentDataSeeder>();
    await seeder.SeedAsync(app.Lifetime.ApplicationStopping);
}

app.UseCors("pos");

app.Use(async (ctx, next) =>
{
    var path = $"{ctx.Request.Path}{ctx.Request.QueryString}";
    var requestBody = await ReadRequestBodyForLog(ctx.Request);

    await using var responseBuffer = new MemoryStream();
    var originalResponseBody = ctx.Response.Body;
    ctx.Response.Body = responseBuffer;

    Exception? pipelineError = null;
    try
    {
        await next();
    }
    catch (Exception ex)
    {
        pipelineError = ex;
    }
    finally
    {
        var responseBody = await ReadResponseBodyForLog(responseBuffer, ctx.Response.ContentType);

        if (pipelineError is null)
        {
            responseBuffer.Position = 0;
            await responseBuffer.CopyToAsync(originalResponseBody, ctx.RequestAborted);
        }

        ctx.Response.Body = originalResponseBody;

        app.Logger.LogInformation(
            """
            HTTP {Method} {Path}
            Request: {RequestBody}
            Response: {StatusCode} {ResponseBody}
            """,
            ctx.Request.Method,
            path,
            TruncateForLog(requestBody),
            ctx.Response.StatusCode,
            TruncateForLog(responseBody));

        if (ShouldWriteAuditLog(ctx.Request.Method, ctx.Request.Path))
        {
            try
            {
                var user = ctx.GetAuthedUser();
                var audit = ctx.RequestServices.GetRequiredService<AuditService>();
                await audit.WriteAuditLog(
                    userId: user?.Id,
                    username: user?.Username,
                    role: user?.Role.DbValue(),
                    method: ctx.Request.Method,
                    path: path,
                    statusCode: pipelineError is null
                        ? ctx.Response.StatusCode
                        : StatusCodes.Status500InternalServerError,
                    requestBody: BuildAuditableBody(requestBody),
                    responseBody: BuildAuditableBody(responseBody),
                    ct: ctx.RequestAborted);
            }
            catch (Exception ex)
            {
                app.Logger.LogWarning(ex, "Failed to persist audit log for {Method} {Path}", ctx.Request.Method, path);
            }
        }
    }

    if (pipelineError is not null)
    {
        throw pipelineError;
    }
});

app.Use(async (ctx, next) =>
{
    if (HttpMethods.IsOptions(ctx.Request.Method))
    {
        ctx.Response.StatusCode = StatusCodes.Status200OK;
        return;
    }

    var authHeader = ctx.Request.Headers.Authorization.ToString();
    if (authHeader.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
    {
        var token = authHeader["Bearer ".Length..].Trim();
        if (!string.IsNullOrWhiteSpace(token))
        {
            var auth = ctx.RequestServices.GetRequiredService<AuthService>();
            var user = await auth.AuthenticateTokenAsync(token, ctx.RequestAborted);
            if (user is not null)
            {
                ctx.SetAuthedUser(user);
            }
        }
    }

    await next();
});

app.MapGet("/api/health", () => ApiResults.Ok(new { ok = true }));

app.MapPost("/api/auth/login", async (HttpRequest req, AuthService auth, CancellationToken ct) =>
{
    var payload = await ReadJsonMap(req, ct);
    var username = GetString(payload, "username")?.Trim() ?? string.Empty;
    var password = GetString(payload, "password")?.Trim() ?? string.Empty;

    if (string.IsNullOrEmpty(username) || string.IsNullOrEmpty(password))
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "username/password required");
    }

    var result = await auth.LoginAsync(username, password, ct);
    if (result is null)
    {
        return ApiResults.Error(StatusCodes.Status401Unauthorized, "UNAUTHORIZED", "Invalid credentials");
    }

    return ApiResults.Ok(new
    {
        token = result.Value.Token,
        expiresAt = result.Value.ExpiresAt,
        user = result.Value.User.ToJson(),
    });
});

app.MapGet("/api/auth/me", (HttpContext ctx) =>
{
    var user = ctx.GetAuthedUser();
    if (user is null)
    {
        return ApiResults.Error(StatusCodes.Status401Unauthorized, "UNAUTHORIZED", "Missing or invalid token");
    }

    return ApiResults.Ok(new { user = user.ToJson() });
});

app.MapGet("/api/menu/categories", async (HttpContext ctx, MenuService menu, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out _) is { } denied)
    {
        return denied;
    }

    var items = await menu.ListActiveCategories(ct);
    return ApiResults.Ok(new { items });
});

app.MapGet("/api/menu/items", async (HttpContext ctx, HttpRequest req, MenuService menu, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out _) is { } denied)
    {
        return denied;
    }

    var categoryRaw = req.Query["categoryId"].ToString().Trim();
    if (!Guid.TryParse(categoryRaw, out var categoryId))
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "categoryId required");
    }

    var items = await menu.ListActiveItems(categoryId, ct);
    return ApiResults.Ok(new { items });
});

app.MapGet("/api/menu/items/{id:guid}/customizations", async (HttpContext ctx, Guid id, MenuService menu, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out _) is { } denied)
    {
        return denied;
    }

    var items = await menu.ListActiveCustomizations(id, ct);
    return ApiResults.Ok(new { items });
});

app.MapGet("/api/tables", async (HttpContext ctx, TablesService tables, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out _) is { } denied)
    {
        return denied;
    }

    var items = await tables.ListActiveWithOpenOrder(ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/tables/{tableId:guid}/open-order", async (HttpContext ctx, HttpRequest req, Guid tableId, OrdersService orders, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out var user) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var peopleCount = GetInt(payload, "peopleCount");

    try
    {
        var result = await orders.CreateOrGetOpenOrderForTable(user!.Id, tableId, peopleCount, ct);
        return ApiResults.Ok(new { item = result.ToJson() }, result.Existing ? StatusCodes.Status200OK : StatusCodes.Status201Created);
    }
    catch (PosRuleException ex) when (ex.Code == "SHIFT_REQUIRED")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "SHIFT_REQUIRED", "Open a shift first");
    }
});

app.MapGet("/api/orders/open", async (HttpContext ctx, OrdersService orders, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out _) is { } denied)
    {
        return denied;
    }

    var items = await orders.ListOpenOrders(ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/orders", async (HttpContext ctx, HttpRequest req, OrdersService orders, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out var user) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var tableId = GetGuid(payload, "tableId");
    var peopleCount = GetInt(payload, "peopleCount");
    var customerId = GetGuid(payload, "customerId");

    try
    {
        var created = await orders.CreateOrder(user!.Id, tableId, peopleCount, customerId, isTakeaway: false, ct);
        return ApiResults.Ok(new { item = created.ToJson() }, created.Existing ? StatusCodes.Status200OK : StatusCodes.Status201Created);
    }
    catch (PosRuleException ex) when (ex.Code == "SHIFT_REQUIRED")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "SHIFT_REQUIRED", "Open a shift first");
    }
});

app.MapPost("/api/orders/merge", async (HttpContext ctx, HttpRequest req, OrdersService orders, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out var user) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var orderIds = ParseGuidList(payload.TryGetValue("orderIds", out var idsEl) ? idsEl : default);
    var targetOrderId = GetGuid(payload, "targetOrderId");

    try
    {
        var result = await orders.MergeOrders(orderIds, user!.Id, targetOrderId, ct);
        return ApiResults.Ok(new { result = result.ToJson() });
    }
    catch (PosNotFoundException)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "One or more orders not found");
    }
    catch (PosRuleException ex) when (ex.Code == "ORDER_LOCKED")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ORDER_LOCKED", "One or more orders are already paid/closed");
    }
    catch (PosRuleException ex) when (ex.Code is "MERGE_MIN_2" or "MERGE_TARGET_INVALID")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, ex.Code, "Invalid merge request");
    }
});

app.MapGet("/api/orders/{id:guid}", async (HttpContext ctx, Guid id, OrdersService orders, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out _) is { } denied)
    {
        return denied;
    }

    var data = await orders.GetOrder(id, ct);
    if (data is null)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Order not found");
    }

    return ApiResults.Ok(data);
});

app.MapDelete("/api/orders/{id:guid}", async (HttpContext ctx, Guid id, OrdersService orders, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out var user) is { } denied)
    {
        return denied;
    }

    try
    {
        await orders.DiscardDraftOrder(id, user!.Id, ct);
        return ApiResults.Ok(new { ok = true });
    }
    catch (PosNotFoundException)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Order not found");
    }
    catch (PosRuleException ex) when (ex.Code == "ORDER_NOT_DRAFT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, ex.Code, "Only draft orders can be discarded");
    }
});

app.MapPost("/api/orders/{id:guid}/items", async (HttpContext ctx, HttpRequest req, Guid id, OrdersService orders, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out var user) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var menuItemId = GetGuid(payload, "menuItemId");
    var qty = GetDouble(payload, "qty") ?? 1;
    var note = GetString(payload, "note")?.Trim();
    var customizations = ParseCustomizations(payload.TryGetValue("customizations", out var customEl) ? customEl : default);

    if (!menuItemId.HasValue)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "menuItemId required");
    }

    try
    {
        var result = await orders.AddItem(id, user!.Id, menuItemId.Value, qty, note, customizations, ct);
        return ApiResults.Ok(result, StatusCodes.Status201Created);
    }
    catch (PosNotFoundException)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Menu item not found");
    }
    catch (PosRuleException ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, ex.Code, "Order rule violation");
    }
});

app.MapPatch("/api/orders/items/{itemId:guid}", async (HttpContext ctx, HttpRequest req, Guid itemId, OrdersService orders, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out var user) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);

    var hasDiscount = payload.ContainsKey("discountAmount") || payload.ContainsKey("discountPercent");
    if (hasDiscount && user!.Role < PosRole.Cashier)
    {
        return ApiResults.Error(StatusCodes.Status403Forbidden, "FORBIDDEN", "Cashier required for discounts");
    }

    var hasNote = payload.ContainsKey("note");
    var hasCustomizations = payload.ContainsKey("customizations");

    var patch = new ItemPatch(
        GetDouble(payload, "qty"),
        GetDouble(payload, "discountAmount"),
        GetDouble(payload, "discountPercent"),
        hasNote,
        GetString(payload, "note"),
        hasCustomizations,
        hasCustomizations ? ParseCustomizations(payload["customizations"]) : null);

    try
    {
        await orders.UpdateItem(itemId, user!.Id, patch, ct);
        return ApiResults.Ok(new { ok = true });
    }
    catch (PosNotFoundException)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Item not found");
    }
    catch (PosRuleException ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", ex.Code);
    }
});

app.MapPost("/api/orders/items/{itemId:guid}/void", async (HttpContext ctx, HttpRequest req, Guid itemId, OrdersService orders, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out var user) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var reason = GetString(payload, "reason")?.Trim() ?? string.Empty;
    if (string.IsNullOrEmpty(reason))
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "reason required");
    }

    try
    {
        await orders.VoidItem(itemId, user!.Id, reason, ct);
        return ApiResults.Ok(new { ok = true });
    }
    catch (PosNotFoundException)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Item not found");
    }
});

app.MapPatch("/api/orders/{id:guid}", async (HttpContext ctx, HttpRequest req, Guid id, OrdersService orders, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);

    var patch = new OrderPatch(
        payload.ContainsKey("tableId"),
        GetGuid(payload, "tableId"),
        GetInt(payload, "peopleCount"),
        payload.ContainsKey("customerId"),
        GetGuid(payload, "customerId"),
        GetDouble(payload, "orderDiscountAmount"),
        GetDouble(payload, "orderDiscountPercent"),
        GetDouble(payload, "serviceFeeAmount"),
        GetDouble(payload, "serviceFeePercent"),
        payload.ContainsKey("isTakeaway"),
        GetBool(payload, "isTakeaway"),
        payload.ContainsKey("nickname"),
        GetString(payload, "nickname"));

    try
    {
        await orders.UpdateOrder(id, patch, ct);
        return ApiResults.Ok(new { ok = true });
    }
    catch (PosRuleException ex) when (ex.Code == "TABLE_ALREADY_HAS_OPEN_ORDER")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, ex.Code, "Table already has an open order");
    }
    catch (PosNotFoundException)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Order not found");
    }
    catch (PosRuleException ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, ex.Code, "Order rule violation");
    }
});

app.MapPost("/api/orders/{id:guid}/send", async (HttpContext ctx, HttpRequest req, Guid id, OrdersService orders, PrintService printService, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var tableId = GetGuid(payload, "tableId");
    var isTakeaway = GetBool(payload, "isTakeaway") ?? false;
    var language = GetString(payload, "language")?.Trim();

    try
    {
        await orders.AssignOrderDestination(id, tableId, isTakeaway, ct);
        var result = await printService.PrintKitchenForOrder(id, language, ct);
        return ApiResults.Ok(new { result });
    }
    catch (PosNotFoundException)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Order not found");
    }
    catch (PosRuleException ex) when (ex.Code == "TABLE_ALREADY_HAS_OPEN_ORDER")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, ex.Code, "Table already has an open order");
    }
    catch (PosRuleException ex) when (ex.Code is "DESTINATION_REQUIRED" or "DESTINATION_CONFLICT")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, ex.Code, "Destination required (table or takeaway)");
    }
    catch (PosRuleException ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, ex.Code, "Order rule violation");
    }
});

app.MapPost("/api/orders/{id:guid}/payments", async (HttpContext ctx, HttpRequest req, Guid id, OrdersService orders, PrintService printService, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out var user) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var method = GetString(payload, "method")?.Trim() ?? string.Empty;
    var amount = GetDouble(payload, "amount");
    var reference = GetString(payload, "reference")?.Trim();
    var receiptPrinterId = GetGuid(payload, "receiptPrinterId");
    var invoicePrinterId = GetGuid(payload, "invoicePrinterId");
    var language = GetString(payload, "language")?.Trim();

    if (string.IsNullOrWhiteSpace(method) || !amount.HasValue)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "method and amount required");
    }

    try
    {
        var totals = await orders.AddPayment(id, user!.Id, method, amount.Value, reference, ct);

        var balance = totals.TryGetValue("balance", out var balanceObj) ? Convert.ToDouble(balanceObj) : 1;
        if (balance <= 0.0001)
        {
            try
            {
                await printService.PrintReceipt(id, receiptPrinterId, language, ct);
            }
            catch
            {
                // Ignore receipt print failures to preserve payment success semantics.
            }

            try
            {
                await printService.PrintInvoice(id, invoicePrinterId, language, ct);
            }
            catch
            {
                // Ignore invoice print failures to preserve payment success semantics.
            }
        }

        return ApiResults.Ok(new { totals });
    }
    catch (PosNotFoundException)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Order not found");
    }
    catch (PosRuleException ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, ex.Code, "Payment error");
    }
});

app.MapPost("/api/orders/{id:guid}/change-table", async (HttpContext ctx, HttpRequest req, Guid id, OrdersService orders, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var tableId = GetGuid(payload, "tableId");

    try
    {
        await orders.ChangeTable(id, tableId, ct);
        return ApiResults.Ok(new { ok = true });
    }
    catch (PosRuleException ex) when (ex.Code == "ORDER_LOCKED")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ORDER_LOCKED", "Order is already paid/closed");
    }
    catch (PosRuleException ex) when (ex.Code == "TABLE_ALREADY_HAS_OPEN_ORDER")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, ex.Code, "Table already has an open order");
    }
    catch (PosNotFoundException)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Order not found");
    }
});

app.MapGet("/api/orders/{id:guid}/kitchen/pending", async (HttpContext ctx, Guid id, OrdersService orders, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out _) is { } denied)
    {
        return denied;
    }

    var items = await orders.KitchenPendingByPrinter(id, ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/orders/kitchen/mark-printed", async (HttpContext ctx, HttpRequest req, OrdersService orders, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var ids = ParseGuidList(payload.TryGetValue("itemIds", out var el) ? el : default);
    await orders.MarkKitchenPrinted(ids, ct);
    return ApiResults.Ok(new { ok = true });
});

app.MapPost("/api/print/orders/{id:guid}/kitchen", async (HttpContext ctx, Guid id, PrintService printService, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out _) is { } denied)
    {
        return denied;
    }

    try
    {
        var result = await printService.PrintKitchenForOrder(id, null, ct);
        return ApiResults.Ok(new { result });
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status500InternalServerError, "PRINT_FAILED", ex.Message);
    }
});

app.MapGet("/api/print/orders/{id:guid}/invoice.pdf", async (HttpContext ctx, Guid id, PrintService printService, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    try
    {
        var bytes = await printService.BuildInvoicePdf(id, ct);
        return Results.File(bytes, "application/pdf", $"invoice-{id}.pdf");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status500InternalServerError, "INVOICE_PDF_FAILED", ex.Message);
    }
});

app.MapPost("/api/print/orders/{id:guid}/receipt", async (HttpContext ctx, HttpRequest req, Guid id, PrintService printService, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var printerId = GetGuid(payload, "printerId");
    var language = GetString(payload, "language")?.Trim();

    try
    {
        await printService.PrintReceipt(id, printerId, language, ct);
        return ApiResults.Ok(new { ok = true });
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status500InternalServerError, "RECEIPT_PRINT_FAILED", ex.Message);
    }
});

app.MapGet("/api/print/printers", async (HttpContext ctx, PrintService printService, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    var items = await printService.ListActivePrinters(ct);
    return ApiResults.Ok(new { items });
});

app.MapGet("/api/settings/printers", async (HttpContext ctx, PrintService printService, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    var item = await printService.GetRuntimePrinterSettings(ct);
    return ApiResults.Ok(new { item });
});

app.MapGet("/api/settings/cashier-expenses", async (HttpContext ctx, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    var item = await admin.GetCashierExpenseSettings(ct);
    return ApiResults.Ok(new { item });
});

app.MapGet("/api/cashier/shifts/current", async (HttpContext ctx, ShiftsService shifts, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    var item = await shifts.GetCurrentShiftSummary(ct);
    return ApiResults.Ok(new { item });
});

app.MapGet("/api/cashier/shifts/current/orders", async (HttpContext ctx, OrdersService orders, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    var item = await orders.ListCurrentShiftOrdersHistory(ct);
    return ApiResults.Ok(new { item });
});

app.MapGet("/api/cashier/shifts", async (HttpContext ctx, HttpRequest req, ShiftsService shifts, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    var limit = int.TryParse(req.Query["limit"], out var parsed) ? parsed : 20;
    var items = await shifts.ListShifts(limit, ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/cashier/shifts/open", async (HttpContext ctx, HttpRequest req, ShiftsService shifts, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out var user) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var openingCash = GetDouble(payload, "openingCash");
    var note = GetString(payload, "note")?.Trim();

    if (!openingCash.HasValue || openingCash.Value < 0)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "openingCash required");
    }

    try
    {
        var result = await shifts.OpenShift(user!.Id, openingCash.Value, note, ct);
        return ApiResults.Ok(result, StatusCodes.Status201Created);
    }
    catch (InvalidOperationException ex) when (ex.Message == "SHIFT_ALREADY_OPEN")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "CONFLICT", "Shift already open");
    }
});

app.MapPost("/api/cashier/shifts/close", async (HttpContext ctx, HttpRequest req, ShiftsService shifts, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out var user) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var shiftId = GetGuid(payload, "shiftId");
    var closingCash = GetDouble(payload, "closingCash");
    var note = GetString(payload, "note")?.Trim();

    if (!shiftId.HasValue || !closingCash.HasValue || closingCash.Value < 0)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "shiftId and closingCash required");
    }

    var result = await shifts.CloseShift(user!.Id, shiftId.Value, closingCash.Value, note, ct);
    if (result is null)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Shift not found");
    }

    return ApiResults.Ok(result);
});

app.MapGet("/api/cashier/shifts/{shiftId:guid}/summary.pdf", async (HttpContext ctx, Guid shiftId, ReportsService reports, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    try
    {
        var bytes = await reports.BuildShiftSummaryPdfBytes(shiftId, "Restaurant POS", ct);
        return Results.File(bytes, "application/pdf", $"shift-{shiftId}-summary.pdf");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "SHIFT_SUMMARY_FAILED", ex.Message);
    }
});

app.MapGet("/api/cashier/expenses", async (HttpContext ctx, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    var item = await accounting.GetCashierExpenseOverview(ct);
    return ApiResults.Ok(new { item });
});

app.MapPost("/api/cashier/expenses", async (HttpContext ctx, HttpRequest req, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out var user) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var amount = GetDouble(payload, "amount");
    var supplierId = GetGuid(payload, "supplierId");
    var employeeId = GetGuid(payload, "employeeId");
    var note = GetString(payload, "note")?.Trim();

    if (!amount.HasValue || amount.Value <= 0)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "amount required");
    }

    try
    {
        var item = await accounting.CreateCashierExpense(
            user!.Id,
            amount.Value,
            supplierId,
            employeeId,
            ParseDateOnly(payload.TryGetValue("date", out var rawDate) ? GetElementAsString(rawDate) : null),
            note,
            ct);
        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (InvalidOperationException ex) when (ex.Message == "SHIFT_REQUIRED")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "SHIFT_REQUIRED", "No open shift");
    }
    catch (InvalidOperationException ex) when (ex.Message == "CASHIER_EXPENSES_DISABLED")
    {
        return ApiResults.Error(StatusCodes.Status403Forbidden, "CASHIER_EXPENSES_DISABLED", "Cashier expenses are disabled");
    }
    catch (InvalidOperationException ex) when (ex.Message == "CASHIER_EXPENSE_CAP_EXCEEDED")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "CASHIER_EXPENSE_CAP_EXCEEDED", "Expense cap exceeded");
    }
    catch (InvalidOperationException ex) when (ex.Message == "SHIFT_CASH_ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "SHIFT_CASH_ACCOUNT_NOT_FOUND", "Shift cash account is unavailable");
    }
    catch (InvalidOperationException ex) when (ex.Message == "SHIFT_EXPENSE_ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "SHIFT_EXPENSE_ACCOUNT_NOT_FOUND", "Shift expense account is unavailable");
    }
    catch (InvalidOperationException ex) when (ex.Message == "CASHIER_EXPENSE_AMOUNT_INVALID")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "CASHIER_EXPENSE_AMOUNT_INVALID", "amount must be greater than zero");
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_EXPENSE_TARGET")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_EXPENSE_TARGET", "Choose supplierId or employeeId, not both");
    }
    catch (InvalidOperationException ex) when (ex.Message is "SUPPLIER_NOT_FOUND" or "SUPPLIER_INACTIVE")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, ex.Message, "Supplier is invalid");
    }
    catch (InvalidOperationException ex) when (ex.Message is "EMPLOYEE_NOT_FOUND" or "EMPLOYEE_INACTIVE")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, ex.Message, "Employee is invalid");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "CREATE_FAILED", ex.Message);
    }
});

app.MapPatch("/api/cashier/expenses/{expenseId:guid}", async (HttpContext ctx, HttpRequest req, Guid expenseId, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var amount = GetDouble(payload, "amount");
    var supplierId = GetGuid(payload, "supplierId");
    var employeeId = GetGuid(payload, "employeeId");
    var note = GetString(payload, "note")?.Trim();

    if (!amount.HasValue || amount.Value <= 0)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "amount required");
    }

    try
    {
        var item = await accounting.UpdateCashierExpense(
            expenseId,
            amount.Value,
            supplierId,
            employeeId,
            ParseDateOnly(payload.TryGetValue("date", out var rawDate) ? GetElementAsString(rawDate) : null),
            note,
            ct);
        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "SHIFT_REQUIRED")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "SHIFT_REQUIRED", "No open shift");
    }
    catch (InvalidOperationException ex) when (ex.Message == "SHIFT_CASH_ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "SHIFT_CASH_ACCOUNT_NOT_FOUND", "Shift cash account is unavailable");
    }
    catch (InvalidOperationException ex) when (ex.Message == "SHIFT_EXPENSE_ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "SHIFT_EXPENSE_ACCOUNT_NOT_FOUND", "Shift expense account is unavailable");
    }
    catch (InvalidOperationException ex) when (ex.Message == "CASHIER_EXPENSE_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "CASHIER_EXPENSE_NOT_FOUND", "Expense not found for current open shift");
    }
    catch (InvalidOperationException ex) when (ex.Message == "CASHIER_EXPENSE_AMOUNT_INVALID")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "CASHIER_EXPENSE_AMOUNT_INVALID", "amount must be greater than zero");
    }
    catch (InvalidOperationException ex) when (ex.Message == "CASHIER_EXPENSE_CAP_EXCEEDED")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "CASHIER_EXPENSE_CAP_EXCEEDED", "Expense cap exceeded");
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_EXPENSE_TARGET")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_EXPENSE_TARGET", "Choose supplierId or employeeId, not both");
    }
    catch (InvalidOperationException ex) when (ex.Message is "SUPPLIER_NOT_FOUND" or "SUPPLIER_INACTIVE")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, ex.Message, "Supplier is invalid");
    }
    catch (InvalidOperationException ex) when (ex.Message is "EMPLOYEE_NOT_FOUND" or "EMPLOYEE_INACTIVE")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, ex.Message, "Employee is invalid");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "UPDATE_FAILED", ex.Message);
    }
});

app.MapDelete("/api/cashier/expenses/{expenseId:guid}", async (HttpContext ctx, Guid expenseId, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    try
    {
        var item = await accounting.DeleteCashierExpense(expenseId, ct);
        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "SHIFT_REQUIRED")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "SHIFT_REQUIRED", "No open shift");
    }
    catch (InvalidOperationException ex) when (ex.Message == "SHIFT_CASH_ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "SHIFT_CASH_ACCOUNT_NOT_FOUND", "Shift cash account is unavailable");
    }
    catch (InvalidOperationException ex) when (ex.Message == "SHIFT_EXPENSE_ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "SHIFT_EXPENSE_ACCOUNT_NOT_FOUND", "Shift expense account is unavailable");
    }
    catch (InvalidOperationException ex) when (ex.Message == "CASHIER_EXPENSE_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "CASHIER_EXPENSE_NOT_FOUND", "Expense not found for current open shift");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "DELETE_FAILED", ex.Message);
    }
});

app.MapGet("/api/customers", async (HttpContext ctx, HttpRequest req, CustomersService customers, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out _) is { } denied)
    {
        return denied;
    }

    var q = req.Query["q"].ToString();
    var items = await customers.ListCustomers(q, 50, ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/customers", async (HttpContext ctx, HttpRequest req, CustomersService customers, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var name = GetString(payload, "name")?.Trim() ?? string.Empty;
    var phone = GetString(payload, "phone")?.Trim();

    if (string.IsNullOrEmpty(name))
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "name required");
    }

    try
    {
        var item = await customers.CreateCustomer(name, phone, ct);
        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (InvalidOperationException ex) when (ex.Message == "CONFLICT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "CONFLICT", "Conflict");
    }
});

app.MapPatch("/api/customers/{id:guid}", async (HttpContext ctx, HttpRequest req, Guid id, CustomersService customers, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Cashier, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    if (payload.ContainsKey("discountPercent"))
    {
        return ApiResults.Error(StatusCodes.Status403Forbidden, "FORBIDDEN", "Use admin endpoint to set discount");
    }

    try
    {
        var item = await customers.UpdateBasic(
            id,
            GetString(payload, "name")?.Trim(),
            payload.ContainsKey("phone") ? GetString(payload, "phone")?.Trim() : null,
            GetBool(payload, "isActive"),
            ct);

        if (item is null)
        {
            return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Customer not found");
        }

        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "CONFLICT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "CONFLICT", "Conflict");
    }
});

app.MapPatch("/api/admin/customers/{id:guid}/discount", async (HttpContext ctx, HttpRequest req, Guid id, CustomersService customers, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var discount = GetDouble(payload, "discountPercent");

    if (!discount.HasValue)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "discountPercent required");
    }

    try
    {
        var item = await customers.SetDiscount(id, discount.Value, ct);
        if (item is null)
        {
            return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Customer not found");
        }

        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "DISCOUNT_OUT_OF_RANGE")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "discountPercent must be between 0 and 100");
    }
});

app.MapPost("/api/admin/orders/{id:guid}/reopen", async (HttpContext ctx, HttpRequest req, Guid id, OrdersService orders, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var clearPayments = GetBool(payload, "clearPayments") ?? false;

    try
    {
        await orders.ReopenOrder(id, clearPayments, ct);
        return ApiResults.Ok(new { ok = true });
    }
    catch (PosNotFoundException)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Order not found");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "REOPEN_FAILED", ex.Message);
    }
});

app.MapGet("/api/admin/printers", async (HttpContext ctx, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var items = await admin.ListPrinters(ct);
    return ApiResults.Ok(new { items });
});

app.MapGet("/api/admin/settings/printers", async (HttpContext ctx, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var item = await admin.GetPrinterSettings(ct);
    return ApiResults.Ok(new { item });
});

app.MapPut("/api/admin/settings/printers", async (HttpContext ctx, HttpRequest req, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var receiptPrinterId = GetGuid(payload, "receiptPrinterId");
    var invoicePrinterId = GetGuid(payload, "invoicePrinterId");
    var cashierDocumentsPrinterId = GetGuid(payload, "cashierDocumentsPrinterId");

    try
    {
        var item = await admin.SetPrinterSettings(receiptPrinterId, invoicePrinterId, cashierDocumentsPrinterId, ct);
        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "RECEIPT_PRINTER_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "RECEIPT_PRINTER_NOT_FOUND", "Receipt printer not found or inactive");
    }
    catch (InvalidOperationException ex) when (ex.Message == "INVOICE_PRINTER_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "INVOICE_PRINTER_NOT_FOUND", "Invoice printer not found or inactive");
    }
    catch (InvalidOperationException ex) when (ex.Message == "CASHIER_DOCS_PRINTER_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "CASHIER_DOCS_PRINTER_NOT_FOUND", "Cashier documents printer not found or inactive");
    }
});

app.MapGet("/api/admin/settings/cashier-expenses", async (HttpContext ctx, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var item = await admin.GetCashierExpenseSettings(ct);
    return ApiResults.Ok(new { item });
});

app.MapPut("/api/admin/settings/cashier-expenses", async (HttpContext ctx, HttpRequest req, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var enabledForCashier = GetBool(payload, "enabledForCashier");
    var hasCapAmount = payload.ContainsKey("capAmount");
    var capAmount = GetDouble(payload, "capAmount");

    try
    {
        var item = await admin.SetCashierExpenseSettings(
            enabledForCashier,
            capAmount,
            hasCapAmount,
            ct);
        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "CASHIER_EXPENSE_CAP_INVALID")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "CASHIER_EXPENSE_CAP_INVALID", "capAmount must be greater than zero");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", ex.Message);
    }
});

app.MapGet("/api/admin/settings/invoice-template", async (HttpContext ctx, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var item = await admin.GetInvoiceTemplateSettings(ct);
    return ApiResults.Ok(new { item });
});

app.MapPut("/api/admin/settings/invoice-template", async (HttpContext ctx, HttpRequest req, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var item = await admin.SetInvoiceTemplateSettings(
        GetString(payload, "businessName"),
        GetString(payload, "businessTagline"),
        GetString(payload, "businessAddress"),
        GetString(payload, "businessPhone"),
        GetString(payload, "businessTaxNumber"),
        GetString(payload, "headerNote"),
        GetString(payload, "footerNote"),
        GetString(payload, "invoiceTitleEn"),
        GetString(payload, "invoiceTitleAr"),
        GetString(payload, "receiptTitleEn"),
        GetString(payload, "receiptTitleAr"),
        GetString(payload, "primaryColorHex"),
        GetString(payload, "accentColorHex"),
        GetString(payload, "layoutVariant"),
        GetBool(payload, "showLogo"),
        GetBool(payload, "showPaymentsSection"),
        ct);
    return ApiResults.Ok(new { item });
});

app.MapGet("/api/admin/settings/currencies", async (HttpContext ctx, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var item = await admin.GetCurrencySettings(ct);
    return ApiResults.Ok(new { item });
});

app.MapGet("/api/settings/currencies", async (HttpContext ctx, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out _) is { } denied)
    {
        return denied;
    }

    var item = await admin.GetCurrencySettings(ct);
    return ApiResults.Ok(new { item });
});

app.MapGet("/api/settings/invoice-template", async (HttpContext ctx, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Service, out _) is { } denied)
    {
        return denied;
    }

    var item = await admin.GetInvoiceTemplateSettings(ct);
    return ApiResults.Ok(new { item });
});

app.MapPut("/api/admin/settings/currencies", async (HttpContext ctx, HttpRequest req, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var defaultCurrencyCode = GetString(payload, "defaultCurrencyCode")?.Trim();
    var currencies = payload.TryGetValue("currencies", out var currenciesEl) ? currenciesEl : default;

    try
    {
        var item = await admin.SetCurrencySettings(defaultCurrencyCode, currencies, ct);
        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "CURRENCIES_REQUIRED")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "CURRENCIES_REQUIRED", "At least one currency is required");
    }
    catch (InvalidOperationException ex) when (ex.Message == "DEFAULT_CURRENCY_REQUIRED")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "DEFAULT_CURRENCY_REQUIRED", "defaultCurrencyCode is required");
    }
    catch (InvalidOperationException ex) when (ex.Message == "DEFAULT_CURRENCY_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "DEFAULT_CURRENCY_NOT_FOUND", "defaultCurrencyCode must exist in currencies list");
    }
});

app.MapGet("/api/admin/settings/currencies/rates", async (HttpContext ctx, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    try
    {
        var item = await admin.GetCurrencyRates(ct);
        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "FX_PROVIDER_FAILED")
    {
        return ApiResults.Error(StatusCodes.Status502BadGateway, "FX_PROVIDER_FAILED", "Failed to fetch currency rates");
    }
});

app.MapPost("/api/admin/printers", async (HttpContext ctx, HttpRequest req, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var name = GetString(payload, "name")?.Trim() ?? string.Empty;
    var type = GetString(payload, "type")?.Trim() ?? "network";
    var address = GetString(payload, "address")?.Trim();

    if (string.IsNullOrWhiteSpace(name))
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "name required");
    }

    try
    {
        var item = await admin.CreatePrinter(name, type, address, ct);
        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (InvalidOperationException ex) when (ex.Message == "CONFLICT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "CONFLICT", "Conflict");
    }
});

app.MapPatch("/api/admin/printers/{id:guid}", async (HttpContext ctx, HttpRequest req, Guid id, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);

    try
    {
        var item = await admin.UpdatePrinter(
            id,
            GetString(payload, "name")?.Trim(),
            GetString(payload, "type")?.Trim(),
            GetString(payload, "address")?.Trim(),
            payload.ContainsKey("address"),
            GetBool(payload, "isActive"),
            ct);

        if (item is null)
        {
            return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Printer not found");
        }

        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "CONFLICT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "CONFLICT", "Conflict");
    }
});

app.MapGet("/api/admin/materials", async (HttpContext ctx, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var items = await admin.ListMaterials(ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/admin/materials", async (HttpContext ctx, HttpRequest req, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var name = GetString(payload, "name")?.Trim() ?? string.Empty;
    var unit = GetString(payload, "unit")?.Trim();
    var stockQty = GetDouble(payload, "stockQty") ?? 0;

    if (string.IsNullOrWhiteSpace(name))
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "name required");
    }

    try
    {
        var item = await admin.CreateMaterial(name, unit, stockQty, ct);
        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (InvalidOperationException ex) when (ex.Message == "CONFLICT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "CONFLICT", "Conflict");
    }
});

app.MapPatch("/api/admin/materials/{id:guid}", async (HttpContext ctx, HttpRequest req, Guid id, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var item = await admin.UpdateMaterial(
        id,
        GetString(payload, "name")?.Trim(),
        GetString(payload, "unit")?.Trim(),
        GetDouble(payload, "stockDelta"),
        GetBool(payload, "isActive"),
        ct);

    if (item is null)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Material not found");
    }

    return ApiResults.Ok(new { item });
});

app.MapGet("/api/admin/categories", async (HttpContext ctx, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var items = await admin.ListCategories(ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/admin/categories", async (HttpContext ctx, HttpRequest req, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var name = GetString(payload, "name")?.Trim() ?? string.Empty;
    var sortOrder = GetInt(payload, "sortOrder") ?? 0;
    var printerId = GetGuid(payload, "printerId");
    var parentId = GetGuid(payload, "parentId");
    var imageUrl = GetString(payload, "imageUrl")?.Trim();

    if (string.IsNullOrWhiteSpace(name))
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "name required");
    }

    try
    {
        var item = await admin.CreateCategory(name, sortOrder, printerId, parentId, imageUrl, ct);
        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (InvalidOperationException ex) when (ex.Message == "CONFLICT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "CONFLICT", "Conflict");
    }
});

app.MapPatch("/api/admin/categories/{id:guid}", async (HttpContext ctx, HttpRequest req, Guid id, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);

    var item = await admin.UpdateCategory(
        id,
        GetString(payload, "name")?.Trim(),
        GetInt(payload, "sortOrder"),
        GetGuid(payload, "printerId"),
        payload.ContainsKey("printerId"),
        GetGuid(payload, "parentId"),
        payload.ContainsKey("parentId"),
        GetString(payload, "imageUrl")?.Trim(),
        payload.ContainsKey("imageUrl"),
        GetBool(payload, "isActive"),
        ct);

    if (item is null)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Category not found");
    }

    return ApiResults.Ok(new { item });
});

app.MapGet("/api/admin/menu-items", async (HttpContext ctx, HttpRequest req, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var categoryId = req.Query.TryGetValue("categoryId", out var categoryRaw) && Guid.TryParse(categoryRaw, out var parsedCategoryId)
        ? parsedCategoryId
        : (Guid?)null;

    var items = await admin.ListMenuItems(categoryId, ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/admin/menu-items", async (HttpContext ctx, HttpRequest req, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var categoryId = GetGuid(payload, "categoryId");
    var name = GetString(payload, "name")?.Trim() ?? string.Empty;
    var price = GetDouble(payload, "price");
    var stockQty = GetDouble(payload, "stockQty") ?? 0;
    var imageUrl = GetString(payload, "imageUrl")?.Trim();

    if (!categoryId.HasValue || string.IsNullOrWhiteSpace(name) || !price.HasValue)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "categoryId, name, price required");
    }

    try
    {
        var item = await admin.CreateMenuItem(categoryId.Value, name, price.Value, stockQty, imageUrl, ct);
        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (InvalidOperationException ex) when (ex.Message == "CONFLICT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "CONFLICT", "Conflict");
    }
});

app.MapPatch("/api/admin/menu-items/{id:guid}", async (HttpContext ctx, HttpRequest req, Guid id, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var item = await admin.UpdateMenuItem(
        id,
        GetString(payload, "name")?.Trim(),
        GetDouble(payload, "price"),
        GetBool(payload, "isActive"),
        GetGuid(payload, "categoryId"),
        GetDouble(payload, "stockDelta"),
        GetString(payload, "imageUrl")?.Trim(),
        payload.ContainsKey("imageUrl"),
        ct);

    if (item is null)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Menu item not found");
    }

    return ApiResults.Ok(new { item });
});

app.MapDelete("/api/admin/menu-items/{id:guid}", async (HttpContext ctx, Guid id, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    try
    {
        await admin.DeleteMenuItem(id, ct);
        return ApiResults.Ok(new { ok = true });
    }
    catch (InvalidOperationException ex) when (ex.Message == "MENU_ITEM_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Menu item not found");
    }
});

app.MapGet("/api/admin/menu-items/{id:guid}/materials", async (HttpContext ctx, Guid id, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var items = await admin.ListMenuItemMaterials(id, ct);
    return ApiResults.Ok(new { items });
});

app.MapPut("/api/admin/menu-items/{id:guid}/materials", async (HttpContext ctx, HttpRequest req, Guid id, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var items = ParseMenuItemMaterials(payload.TryGetValue("items", out var el) ? el : default);

    try
    {
        await admin.SetMenuItemMaterials(id, items, ct);
        return ApiResults.Ok(new { ok = true });
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "UPDATE_FAILED", ex.Message);
    }
});

app.MapGet("/api/admin/menu-items/{id:guid}/customizations", async (HttpContext ctx, Guid id, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var items = await admin.ListMenuItemCustomizations(id, includeInactive: true, ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/admin/menu-items/{id:guid}/customizations/groups", async (HttpContext ctx, HttpRequest req, Guid id, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var name = GetString(payload, "name")?.Trim() ?? string.Empty;
    if (string.IsNullOrWhiteSpace(name))
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "name required");
    }

    try
    {
        var item = await admin.CreateCustomizationGroup(
            id,
            name,
            GetBool(payload, "isRequired") ?? false,
            GetInt(payload, "minSelect") ?? 0,
            GetInt(payload, "maxSelect"),
            GetBool(payload, "allowQuantity") ?? false,
            GetInt(payload, "sortOrder") ?? 0,
            GetBool(payload, "isActive") ?? true,
            ct);
        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "CREATE_FAILED", ex.Message);
    }
});

app.MapPatch("/api/admin/customizations/groups/{id:guid}", async (HttpContext ctx, HttpRequest req, Guid id, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var item = await admin.UpdateCustomizationGroup(
        id,
        GetString(payload, "name")?.Trim(),
        GetBool(payload, "isRequired"),
        GetInt(payload, "minSelect"),
        GetInt(payload, "maxSelect"),
        payload.ContainsKey("maxSelect"),
        GetBool(payload, "allowQuantity"),
        GetInt(payload, "sortOrder"),
        GetBool(payload, "isActive"),
        ct);

    if (item is null)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Customization group not found");
    }

    return ApiResults.Ok(new { item });
});

app.MapPost("/api/admin/customizations/groups/{id:guid}/options", async (HttpContext ctx, HttpRequest req, Guid id, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var name = GetString(payload, "name")?.Trim() ?? string.Empty;
    if (string.IsNullOrWhiteSpace(name))
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "name required");
    }

    try
    {
        var item = await admin.CreateCustomizationOption(
            id,
            name,
            GetDouble(payload, "priceDelta") ?? 0,
            GetInt(payload, "maxQty"),
            GetInt(payload, "sortOrder") ?? 0,
            GetBool(payload, "isActive") ?? true,
            ct);
        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "CREATE_FAILED", ex.Message);
    }
});

app.MapPatch("/api/admin/customizations/options/{id:guid}", async (HttpContext ctx, HttpRequest req, Guid id, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var item = await admin.UpdateCustomizationOption(
        id,
        GetString(payload, "name")?.Trim(),
        GetDouble(payload, "priceDelta"),
        GetInt(payload, "maxQty"),
        payload.ContainsKey("maxQty"),
        GetInt(payload, "sortOrder"),
        GetBool(payload, "isActive"),
        ct);

    if (item is null)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Customization option not found");
    }

    return ApiResults.Ok(new { item });
});

app.MapGet("/api/admin/tables", async (HttpContext ctx, HttpRequest req, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var includeInactive = !req.Query.TryGetValue("includeInactive", out var includeRaw)
        || !string.Equals(includeRaw.ToString(), "false", StringComparison.OrdinalIgnoreCase);
    var items = await admin.ListTables(includeInactive, ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/admin/tables", async (HttpContext ctx, HttpRequest req, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var name = GetString(payload, "name")?.Trim() ?? string.Empty;
    if (string.IsNullOrWhiteSpace(name))
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "name required");
    }

    try
    {
        var item = await admin.CreateTable(name, ct);
        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (InvalidOperationException ex) when (ex.Message == "CONFLICT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "CONFLICT", "Conflict");
    }
});

app.MapPatch("/api/admin/tables/{id:guid}", async (HttpContext ctx, HttpRequest req, Guid id, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    try
    {
        var item = await admin.UpdateTable(id, GetString(payload, "name")?.Trim(), GetBool(payload, "isActive"), ct);
        if (item is null)
        {
            return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Table not found");
        }

        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "CONFLICT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "CONFLICT", "Conflict");
    }
});

app.MapGet("/api/admin/users", async (HttpContext ctx, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var items = await admin.ListUsers(ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/admin/users", async (HttpContext ctx, HttpRequest req, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var username = GetString(payload, "username")?.Trim() ?? string.Empty;
    var password = GetString(payload, "password") ?? string.Empty;
    var role = GetString(payload, "role")?.Trim() ?? "service";

    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "username and password required");
    }

    try
    {
        var item = await admin.CreateUser(username, password, role, ct);
        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "CREATE_FAILED", ex.Message);
    }
});

app.MapPatch("/api/admin/users/{id:guid}", async (HttpContext ctx, HttpRequest req, Guid id, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    try
    {
        var item = await admin.UpdateUser(
            id,
            GetString(payload, "role")?.Trim(),
            GetBool(payload, "isActive"),
            GetString(payload, "password"),
            ct);

        if (item is null)
        {
            return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "User not found");
        }

        return ApiResults.Ok(new { item });
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "UPDATE_FAILED", ex.Message);
    }
});

app.MapGet("/api/admin/employees", async (HttpContext ctx, HttpRequest req, PayrollService payroll, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var includeInactive = !bool.TryParse(req.Query["includeInactive"], out var parsed) || parsed;
    var items = await payroll.ListEmployees(includeInactive, ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/admin/employees", async (HttpContext ctx, HttpRequest req, PayrollService payroll, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var name = GetString(payload, "name")?.Trim() ?? string.Empty;
    var payRate = GetDouble(payload, "payRate");
    var overtimeModifier = GetDouble(payload, "overtimeModifier") ?? 1.5;
    var overtimeThresholdHours = GetDouble(payload, "overtimeThresholdHours") ?? 8;
    var accountPayload = GetObject(payload, "account");

    if (string.IsNullOrWhiteSpace(name) || !payRate.HasValue)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "name and payRate required");
    }

    try
    {
        var item = await payroll.CreateEmployee(
            name,
            payRate.Value,
            overtimeModifier,
            overtimeThresholdHours,
            GetString(payload, "note"),
            GetGuid(payload, "accountId"),
            GetString(accountPayload, "name")?.Trim(),
            GetString(accountPayload, "type")?.Trim(),
            GetString(accountPayload, "currency")?.Trim(),
            ct);

        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_PAY_RATE")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_PAY_RATE", "payRate must be greater than 0");
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_OVERTIME_MODIFIER")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_OVERTIME_MODIFIER", "overtimeModifier must be >= 1");
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_OVERTIME_THRESHOLD")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_OVERTIME_THRESHOLD", "overtimeThresholdHours must be between 0 and 24");
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_ACCOUNT_SELECTION")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_ACCOUNT_SELECTION", "Provide either accountId or account");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "ACCOUNT_NOT_FOUND", "Account not found");
    }
    catch (InvalidOperationException ex) when (ex.Message == "CONFLICT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "CONFLICT", "Conflict");
    }
});

app.MapPatch("/api/admin/employees/{id:guid}", async (HttpContext ctx, HttpRequest req, Guid id, PayrollService payroll, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var accountPayload = GetObject(payload, "account");
    try
    {
        var item = await payroll.UpdateEmployee(
            id,
            GetString(payload, "name"),
            GetDouble(payload, "payRate"),
            GetDouble(payload, "overtimeModifier"),
            GetDouble(payload, "overtimeThresholdHours"),
            GetString(payload, "note"),
            payload.ContainsKey("note"),
            GetGuid(payload, "accountId"),
            payload.ContainsKey("accountId"),
            GetString(accountPayload, "name")?.Trim(),
            GetString(accountPayload, "type")?.Trim(),
            GetString(accountPayload, "currency")?.Trim(),
            GetBool(payload, "isActive"),
            ct);

        if (item is null)
        {
            return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Employee not found");
        }

        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_PAY_RATE")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_PAY_RATE", "payRate must be greater than 0");
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_OVERTIME_MODIFIER")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_OVERTIME_MODIFIER", "overtimeModifier must be >= 1");
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_OVERTIME_THRESHOLD")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_OVERTIME_THRESHOLD", "overtimeThresholdHours must be between 0 and 24");
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_ACCOUNT_SELECTION")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_ACCOUNT_SELECTION", "Provide either accountId or account");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "ACCOUNT_NOT_FOUND", "Account not found");
    }
    catch (InvalidOperationException ex) when (ex.Message == "CONFLICT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "CONFLICT", "Conflict");
    }
});

app.MapGet("/api/admin/employees/{id:guid}/time-entries", async (HttpContext ctx, HttpRequest req, Guid id, PayrollService payroll, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var start = ParseDateOnly(req.Query["start"]);
    var end = ParseDateOnly(req.Query["end"]);
    var items = await payroll.ListTimeEntries(id, start, end, ct);
    if (items is null)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Employee not found");
    }

    return ApiResults.Ok(new { items });
});

app.MapPost("/api/admin/employees/{id:guid}/time-entries", async (HttpContext ctx, HttpRequest req, Guid id, PayrollService payroll, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var date = ParseDateOnly(GetString(payload, "date"));
    var startTime = ParseTimeOnly(GetString(payload, "start"));
    var endTime = ParseTimeOnly(GetString(payload, "end"));
    if (!date.HasValue || !startTime.HasValue || !endTime.HasValue)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "date, start, end required");
    }

    try
    {
        var item = await payroll.CreateTimeEntry(
            id,
            date.Value,
            startTime.Value,
            endTime.Value,
            GetString(payload, "note"),
            "manual",
            ct);

        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (InvalidOperationException ex) when (ex.Message == "EMPLOYEE_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Employee not found");
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_TIME_SPAN")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_TIME_SPAN", "Invalid start/end time span");
    }
    catch (InvalidOperationException ex) when (ex.Message == "CONFLICT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "DUPLICATE_ENTRY", "Duplicate entry");
    }
});

app.MapPost("/api/admin/employees/{id:guid}/time-entries/import-csv", async (HttpContext ctx, HttpRequest req, Guid id, PayrollService payroll, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var csv = GetString(payload, "csv") ?? string.Empty;
    if (string.IsNullOrWhiteSpace(csv))
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "csv required");
    }

    try
    {
        var result = await payroll.ImportCsv(id, csv, ct);
        return ApiResults.Ok(new { result });
    }
    catch (InvalidOperationException ex) when (ex.Message == "EMPLOYEE_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Employee not found");
    }
});

app.MapPatch("/api/admin/time-entries/{id:guid}", async (HttpContext ctx, HttpRequest req, Guid id, PayrollService payroll, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    try
    {
        var item = await payroll.UpdateTimeEntry(
            id,
            payload.ContainsKey("date") ? ParseDateOnly(GetString(payload, "date")) : null,
            payload.ContainsKey("start") ? ParseTimeOnly(GetString(payload, "start")) : null,
            payload.ContainsKey("end") ? ParseTimeOnly(GetString(payload, "end")) : null,
            GetString(payload, "note"),
            payload.ContainsKey("note"),
            ct);

        if (item is null)
        {
            return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Time entry not found");
        }

        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_TIME_SPAN")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_TIME_SPAN", "Invalid start/end time span");
    }
    catch (InvalidOperationException ex) when (ex.Message == "CONFLICT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "DUPLICATE_ENTRY", "Duplicate entry");
    }
});

app.MapDelete("/api/admin/time-entries/{id:guid}", async (HttpContext ctx, Guid id, PayrollService payroll, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var deleted = await payroll.DeleteTimeEntry(id, ct);
    if (!deleted)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Time entry not found");
    }

    return ApiResults.Ok(new { ok = true });
});

app.MapGet("/api/admin/payroll", async (HttpContext ctx, HttpRequest req, PayrollService payroll, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var start = ParseDateOnly(req.Query["start"]);
    var end = ParseDateOnly(req.Query["end"]);
    if (!start.HasValue || !end.HasValue)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "start and end required");
    }

    var employeeId = Guid.TryParse(req.Query["employeeId"], out var parsedId) ? parsedId : (Guid?)null;
    try
    {
        var item = await payroll.GetPayrollSummary(start.Value, end.Value, employeeId, ct);
        if (item is null)
        {
            return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Employee not found");
        }

        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_DATE_RANGE")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_DATE_RANGE", "start must be before or equal to end");
    }
});

app.MapGet("/api/admin/accounts", async (HttpContext ctx, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var items = await accounting.ListAccounts(ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/admin/accounts", async (HttpContext ctx, HttpRequest req, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var name = GetString(payload, "name")?.Trim() ?? string.Empty;
    var type = GetString(payload, "type")?.Trim() ?? "cash";
    var currency = GetString(payload, "currency")?.Trim() ?? "ILS";
    var parentAccountId = GetGuid(payload, "parentAccountId");
    var relations = ParseAccountRelations(payload.TryGetValue("relations", out var relEl) ? relEl : default);
    if (string.IsNullOrWhiteSpace(name))
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "name required");
    }

    try
    {
        var item = await accounting.CreateAccount(name, type, currency, parentAccountId, relations, ct);
        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (InvalidOperationException ex) when (ex.Message == "PARENT_ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "PARENT_ACCOUNT_NOT_FOUND", "Parent account not found");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_PARENT_SELF")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "ACCOUNT_PARENT_SELF", "Account cannot be its own parent");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_PARENT_CYCLE")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "ACCOUNT_PARENT_CYCLE", "Parent assignment would create a cycle");
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_ACCOUNT_RELATION")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_ACCOUNT_RELATION", "Each relation requires targetAccountId");
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_ACCOUNT_RELATIONS_PAYLOAD")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_ACCOUNT_RELATIONS_PAYLOAD", "relations must be an array");
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_ACCOUNT_RELATION_PERCENTAGE")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_ACCOUNT_RELATION_PERCENTAGE", "percentage must be > 0 and <= 100");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_RELATION_SELF")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "ACCOUNT_RELATION_SELF", "Account cannot relate to itself");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_RELATION_DUPLICATE")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "ACCOUNT_RELATION_DUPLICATE", "Duplicate relation target/kind");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_RELATION_PERCENTAGE_OVER_100")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "ACCOUNT_RELATION_PERCENTAGE_OVER_100", "Total percentage per kind cannot exceed 100");
    }
    catch (InvalidOperationException ex) when (ex.Message == "RELATION_ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "RELATION_ACCOUNT_NOT_FOUND", "One or more related accounts not found");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_MANAGED_BY_SHIFT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ACCOUNT_MANAGED_BY_SHIFT", "Shift session accounts cannot be used in parent/relations");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "CREATE_FAILED", ex.Message);
    }
});

app.MapPatch("/api/admin/accounts/{id:guid}", async (HttpContext ctx, HttpRequest req, Guid id, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var parentAccountId = GetGuid(payload, "parentAccountId");
    var hasParentAccountId = payload.ContainsKey("parentAccountId");
    var relations = ParseAccountRelations(payload.TryGetValue("relations", out var relEl) ? relEl : default);
    var hasRelations = payload.ContainsKey("relations");
    try
    {
        var item = await accounting.UpdateAccount(
            id,
            GetString(payload, "name")?.Trim(),
            GetString(payload, "type")?.Trim(),
            GetString(payload, "currency")?.Trim(),
            GetBool(payload, "isActive"),
            parentAccountId,
            hasParentAccountId,
            relations,
            hasRelations,
            ct);

        if (item is null)
        {
            return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Account not found");
        }

        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_LOCKED")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ACCOUNT_LOCKED", "System base accounts cannot be edited");
    }
    catch (InvalidOperationException ex) when (ex.Message == "PARENT_ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "PARENT_ACCOUNT_NOT_FOUND", "Parent account not found");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_PARENT_SELF")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "ACCOUNT_PARENT_SELF", "Account cannot be its own parent");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_PARENT_CYCLE")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "ACCOUNT_PARENT_CYCLE", "Parent assignment would create a cycle");
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_ACCOUNT_RELATION")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_ACCOUNT_RELATION", "Each relation requires targetAccountId");
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_ACCOUNT_RELATIONS_PAYLOAD")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_ACCOUNT_RELATIONS_PAYLOAD", "relations must be an array");
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_ACCOUNT_RELATION_PERCENTAGE")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_ACCOUNT_RELATION_PERCENTAGE", "percentage must be > 0 and <= 100");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_RELATION_SELF")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "ACCOUNT_RELATION_SELF", "Account cannot relate to itself");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_RELATION_DUPLICATE")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "ACCOUNT_RELATION_DUPLICATE", "Duplicate relation target/kind");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_RELATION_PERCENTAGE_OVER_100")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "ACCOUNT_RELATION_PERCENTAGE_OVER_100", "Total percentage per kind cannot exceed 100");
    }
    catch (InvalidOperationException ex) when (ex.Message == "RELATION_ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "RELATION_ACCOUNT_NOT_FOUND", "One or more related accounts not found");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_MANAGED_BY_SHIFT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ACCOUNT_MANAGED_BY_SHIFT", "Shift session accounts cannot be used in parent/relations");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "UPDATE_FAILED", ex.Message);
    }
});

app.MapGet("/api/admin/payment-method-accounts", async (HttpContext ctx, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var items = await accounting.ListPaymentMethodAccounts(ct);
    return ApiResults.Ok(new { items });
});

app.MapPut("/api/admin/payment-method-accounts/{method}", async (HttpContext ctx, HttpRequest req, string method, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var accountId = GetGuid(payload, "accountId");
    if (!accountId.HasValue)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "accountId required");
    }

    try
    {
        var item = await accounting.SetPaymentMethodAccount(method, accountId.Value, ct);
        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_METHOD")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_METHOD", "Invalid payment method");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "ACCOUNT_NOT_FOUND", "Account not found");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_INACTIVE")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ACCOUNT_INACTIVE", "Account is inactive");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_MANAGED_BY_SHIFT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ACCOUNT_MANAGED_BY_SHIFT", "Shift session accounts cannot be assigned manually");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "UPDATE_FAILED", ex.Message);
    }
});

app.MapGet("/api/admin/suppliers", async (HttpContext ctx, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var items = await accounting.ListSuppliers(ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/admin/suppliers", async (HttpContext ctx, HttpRequest req, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var accountPayload = GetObject(payload, "account");
    var name = GetString(payload, "name")?.Trim() ?? string.Empty;
    if (string.IsNullOrWhiteSpace(name))
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "name required");
    }

    try
    {
        var item = await accounting.CreateSupplier(
            name,
            GetString(payload, "phone")?.Trim(),
            GetString(payload, "email")?.Trim(),
            GetString(payload, "note")?.Trim(),
            GetGuid(payload, "accountId"),
            GetString(accountPayload, "name")?.Trim(),
            GetString(accountPayload, "type")?.Trim(),
            GetString(accountPayload, "currency")?.Trim(),
            ct);
        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_ACCOUNT_SELECTION")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_ACCOUNT_SELECTION", "Provide either accountId or account");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "ACCOUNT_NOT_FOUND", "Account not found");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "CREATE_FAILED", ex.Message);
    }
});

app.MapPatch("/api/admin/suppliers/{id:guid}", async (HttpContext ctx, HttpRequest req, Guid id, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var accountPayload = GetObject(payload, "account");
    try
    {
        var item = await accounting.UpdateSupplier(
            id,
            GetString(payload, "name")?.Trim(),
            GetString(payload, "phone")?.Trim(),
            GetString(payload, "email")?.Trim(),
            GetString(payload, "note")?.Trim(),
            GetGuid(payload, "accountId"),
            payload.ContainsKey("accountId"),
            GetString(accountPayload, "name")?.Trim(),
            GetString(accountPayload, "type")?.Trim(),
            GetString(accountPayload, "currency")?.Trim(),
            GetBool(payload, "isActive"),
            ct);

        if (item is null)
        {
            return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Supplier not found");
        }

        return ApiResults.Ok(new { item });
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_ACCOUNT_SELECTION")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_ACCOUNT_SELECTION", "Provide either accountId or account");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "ACCOUNT_NOT_FOUND", "Account not found");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", ex.Message);
    }
});

app.MapGet("/api/admin/receipts", async (HttpContext ctx, HttpRequest req, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var start = ParseDateOnly(req.Query["start"]);
    var end = ParseDateOnly(req.Query["end"]);
    var items = await accounting.ListReceipts(start, end, ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/admin/receipts", async (HttpContext ctx, HttpRequest req, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out var user) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var amount = GetDouble(payload, "amount");
    var method = GetString(payload, "method")?.Trim() ?? string.Empty;
    var accountId = GetGuid(payload, "accountId");

    if (!amount.HasValue || amount <= 0 || string.IsNullOrWhiteSpace(method) || !accountId.HasValue)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "amount, method, accountId required");
    }

    try
    {
        var item = await accounting.CreateReceipt(
            user!.Id,
            amount.Value,
            method,
            accountId.Value,
            GetString(payload, "source")?.Trim(),
            GetGuid(payload, "supplierId"),
            ParseDateOnly(payload.TryGetValue("date", out var rawDate) ? GetElementAsString(rawDate) : null),
            GetString(payload, "note")?.Trim(),
            ct);
        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "ACCOUNT_NOT_FOUND", "Account not found");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_INACTIVE")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ACCOUNT_INACTIVE", "Account is inactive");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_MANAGED_BY_SHIFT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ACCOUNT_MANAGED_BY_SHIFT", "Shift session accounts are managed automatically");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "CREATE_FAILED", ex.Message);
    }
});

app.MapGet("/api/admin/expenses", async (HttpContext ctx, HttpRequest req, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var start = ParseDateOnly(req.Query["start"]);
    var end = ParseDateOnly(req.Query["end"]);
    var items = await accounting.ListExpenses(start, end, ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/admin/expenses", async (HttpContext ctx, HttpRequest req, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out var user) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var category = GetString(payload, "category")?.Trim() ?? string.Empty;
    var amount = GetDouble(payload, "amount");
    var method = GetString(payload, "method")?.Trim() ?? string.Empty;
    var accountId = GetGuid(payload, "accountId");
    var supplierId = GetGuid(payload, "supplierId");
    var employeeId = GetGuid(payload, "employeeId");

    if (string.IsNullOrWhiteSpace(category) ||
        !amount.HasValue ||
        amount <= 0 ||
        string.IsNullOrWhiteSpace(method) ||
        (!accountId.HasValue && !supplierId.HasValue && !employeeId.HasValue))
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "category, amount, method and one of accountId/supplierId/employeeId required");
    }

    try
    {
        var item = await accounting.CreateExpense(
            user!.Id,
            category,
            amount.Value,
            method,
            accountId,
            supplierId,
            employeeId,
            ParseDateOnly(payload.TryGetValue("date", out var rawDate) ? GetElementAsString(rawDate) : null),
            GetString(payload, "attachmentUrl")?.Trim(),
            GetString(payload, "note")?.Trim(),
            ct);
        return ApiResults.Ok(new { item }, StatusCodes.Status201Created);
    }
    catch (InvalidOperationException ex) when (ex.Message == "BAD_EXPENSE_TARGET")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_EXPENSE_TARGET", "Choose supplierId or employeeId, not both");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_REQUIRED")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "ACCOUNT_REQUIRED", "Selected source has no linked account");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "ACCOUNT_NOT_FOUND", "Account not found");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_INACTIVE")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ACCOUNT_INACTIVE", "Account is inactive");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_MANAGED_BY_SHIFT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ACCOUNT_MANAGED_BY_SHIFT", "Shift session accounts are managed automatically");
    }
    catch (InvalidOperationException ex) when (ex.Message == "SUPPLIER_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "SUPPLIER_NOT_FOUND", "Supplier not found");
    }
    catch (InvalidOperationException ex) when (ex.Message == "EMPLOYEE_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "EMPLOYEE_NOT_FOUND", "Employee not found");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "CREATE_FAILED", ex.Message);
    }
});

app.MapPost("/api/admin/vault/deposit", async (HttpContext ctx, HttpRequest req, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out var user) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var accountId = GetGuid(payload, "accountId");
    var amount = GetDouble(payload, "amount");
    if (!accountId.HasValue || !amount.HasValue || amount <= 0)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "accountId and amount required");
    }

    try
    {
        await accounting.Deposit(user!.Id, accountId.Value, amount.Value, GetString(payload, "note")?.Trim(), ct);
        return ApiResults.Ok(new { ok = true });
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "ACCOUNT_NOT_FOUND", "Account not found");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_INACTIVE")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ACCOUNT_INACTIVE", "Account is inactive");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_MANAGED_BY_SHIFT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ACCOUNT_MANAGED_BY_SHIFT", "Shift session accounts are managed automatically");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "DEPOSIT_FAILED", ex.Message);
    }
});

app.MapPost("/api/admin/vault/withdraw", async (HttpContext ctx, HttpRequest req, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out var user) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var accountId = GetGuid(payload, "accountId");
    var amount = GetDouble(payload, "amount");
    if (!accountId.HasValue || !amount.HasValue || amount <= 0)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "accountId and amount required");
    }

    try
    {
        await accounting.Withdraw(user!.Id, accountId.Value, amount.Value, GetString(payload, "note")?.Trim(), ct);
        return ApiResults.Ok(new { ok = true });
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "ACCOUNT_NOT_FOUND", "Account not found");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_INACTIVE")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ACCOUNT_INACTIVE", "Account is inactive");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_MANAGED_BY_SHIFT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ACCOUNT_MANAGED_BY_SHIFT", "Shift session accounts are managed automatically");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "WITHDRAW_FAILED", ex.Message);
    }
});

app.MapPost("/api/admin/vault/transfer", async (HttpContext ctx, HttpRequest req, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out var user) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var fromId = GetGuid(payload, "fromAccountId");
    var toId = GetGuid(payload, "toAccountId");
    var amount = GetDouble(payload, "amount");

    if (!fromId.HasValue || !toId.HasValue || !amount.HasValue || amount <= 0)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "fromAccountId, toAccountId, amount required");
    }

    try
    {
        await accounting.Transfer(user!.Id, fromId.Value, toId.Value, amount.Value, GetString(payload, "note")?.Trim(), ct);
        return ApiResults.Ok(new { ok = true });
    }
    catch (InvalidOperationException ex) when (ex.Message == "TRANSFER_SAME_ACCOUNT")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "TRANSFER_SAME_ACCOUNT", "Source and destination accounts must be different");
    }
    catch (InvalidOperationException ex) when (ex.Message == "TRANSFER_CURRENCY_MISMATCH")
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "TRANSFER_CURRENCY_MISMATCH", "Accounts must use the same currency");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_NOT_FOUND")
    {
        return ApiResults.Error(StatusCodes.Status404NotFound, "ACCOUNT_NOT_FOUND", "Account not found");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_INACTIVE")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ACCOUNT_INACTIVE", "Account is inactive");
    }
    catch (InvalidOperationException ex) when (ex.Message == "ACCOUNT_MANAGED_BY_SHIFT")
    {
        return ApiResults.Error(StatusCodes.Status409Conflict, "ACCOUNT_MANAGED_BY_SHIFT", "Shift session accounts are managed automatically");
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "TRANSFER_FAILED", ex.Message);
    }
});

app.MapGet("/api/admin/ledger", async (HttpContext ctx, HttpRequest req, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var start = ParseDateTime(req.Query["start"]);
    var end = EndOfDay(ParseDateTime(req.Query["end"]));
    var items = await accounting.ListLedger(start, end, ct);
    return ApiResults.Ok(new { items });
});

app.MapGet("/api/admin/reports/daily-sales", async (HttpContext ctx, HttpRequest req, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var start = ParseDateOnly(req.Query["start"]);
    var end = ParseDateOnly(req.Query["end"]);
    if (!start.HasValue || !end.HasValue)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "start and end required");
    }

    var items = await accounting.ReportDailySales(start.Value, end.Value, ct);
    return ApiResults.Ok(new { items });
});

app.MapGet("/api/admin/reports/profit-loss", async (HttpContext ctx, HttpRequest req, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var start = ParseDateOnly(req.Query["start"]);
    var end = ParseDateOnly(req.Query["end"]);
    if (!start.HasValue || !end.HasValue)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "start and end required");
    }

    var item = await accounting.ReportProfitLoss(start.Value, end.Value, ct);
    return ApiResults.Ok(new { item });
});

app.MapGet("/api/admin/reports/cash-flow", async (HttpContext ctx, HttpRequest req, AccountingService accounting, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var start = ParseDateTime(req.Query["start"]);
    var end = EndOfDay(ParseDateTime(req.Query["end"]));
    if (!start.HasValue || !end.HasValue)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "BAD_REQUEST", "start and end required");
    }

    var items = await accounting.ReportCashFlow(start.Value, end.Value, ct);
    return ApiResults.Ok(new { items });
});

app.MapGet("/api/admin/print-queue", async (HttpContext ctx, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var items = await admin.ListPrintQueue(ct);
    return ApiResults.Ok(new { items });
});

app.MapPost("/api/admin/print-queue/{id:guid}/retry", async (HttpContext ctx, Guid id, PrintService printService, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    try
    {
        var result = await printService.RetryQueueItem(id, ct);
        return ApiResults.Ok(new { result });
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "RETRY_FAILED", ex.Message);
    }
});

app.MapPost("/api/admin/print-queue/retry-all", async (HttpContext ctx, HttpRequest req, PrintService printService, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var payload = await ReadJsonMap(req, ct);
    var limit = GetInt(payload, "limit") ?? 20;
    var result = await printService.RetryPendingQueue(limit, ct);
    return ApiResults.Ok(new { result });
});

app.MapDelete("/api/admin/print-queue/{id:guid}", async (HttpContext ctx, Guid id, AdminService admin, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    try
    {
        await admin.DeletePrintQueueItem(id, ct);
        return ApiResults.Ok(new { ok = true });
    }
    catch (Exception ex)
    {
        return ApiResults.Error(StatusCodes.Status400BadRequest, "DELETE_FAILED", ex.Message);
    }
});

app.MapGet("/api/admin/audit-logs", async (HttpContext ctx, HttpRequest req, AuditService audit, CancellationToken ct) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    var limit = int.TryParse(req.Query["limit"], out var parsed) ? parsed : 200;
    var items = await audit.ListAuditLogs(limit, ct);
    return ApiResults.Ok(new { items });
});

app.MapMethods("/api/admin/{**path}", ["GET", "POST", "PATCH", "PUT", "DELETE"], (HttpContext ctx) =>
{
    if (ctx.RequireMinRole(PosRole.Admin, out _) is { } denied)
    {
        return denied;
    }

    return ApiResults.Error(StatusCodes.Status404NotFound, "NOT_FOUND", "Admin endpoint not found");
});

app.MapGet("/", () => ApiResults.Ok(new
{
    name = "restaurant_pos ASP.NET backend",
    framework = ".NET 10",
    status = "ok",
}));

app.Run();

static async Task<Dictionary<string, JsonElement>> ReadJsonMap(HttpRequest req, CancellationToken ct)
{
    return await req.ReadFromJsonAsync<Dictionary<string, JsonElement>>(cancellationToken: ct) ?? new Dictionary<string, JsonElement>();
}

static string? GetString(IReadOnlyDictionary<string, JsonElement>? payload, string key)
{
    if (payload is null || !payload.TryGetValue(key, out var value))
    {
        return null;
    }

    return value.ValueKind switch
    {
        JsonValueKind.Null => null,
        JsonValueKind.String => value.GetString(),
        JsonValueKind.Number => value.ToString(),
        JsonValueKind.True => "true",
        JsonValueKind.False => "false",
        _ => null,
    };
}

static int? GetInt(IReadOnlyDictionary<string, JsonElement> payload, string key)
{
    if (!payload.TryGetValue(key, out var value))
    {
        return null;
    }

    if (value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var num))
    {
        return num;
    }

    if (value.ValueKind == JsonValueKind.String && int.TryParse(value.GetString(), out var parsed))
    {
        return parsed;
    }

    return null;
}

static double? GetDouble(IReadOnlyDictionary<string, JsonElement> payload, string key)
{
    if (!payload.TryGetValue(key, out var value))
    {
        return null;
    }

    if (value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out var num))
    {
        return num;
    }

    if (value.ValueKind == JsonValueKind.String && double.TryParse(value.GetString(), out var parsed))
    {
        return parsed;
    }

    return null;
}

static bool? GetBool(IReadOnlyDictionary<string, JsonElement> payload, string key)
{
    if (!payload.TryGetValue(key, out var value))
    {
        return null;
    }

    return value.ValueKind switch
    {
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.String when bool.TryParse(value.GetString(), out var parsed) => parsed,
        _ => null,
    };
}

static Guid? GetGuid(IReadOnlyDictionary<string, JsonElement> payload, string key)
{
    var raw = GetString(payload, key)?.Trim();
    if (string.IsNullOrWhiteSpace(raw))
    {
        return null;
    }

    return Guid.TryParse(raw, out var id) ? id : null;
}

static Dictionary<string, JsonElement>? GetObject(IReadOnlyDictionary<string, JsonElement> payload, string key)
{
    if (!payload.TryGetValue(key, out var value) || value.ValueKind != JsonValueKind.Object)
    {
        return null;
    }

    var map = new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);
    foreach (var prop in value.EnumerateObject())
    {
        map[prop.Name] = prop.Value;
    }

    return map;
}

static List<CustomizationSelection> ParseCustomizations(JsonElement el)
{
    if (el.ValueKind != JsonValueKind.Array)
    {
        return [];
    }

    var outList = new List<CustomizationSelection>();
    foreach (var row in el.EnumerateArray())
    {
        if (row.ValueKind != JsonValueKind.Object)
        {
            continue;
        }

        if (!row.TryGetProperty("optionId", out var optionIdEl))
        {
            continue;
        }

        var optionRaw = optionIdEl.GetString();
        if (!Guid.TryParse(optionRaw, out var optionId))
        {
            continue;
        }

        double qty = 1;
        if (row.TryGetProperty("qty", out var qtyEl))
        {
            if (qtyEl.ValueKind == JsonValueKind.Number && qtyEl.TryGetDouble(out var qn))
            {
                qty = qn;
            }
            else if (qtyEl.ValueKind == JsonValueKind.String && double.TryParse(qtyEl.GetString(), out var qs))
            {
                qty = qs;
            }
        }

        outList.Add(new CustomizationSelection(optionId, qty));
    }

    return outList;
}

static List<Guid> ParseGuidList(JsonElement el)
{
    if (el.ValueKind != JsonValueKind.Array)
    {
        return [];
    }

    var outList = new List<Guid>();
    foreach (var row in el.EnumerateArray())
    {
        var raw = row.ValueKind == JsonValueKind.String ? row.GetString() : row.ToString();
        if (Guid.TryParse(raw, out var id))
        {
            outList.Add(id);
        }
    }

    return outList;
}

static List<AccountRelationInput> ParseAccountRelations(JsonElement el)
{
    if (el.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null)
    {
        return [];
    }

    if (el.ValueKind != JsonValueKind.Array)
    {
        throw new InvalidOperationException("BAD_ACCOUNT_RELATIONS_PAYLOAD");
    }

    var outList = new List<AccountRelationInput>();
    foreach (var row in el.EnumerateArray())
    {
        if (row.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException("BAD_ACCOUNT_RELATION");
        }

        var targetRaw = GetElementAsString(row.TryGetProperty("targetAccountId", out var targetEl) ? targetEl : default);
        if (!Guid.TryParse(targetRaw, out var targetAccountId))
        {
            throw new InvalidOperationException("BAD_ACCOUNT_RELATION");
        }

        var percentageRaw = GetElementAsString(row.TryGetProperty("percentage", out var pctEl) ? pctEl : default);
        if (!decimal.TryParse(percentageRaw, out var percentage))
        {
            throw new InvalidOperationException("BAD_ACCOUNT_RELATION_PERCENTAGE");
        }

        var kind = GetElementAsString(row.TryGetProperty("kind", out var kindEl) ? kindEl : default);
        outList.Add(new AccountRelationInput(targetAccountId, percentage, kind));
    }

    return outList;
}

static List<(Guid MaterialId, double Qty)> ParseMenuItemMaterials(JsonElement el)
{
    if (el.ValueKind != JsonValueKind.Array)
    {
        return [];
    }

    var outList = new List<(Guid MaterialId, double Qty)>();
    foreach (var row in el.EnumerateArray())
    {
        if (row.ValueKind != JsonValueKind.Object)
        {
            continue;
        }

        if (!row.TryGetProperty("materialId", out var materialEl))
        {
            continue;
        }

        if (!Guid.TryParse(materialEl.GetString(), out var materialId))
        {
            continue;
        }

        double qty = 0;
        if (row.TryGetProperty("qty", out var qtyEl))
        {
            if (qtyEl.ValueKind == JsonValueKind.Number && qtyEl.TryGetDouble(out var qn))
            {
                qty = qn;
            }
            else if (qtyEl.ValueKind == JsonValueKind.String && double.TryParse(qtyEl.GetString(), out var qs))
            {
                qty = qs;
            }
        }

        outList.Add((materialId, qty));
    }

    return outList;
}

static string? GetElementAsString(JsonElement element)
{
    return element.ValueKind switch
    {
        JsonValueKind.String => element.GetString(),
        JsonValueKind.Null => null,
        JsonValueKind.Undefined => null,
        _ => element.ToString(),
    };
}

static DateOnly? ParseDateOnly(string? raw)
{
    if (string.IsNullOrWhiteSpace(raw))
    {
        return null;
    }

    return DateOnly.TryParse(raw, out var d) ? d : null;
}

static TimeOnly? ParseTimeOnly(string? raw)
{
    if (string.IsNullOrWhiteSpace(raw))
    {
        return null;
    }

    if (TimeOnly.TryParse(raw, out var time))
    {
        return time;
    }

    var trimmed = raw.Trim();
    if (trimmed.Length == 4 &&
        int.TryParse(trimmed[..2], out var hh) &&
        int.TryParse(trimmed[2..], out var mm) &&
        hh >= 0 && hh <= 23 &&
        mm >= 0 && mm <= 59)
    {
        return new TimeOnly(hh, mm);
    }

    return null;
}

static DateTime? ParseDateTime(string? raw)
{
    if (string.IsNullOrWhiteSpace(raw))
    {
        return null;
    }

    return DateTime.TryParse(raw, out var d) ? d : null;
}

static DateTime? EndOfDay(DateTime? date)
{
    if (!date.HasValue)
    {
        return null;
    }

    var d = date.Value;
    return new DateTime(d.Year, d.Month, d.Day, 23, 59, 59, DateTimeKind.Utc);
}

static async Task<string> ReadRequestBodyForLog(HttpRequest request)
{
    if (request.Body is null || !request.Body.CanRead)
    {
        return "<empty>";
    }

    var hasBody = (request.ContentLength ?? 0) > 0 || request.Headers.ContainsKey("Transfer-Encoding");
    if (!hasBody)
    {
        return "<empty>";
    }

    if (!IsTextContentType(request.ContentType))
    {
        return $"<binary:{request.ContentLength?.ToString() ?? "unknown"} bytes>";
    }

    request.EnableBuffering();
    request.Body.Position = 0;

    using var reader = new StreamReader(request.Body, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
    var body = await reader.ReadToEndAsync();
    request.Body.Position = 0;

    return string.IsNullOrWhiteSpace(body) ? "<empty>" : body;
}

static async Task<string> ReadResponseBodyForLog(Stream responseBody, string? contentType)
{
    responseBody.Position = 0;

    if (!IsTextContentType(contentType))
    {
        return $"<binary:{responseBody.Length} bytes>";
    }

    using var reader = new StreamReader(responseBody, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
    var body = await reader.ReadToEndAsync();
    responseBody.Position = 0;

    return string.IsNullOrWhiteSpace(body) ? "<empty>" : body;
}

static bool IsTextContentType(string? contentType)
{
    if (string.IsNullOrWhiteSpace(contentType))
    {
        return true;
    }

    return contentType.StartsWith("text/", StringComparison.OrdinalIgnoreCase)
           || contentType.Contains("json", StringComparison.OrdinalIgnoreCase)
           || contentType.Contains("xml", StringComparison.OrdinalIgnoreCase)
           || contentType.Contains("javascript", StringComparison.OrdinalIgnoreCase)
           || contentType.Contains("x-www-form-urlencoded", StringComparison.OrdinalIgnoreCase);
}

static string TruncateForLog(string value, int max = 8192)
{
    if (value.Length <= max)
    {
        return value;
    }

    return $"{value[..max]}...(truncated {value.Length - max} chars)";
}

static bool ShouldWriteAuditLog(string method, PathString path)
{
    var rawPath = path.Value ?? string.Empty;

    if (!rawPath.StartsWith("/api/admin", StringComparison.OrdinalIgnoreCase))
    {
        return false;
    }

    if (rawPath.StartsWith("/api/admin/audit-logs", StringComparison.OrdinalIgnoreCase))
    {
        return false;
    }

    return HttpMethods.IsPost(method)
           || HttpMethods.IsPut(method)
           || HttpMethods.IsPatch(method)
           || HttpMethods.IsDelete(method);
}

static string? BuildAuditableBody(string value)
{
    if (string.IsNullOrWhiteSpace(value) || value == "<empty>")
    {
        return null;
    }

    if (value.StartsWith("<binary:", StringComparison.OrdinalIgnoreCase))
    {
        return value;
    }

    var redacted = RedactSensitiveJsonFields(value);
    return TruncateForLog(redacted, 4000);
}

static string RedactSensitiveJsonFields(string raw)
{
    try
    {
        using var doc = JsonDocument.Parse(raw);
        var redacted = RedactJsonElement(doc.RootElement);
        return JsonSerializer.Serialize(redacted);
    }
    catch
    {
        return raw;
    }
}

static object? RedactJsonElement(JsonElement element)
{
    return element.ValueKind switch
    {
        JsonValueKind.Object => RedactJsonObject(element),
        JsonValueKind.Array => element.EnumerateArray().Select(RedactJsonElement).ToList(),
        JsonValueKind.String => element.GetString(),
        JsonValueKind.Number => element.ToString(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null => null,
        JsonValueKind.Undefined => null,
        _ => element.ToString(),
    };
}

static Dictionary<string, object?> RedactJsonObject(JsonElement element)
{
    var map = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
    foreach (var prop in element.EnumerateObject())
    {
        if (IsSensitiveAuditField(prop.Name))
        {
            map[prop.Name] = "***";
            continue;
        }

        map[prop.Name] = RedactJsonElement(prop.Value);
    }

    return map;
}

static bool IsSensitiveAuditField(string field)
{
    return field.Contains("password", StringComparison.OrdinalIgnoreCase)
           || field.Contains("token", StringComparison.OrdinalIgnoreCase)
           || field.Contains("secret", StringComparison.OrdinalIgnoreCase)
           || field.Contains("pin", StringComparison.OrdinalIgnoreCase);
}

static async Task EnsureOrderNicknameColumnAsync(WebApplication app, CancellationToken ct)
{
    await using var scope = app.Services.CreateAsyncScope();
    var db = scope.ServiceProvider.GetRequiredService<PosDbContext>();
    await db.Database.ExecuteSqlRawAsync(
        """
        ALTER TABLE IF EXISTS pos_orders
        ADD COLUMN IF NOT EXISTS nickname text;
        """,
        ct);
}

static async Task EnsureAuditLogsTableAsync(WebApplication app, CancellationToken ct)
{
    await using var scope = app.Services.CreateAsyncScope();
    var db = scope.ServiceProvider.GetRequiredService<PosDbContext>();
    await db.Database.ExecuteSqlRawAsync(
        """
        CREATE TABLE IF NOT EXISTS pos_audit_logs (
            id uuid PRIMARY KEY,
            user_id uuid NULL,
            username text NULL,
            role text NULL,
            method text NOT NULL,
            path text NOT NULL,
            status_code integer NOT NULL,
            request_body text NULL,
            response_body text NULL,
            created_at timestamp with time zone NOT NULL DEFAULT now()
        );
        """,
        ct);

    await db.Database.ExecuteSqlRawAsync(
        """
        CREATE INDEX IF NOT EXISTS ix_pos_audit_logs_created_at
        ON pos_audit_logs (created_at DESC);
        """,
        ct);
}

static async Task EnsureAccountingLinksColumnsAsync(WebApplication app, CancellationToken ct)
{
    await using var scope = app.Services.CreateAsyncScope();
    var db = scope.ServiceProvider.GetRequiredService<PosDbContext>();
    await db.Database.ExecuteSqlRawAsync(
        """
        ALTER TABLE IF EXISTS pos_suppliers
        ADD COLUMN IF NOT EXISTS account_id uuid NULL;

        ALTER TABLE IF EXISTS pos_employees
        ADD COLUMN IF NOT EXISTS account_id uuid NULL;

        ALTER TABLE IF EXISTS pos_expenses
        ADD COLUMN IF NOT EXISTS employee_id uuid NULL;
        """,
        ct);
}

static async Task EnsureSystemAccountsColumnsAsync(WebApplication app, CancellationToken ct)
{
    await using var scope = app.Services.CreateAsyncScope();
    var db = scope.ServiceProvider.GetRequiredService<PosDbContext>();
    await db.Database.ExecuteSqlRawAsync(
        """
        ALTER TABLE IF EXISTS pos_accounts
        ADD COLUMN IF NOT EXISTS account_scope text NOT NULL DEFAULT 'custom';

        ALTER TABLE IF EXISTS pos_accounts
        ADD COLUMN IF NOT EXISTS account_key text NULL;

        ALTER TABLE IF EXISTS pos_accounts
        ADD COLUMN IF NOT EXISTS is_system boolean NOT NULL DEFAULT false;

        ALTER TABLE IF EXISTS pos_accounts
        ADD COLUMN IF NOT EXISTS is_locked boolean NOT NULL DEFAULT false;

        ALTER TABLE IF EXISTS pos_accounts
        ADD COLUMN IF NOT EXISTS shift_id uuid NULL;

        ALTER TABLE IF EXISTS pos_accounts
        ADD COLUMN IF NOT EXISTS base_account_id uuid NULL;

        ALTER TABLE IF EXISTS pos_accounts
        ADD COLUMN IF NOT EXISTS parent_account_id uuid NULL;
        """,
        ct);

    await db.Database.ExecuteSqlRawAsync(
        """
        CREATE INDEX IF NOT EXISTS ix_pos_accounts_scope_key
        ON pos_accounts (account_scope, account_key);

        CREATE INDEX IF NOT EXISTS ix_pos_accounts_shift_id
        ON pos_accounts (shift_id);

        CREATE INDEX IF NOT EXISTS ix_pos_accounts_parent_account_id
        ON pos_accounts (parent_account_id);
        """,
        ct);

    await db.Database.ExecuteSqlRawAsync(
        """
        CREATE TABLE IF NOT EXISTS pos_account_relations (
            id uuid PRIMARY KEY,
            from_account_id uuid NOT NULL,
            to_account_id uuid NOT NULL,
            percentage numeric(5,2) NOT NULL,
            kind text NOT NULL DEFAULT 'allocation',
            created_at timestamp with time zone NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS ix_pos_account_relations_from_account_id
        ON pos_account_relations (from_account_id);

        CREATE INDEX IF NOT EXISTS ix_pos_account_relations_to_account_id
        ON pos_account_relations (to_account_id);

        CREATE UNIQUE INDEX IF NOT EXISTS ux_pos_account_relations_from_to_kind
        ON pos_account_relations (from_account_id, to_account_id, kind);
        """,
        ct);
}

static async Task EnsureSystemAccountsBootstrapAsync(WebApplication app, CancellationToken ct)
{
    await using var scope = app.Services.CreateAsyncScope();
    var systemAccounts = scope.ServiceProvider.GetRequiredService<SystemAccountsService>();
    await systemAccounts.EnsureVaultBaseAccounts(DateTime.UtcNow, ct);
    await systemAccounts.EnsureIngredientStockAccounts(DateTime.UtcNow, ct);
    var db = scope.ServiceProvider.GetRequiredService<PosDbContext>();
    await db.SaveChangesAsync(ct);
}
