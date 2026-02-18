using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace PosBackend.AspNet.Models;

[Table("pos_users")]
public sealed class PosUser
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("username")]
    public string Username { get; set; } = string.Empty;

    [Column("password_hash")]
    public string PasswordHash { get; set; } = string.Empty;

    [Column("role")]
    public string Role { get; set; } = "service";

    [Column("is_active")]
    public bool IsActive { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_sessions")]
public sealed class PosSession
{
    [Key]
    [Column("token")]
    public string Token { get; set; } = string.Empty;

    [Column("user_id")]
    public Guid UserId { get; set; }

    [Column("expires_at")]
    public DateTime ExpiresAt { get; set; }

    public PosUser? User { get; set; }
}

[Table("pos_categories")]
public sealed class PosCategory
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("name")]
    public string Name { get; set; } = string.Empty;

    [Column("sort_order")]
    public int SortOrder { get; set; }

    [Column("printer_id")]
    public Guid? PrinterId { get; set; }

    [Column("parent_id")]
    public Guid? ParentId { get; set; }

    [Column("image_url")]
    public string? ImageUrl { get; set; }

    [Column("is_active")]
    public bool IsActive { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_menu_items")]
public sealed class PosMenuItem
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("category_id")]
    public Guid CategoryId { get; set; }

    [Column("name")]
    public string Name { get; set; } = string.Empty;

    [Column("price")]
    public decimal Price { get; set; }

    [Column("stock_qty")]
    public decimal StockQty { get; set; }

    [Column("image_url")]
    public string? ImageUrl { get; set; }

    [Column("is_active")]
    public bool IsActive { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_menu_item_option_groups")]
public sealed class PosMenuItemOptionGroup
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("menu_item_id")]
    public Guid MenuItemId { get; set; }

    [Column("name")]
    public string Name { get; set; } = string.Empty;

    [Column("is_required")]
    public bool IsRequired { get; set; }

    [Column("min_select")]
    public int MinSelect { get; set; }

    [Column("max_select")]
    public int? MaxSelect { get; set; }

    [Column("allow_quantity")]
    public bool AllowQuantity { get; set; }

    [Column("sort_order")]
    public int SortOrder { get; set; }

    [Column("is_active")]
    public bool IsActive { get; set; }
}

[Table("pos_menu_item_options")]
public sealed class PosMenuItemOption
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("group_id")]
    public Guid GroupId { get; set; }

    [Column("name")]
    public string Name { get; set; } = string.Empty;

    [Column("price_delta")]
    public decimal PriceDelta { get; set; }

    [Column("max_qty")]
    public int? MaxQty { get; set; }

    [Column("sort_order")]
    public int SortOrder { get; set; }

    [Column("is_active")]
    public bool IsActive { get; set; }
}

[Table("pos_tables")]
public sealed class PosTable
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("name")]
    public string Name { get; set; } = string.Empty;

    [Column("sort_order")]
    public int SortOrder { get; set; }

    [Column("is_active")]
    public bool IsActive { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_customers")]
public sealed class PosCustomer
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("name")]
    public string Name { get; set; } = string.Empty;

    [Column("phone")]
    public string? Phone { get; set; }

    [Column("discount_percent")]
    public decimal DiscountPercent { get; set; }

    [Column("is_active")]
    public bool IsActive { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_shifts")]
public sealed class PosShift
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("opened_by")]
    public Guid OpenedBy { get; set; }

    [Column("opened_at")]
    public DateTime OpenedAt { get; set; }

    [Column("opening_cash")]
    public decimal OpeningCash { get; set; }

    [Column("closed_by")]
    public Guid? ClosedBy { get; set; }

    [Column("closed_at")]
    public DateTime? ClosedAt { get; set; }

    [Column("closing_cash")]
    public decimal? ClosingCash { get; set; }

    [Column("note")]
    public string? Note { get; set; }
}

[Table("pos_orders")]
public sealed class PosOrder
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("business_date")]
    public DateOnly BusinessDate { get; set; }

    [Column("order_no")]
    public short OrderNo { get; set; }

    [Column("status")]
    public string Status { get; set; } = "open";

    [Column("table_id")]
    public Guid? TableId { get; set; }

    [Column("people_count")]
    public short? PeopleCount { get; set; }

    [Column("customer_id")]
    public Guid? CustomerId { get; set; }

    [Column("is_takeaway")]
    public bool IsTakeaway { get; set; }

    [Column("customer_discount_percent")]
    public decimal CustomerDiscountPercent { get; set; }

    [Column("discount_amount")]
    public decimal DiscountAmount { get; set; }

    [Column("discount_percent")]
    public decimal DiscountPercent { get; set; }

    [Column("service_fee")]
    public decimal ServiceFee { get; set; }

    [Column("service_fee_percent")]
    public decimal ServiceFeePercent { get; set; }

    [Column("created_by")]
    public Guid CreatedBy { get; set; }

    [Column("shift_id")]
    public Guid? ShiftId { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_order_items")]
public sealed class PosOrderItem
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("order_id")]
    public Guid OrderId { get; set; }

    [Column("menu_item_id")]
    public Guid? MenuItemId { get; set; }

    [Column("name")]
    public string Name { get; set; } = string.Empty;

    [Column("qty")]
    public decimal Qty { get; set; }

    [Column("unit_price")]
    public decimal UnitPrice { get; set; }

    [Column("discount_amount")]
    public decimal DiscountAmount { get; set; }

    [Column("discount_percent")]
    public decimal DiscountPercent { get; set; }

    [Column("voided")]
    public bool Voided { get; set; }

    [Column("void_reason")]
    public string? VoidReason { get; set; }

    [Column("voided_by")]
    public Guid? VoidedBy { get; set; }

    [Column("voided_at")]
    public DateTime? VoidedAt { get; set; }

    [Column("printer_id")]
    public Guid? PrinterId { get; set; }

    [Column("kitchen_printed_at")]
    public DateTime? KitchenPrintedAt { get; set; }

    [Column("created_by")]
    public Guid CreatedBy { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }

    [Column("note")]
    public string? Note { get; set; }

    [Column("customization_signature")]
    public string? CustomizationSignature { get; set; }
}

[Table("pos_order_item_customizations")]
public sealed class PosOrderItemCustomization
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("order_item_id")]
    public Guid OrderItemId { get; set; }

    [Column("group_id")]
    public Guid? GroupId { get; set; }

    [Column("option_id")]
    public Guid? OptionId { get; set; }

    [Column("group_name")]
    public string GroupName { get; set; } = string.Empty;

    [Column("option_name")]
    public string OptionName { get; set; } = string.Empty;

    [Column("qty")]
    public decimal Qty { get; set; }

    [Column("price_delta")]
    public decimal PriceDelta { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_payments")]
public sealed class PosPayment
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("order_id")]
    public Guid OrderId { get; set; }

    [Column("method")]
    public string Method { get; set; } = string.Empty;

    [Column("amount")]
    public decimal Amount { get; set; }

    [Column("reference")]
    public string? Reference { get; set; }

    [Column("created_by")]
    public Guid? CreatedBy { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_printers")]
public sealed class PosPrinter
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("name")]
    public string Name { get; set; } = string.Empty;

    [Column("type")]
    public string Type { get; set; } = "network";

    [Column("address")]
    public string? Address { get; set; }

    [Column("is_active")]
    public bool IsActive { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_app_settings")]
public sealed class PosAppSetting
{
    [Key]
    [Column("key")]
    public string Key { get; set; } = string.Empty;

    [Column("value")]
    public string? Value { get; set; }

    [Column("updated_at")]
    public DateTime UpdatedAt { get; set; }
}

[Table("pos_order_counters")]
public sealed class PosOrderCounter
{
    [Key]
    [Column("business_date")]
    public DateOnly BusinessDate { get; set; }

    [Column("next_no")]
    public int NextNo { get; set; }
}

[Table("pos_raw_materials")]
public sealed class PosRawMaterial
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("name")]
    public string Name { get; set; } = string.Empty;

    [Column("unit")]
    public string? Unit { get; set; }

    [Column("stock_qty")]
    public decimal StockQty { get; set; }

    [Column("is_active")]
    public bool IsActive { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_menu_item_materials")]
public sealed class PosMenuItemMaterial
{
    [Key]
    [Column("menu_item_id")]
    public Guid MenuItemId { get; set; }

    [Column("material_id")]
    public Guid MaterialId { get; set; }

    [Column("qty")]
    public decimal Qty { get; set; }
}

[Table("pos_print_queue")]
public sealed class PosPrintQueue
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("order_id")]
    public Guid OrderId { get; set; }

    [Column("printer_id")]
    public Guid PrinterId { get; set; }

    [Column("kind")]
    public string Kind { get; set; } = "kitchen";

    [Column("status")]
    public string Status { get; set; } = "pending";

    [Column("attempts")]
    public int Attempts { get; set; }

    [Column("last_error")]
    public string? LastError { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }

    [Column("updated_at")]
    public DateTime UpdatedAt { get; set; }
}

[Table("pos_accounts")]
public sealed class PosAccount
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("name")]
    public string Name { get; set; } = string.Empty;

    [Column("type")]
    public string Type { get; set; } = "cash";

    [Column("currency")]
    public string Currency { get; set; } = "ILS";

    [Column("is_active")]
    public bool IsActive { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_payment_method_accounts")]
public sealed class PosPaymentMethodAccount
{
    [Key]
    [Column("method")]
    public string Method { get; set; } = string.Empty;

    [Column("account_id")]
    public Guid AccountId { get; set; }
}

[Table("pos_suppliers")]
public sealed class PosSupplier
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("name")]
    public string Name { get; set; } = string.Empty;

    [Column("phone")]
    public string? Phone { get; set; }

    [Column("email")]
    public string? Email { get; set; }

    [Column("note")]
    public string? Note { get; set; }

    [Column("is_active")]
    public bool IsActive { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_receipts")]
public sealed class PosReceipt
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("receipt_date")]
    public DateOnly ReceiptDate { get; set; }

    [Column("source")]
    public string? Source { get; set; }

    [Column("supplier_id")]
    public Guid? SupplierId { get; set; }

    [Column("amount")]
    public decimal Amount { get; set; }

    [Column("method")]
    public string Method { get; set; } = string.Empty;

    [Column("account_id")]
    public Guid? AccountId { get; set; }

    [Column("note")]
    public string? Note { get; set; }

    [Column("created_by")]
    public Guid CreatedBy { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_expenses")]
public sealed class PosExpense
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("expense_date")]
    public DateOnly ExpenseDate { get; set; }

    [Column("category")]
    public string Category { get; set; } = string.Empty;

    [Column("supplier_id")]
    public Guid? SupplierId { get; set; }

    [Column("amount")]
    public decimal Amount { get; set; }

    [Column("method")]
    public string Method { get; set; } = string.Empty;

    [Column("account_id")]
    public Guid? AccountId { get; set; }

    [Column("attachment_url")]
    public string? AttachmentUrl { get; set; }

    [Column("note")]
    public string? Note { get; set; }

    [Column("created_by")]
    public Guid CreatedBy { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_account_transfers")]
public sealed class PosAccountTransfer
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("from_account_id")]
    public Guid FromAccountId { get; set; }

    [Column("to_account_id")]
    public Guid ToAccountId { get; set; }

    [Column("amount")]
    public decimal Amount { get; set; }

    [Column("note")]
    public string? Note { get; set; }

    [Column("created_by")]
    public Guid CreatedBy { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_account_transactions")]
public sealed class PosAccountTransaction
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("account_id")]
    public Guid AccountId { get; set; }

    [Column("direction")]
    public string Direction { get; set; } = "in";

    [Column("amount")]
    public decimal Amount { get; set; }

    [Column("source_type")]
    public string SourceType { get; set; } = string.Empty;

    [Column("source_id")]
    public Guid? SourceId { get; set; }

    [Column("note")]
    public string? Note { get; set; }

    [Column("created_by")]
    public Guid CreatedBy { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }
}

[Table("pos_employees")]
public sealed class PosEmployee
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("name")]
    public string Name { get; set; } = string.Empty;

    [Column("pay_rate")]
    public decimal PayRate { get; set; }

    [Column("overtime_modifier")]
    public decimal OvertimeModifier { get; set; }

    [Column("overtime_threshold_hours")]
    public decimal OvertimeThresholdHours { get; set; }

    [Column("note")]
    public string? Note { get; set; }

    [Column("is_active")]
    public bool IsActive { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }

    [Column("updated_at")]
    public DateTime UpdatedAt { get; set; }
}

[Table("pos_employee_time_entries")]
public sealed class PosEmployeeTimeEntry
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("employee_id")]
    public Guid EmployeeId { get; set; }

    [Column("work_date")]
    public DateOnly WorkDate { get; set; }

    [Column("start_time")]
    public TimeOnly StartTime { get; set; }

    [Column("end_time")]
    public TimeOnly EndTime { get; set; }

    [Column("duration_hours")]
    public decimal DurationHours { get; set; }

    [Column("source")]
    public string Source { get; set; } = "manual";

    [Column("note")]
    public string? Note { get; set; }

    [Column("created_at")]
    public DateTime CreatedAt { get; set; }

    [Column("updated_at")]
    public DateTime UpdatedAt { get; set; }

    public PosEmployee? Employee { get; set; }
}
