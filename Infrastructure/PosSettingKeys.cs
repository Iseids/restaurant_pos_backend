namespace ResPosBackend.Infrastructure;

public static class PosSettingKeys
{
    public const string DefaultReceiptPrinterId = "default_receipt_printer_id";
    public const string DefaultInvoicePrinterId = "default_invoice_printer_id";
    public const string DefaultCashierDocumentsPrinterId = "default_cashier_documents_printer_id";
    public const string CashierExpensesEnabled = "cashier_expenses_enabled";
    public const string CashierExpensesCapAmount = "cashier_expenses_cap_amount";
    public const string DefaultCurrencyCode = "default_currency_code";
    public const string CurrenciesConfig = "currencies_config";
    public const string InvoiceTemplateConfig = "invoice_template_config";
}
