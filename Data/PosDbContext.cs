using Microsoft.EntityFrameworkCore;
using ResPosBackend.Models;

namespace ResPosBackend.Data;

public sealed class PosDbContext(DbContextOptions<PosDbContext> options) : DbContext(options)
{
    public DbSet<PosUser> Users => Set<PosUser>();
    public DbSet<PosSession> Sessions => Set<PosSession>();
    public DbSet<PosCategory> Categories => Set<PosCategory>();
    public DbSet<PosMenuItem> MenuItems => Set<PosMenuItem>();
    public DbSet<PosMenuItemOptionGroup> MenuItemOptionGroups => Set<PosMenuItemOptionGroup>();
    public DbSet<PosMenuItemOption> MenuItemOptions => Set<PosMenuItemOption>();
    public DbSet<PosTable> Tables => Set<PosTable>();
    public DbSet<PosCustomer> Customers => Set<PosCustomer>();
    public DbSet<PosShift> Shifts => Set<PosShift>();
    public DbSet<PosOrder> Orders => Set<PosOrder>();
    public DbSet<PosOrderItem> OrderItems => Set<PosOrderItem>();
    public DbSet<PosOrderItemCustomization> OrderItemCustomizations => Set<PosOrderItemCustomization>();
    public DbSet<PosPayment> Payments => Set<PosPayment>();
    public DbSet<PosPrinter> Printers => Set<PosPrinter>();
    public DbSet<PosAppSetting> AppSettings => Set<PosAppSetting>();
    public DbSet<PosOrderCounter> OrderCounters => Set<PosOrderCounter>();
    public DbSet<PosRawMaterial> RawMaterials => Set<PosRawMaterial>();
    public DbSet<PosMenuItemMaterial> MenuItemMaterials => Set<PosMenuItemMaterial>();
    public DbSet<PosPrintQueue> PrintQueue => Set<PosPrintQueue>();
    public DbSet<PosAccount> Accounts => Set<PosAccount>();
    public DbSet<PosPaymentMethodAccount> PaymentMethodAccounts => Set<PosPaymentMethodAccount>();
    public DbSet<PosSupplier> Suppliers => Set<PosSupplier>();
    public DbSet<PosReceipt> Receipts => Set<PosReceipt>();
    public DbSet<PosExpense> Expenses => Set<PosExpense>();
    public DbSet<PosAccountTransfer> AccountTransfers => Set<PosAccountTransfer>();
    public DbSet<PosAccountTransaction> AccountTransactions => Set<PosAccountTransaction>();
    public DbSet<PosEmployee> Employees => Set<PosEmployee>();
    public DbSet<PosEmployeeTimeEntry> EmployeeTimeEntries => Set<PosEmployeeTimeEntry>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PosSession>()
            .HasOne(x => x.User)
            .WithMany()
            .HasForeignKey(x => x.UserId)
            .OnDelete(DeleteBehavior.Cascade);

        modelBuilder.Entity<PosMenuItemMaterial>()
            .HasKey(x => new { x.MenuItemId, x.MaterialId });

        modelBuilder.Entity<PosOrder>().Property(x => x.BusinessDate).HasColumnType("date");
        modelBuilder.Entity<PosOrderCounter>().Property(x => x.BusinessDate).HasColumnType("date");
        modelBuilder.Entity<PosReceipt>().Property(x => x.ReceiptDate).HasColumnType("date");
        modelBuilder.Entity<PosExpense>().Property(x => x.ExpenseDate).HasColumnType("date");
        modelBuilder.Entity<PosEmployeeTimeEntry>().Property(x => x.WorkDate).HasColumnType("date");
        modelBuilder.Entity<PosEmployeeTimeEntry>().Property(x => x.StartTime).HasColumnType("time");
        modelBuilder.Entity<PosEmployeeTimeEntry>().Property(x => x.EndTime).HasColumnType("time");

        modelBuilder.Entity<PosOrderItem>().Property(x => x.Qty).HasPrecision(12, 3);
        modelBuilder.Entity<PosOrderItemCustomization>().Property(x => x.Qty).HasPrecision(12, 3);
        modelBuilder.Entity<PosMenuItem>().Property(x => x.StockQty).HasPrecision(12, 3);
        modelBuilder.Entity<PosRawMaterial>().Property(x => x.StockQty).HasPrecision(12, 3);
        modelBuilder.Entity<PosMenuItemMaterial>().Property(x => x.Qty).HasPrecision(12, 3);
        modelBuilder.Entity<PosEmployee>().Property(x => x.OvertimeModifier).HasPrecision(6, 3);
        modelBuilder.Entity<PosEmployee>().Property(x => x.OvertimeThresholdHours).HasPrecision(6, 2);
        modelBuilder.Entity<PosEmployeeTimeEntry>().Property(x => x.DurationHours).HasPrecision(6, 2);

        modelBuilder.Entity<PosMenuItem>().Property(x => x.Price).HasPrecision(12, 2);
        modelBuilder.Entity<PosMenuItemOption>().Property(x => x.PriceDelta).HasPrecision(12, 2);
        modelBuilder.Entity<PosCustomer>().Property(x => x.DiscountPercent).HasPrecision(5, 2);
        modelBuilder.Entity<PosOrder>().Property(x => x.CustomerDiscountPercent).HasPrecision(5, 2);
        modelBuilder.Entity<PosOrder>().Property(x => x.DiscountAmount).HasPrecision(12, 2);
        modelBuilder.Entity<PosOrder>().Property(x => x.DiscountPercent).HasPrecision(5, 2);
        modelBuilder.Entity<PosOrder>().Property(x => x.ServiceFee).HasPrecision(12, 2);
        modelBuilder.Entity<PosOrder>().Property(x => x.ServiceFeePercent).HasPrecision(5, 2);
        modelBuilder.Entity<PosOrderItem>().Property(x => x.UnitPrice).HasPrecision(12, 2);
        modelBuilder.Entity<PosOrderItem>().Property(x => x.DiscountAmount).HasPrecision(12, 2);
        modelBuilder.Entity<PosOrderItem>().Property(x => x.DiscountPercent).HasPrecision(5, 2);
        modelBuilder.Entity<PosPayment>().Property(x => x.Amount).HasPrecision(12, 2);
        modelBuilder.Entity<PosShift>().Property(x => x.OpeningCash).HasPrecision(12, 2);
        modelBuilder.Entity<PosShift>().Property(x => x.ClosingCash).HasPrecision(12, 2);
        modelBuilder.Entity<PosReceipt>().Property(x => x.Amount).HasPrecision(12, 2);
        modelBuilder.Entity<PosExpense>().Property(x => x.Amount).HasPrecision(12, 2);
        modelBuilder.Entity<PosAccountTransfer>().Property(x => x.Amount).HasPrecision(12, 2);
        modelBuilder.Entity<PosAccountTransaction>().Property(x => x.Amount).HasPrecision(12, 2);
        modelBuilder.Entity<PosEmployee>().Property(x => x.PayRate).HasPrecision(12, 2);

        modelBuilder.Entity<PosEmployeeTimeEntry>()
            .HasOne(x => x.Employee)
            .WithMany()
            .HasForeignKey(x => x.EmployeeId)
            .OnDelete(DeleteBehavior.Cascade);

        modelBuilder.Entity<PosEmployeeTimeEntry>()
            .HasIndex(x => new { x.EmployeeId, x.WorkDate });

        modelBuilder.Entity<PosEmployeeTimeEntry>()
            .HasIndex(x => new { x.EmployeeId, x.WorkDate, x.StartTime, x.EndTime })
            .IsUnique();

        base.OnModelCreating(modelBuilder);
    }
}
