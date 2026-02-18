using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using PosBackend.AspNet.Data;
using PosBackend.AspNet.Models;

namespace PosBackend.AspNet.Services;

public sealed class PayrollService(PosDbContext db)
{
    public async Task<List<object>> ListEmployees(bool includeInactive, CancellationToken ct)
    {
        var query = db.Employees.AsNoTracking();
        if (!includeInactive)
        {
            query = query.Where(x => x.IsActive);
        }

        var items = await query.OrderBy(x => x.Name).ToListAsync(ct);
        return items.Select(ToEmployeeJson).ToList();
    }

    public async Task<object> CreateEmployee(
        string name,
        double payRate,
        double overtimeModifier,
        double overtimeThresholdHours,
        string? note,
        CancellationToken ct)
    {
        ValidateEmployeeInputs(payRate, overtimeModifier, overtimeThresholdHours);

        var now = DateTime.UtcNow;
        var entity = new PosEmployee
        {
            Id = Guid.NewGuid(),
            Name = name.Trim(),
            PayRate = (decimal)payRate,
            OvertimeModifier = (decimal)overtimeModifier,
            OvertimeThresholdHours = (decimal)overtimeThresholdHours,
            Note = string.IsNullOrWhiteSpace(note) ? null : note.Trim(),
            IsActive = true,
            CreatedAt = now,
            UpdatedAt = now,
        };

        db.Employees.Add(entity);
        await SaveWithConflictHandling(ct);
        return ToEmployeeJson(entity);
    }

    public async Task<object?> UpdateEmployee(
        Guid id,
        string? name,
        double? payRate,
        double? overtimeModifier,
        double? overtimeThresholdHours,
        string? note,
        bool hasNote,
        bool? isActive,
        CancellationToken ct)
    {
        var entity = await db.Employees.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(name))
        {
            entity.Name = name.Trim();
        }

        if (payRate.HasValue)
        {
            if (payRate.Value <= 0)
            {
                throw new InvalidOperationException("BAD_PAY_RATE");
            }

            entity.PayRate = (decimal)payRate.Value;
        }

        if (overtimeModifier.HasValue)
        {
            if (overtimeModifier.Value < 1)
            {
                throw new InvalidOperationException("BAD_OVERTIME_MODIFIER");
            }

            entity.OvertimeModifier = (decimal)overtimeModifier.Value;
        }

        if (overtimeThresholdHours.HasValue)
        {
            if (overtimeThresholdHours.Value <= 0 || overtimeThresholdHours.Value > 24)
            {
                throw new InvalidOperationException("BAD_OVERTIME_THRESHOLD");
            }

            entity.OvertimeThresholdHours = (decimal)overtimeThresholdHours.Value;
        }

        if (hasNote)
        {
            entity.Note = string.IsNullOrWhiteSpace(note) ? null : note.Trim();
        }

        if (isActive.HasValue)
        {
            entity.IsActive = isActive.Value;
        }

        entity.UpdatedAt = DateTime.UtcNow;
        await SaveWithConflictHandling(ct);
        return ToEmployeeJson(entity);
    }

    public async Task<List<object>?> ListTimeEntries(Guid employeeId, DateOnly? start, DateOnly? end, CancellationToken ct)
    {
        var employee = await db.Employees.AsNoTracking().FirstOrDefaultAsync(x => x.Id == employeeId, ct);
        if (employee is null)
        {
            return null;
        }

        var query = db.EmployeeTimeEntries
            .AsNoTracking()
            .Where(x => x.EmployeeId == employeeId);

        if (start.HasValue)
        {
            query = query.Where(x => x.WorkDate >= start.Value);
        }

        if (end.HasValue)
        {
            query = query.Where(x => x.WorkDate <= end.Value);
        }

        var items = await query
            .OrderByDescending(x => x.WorkDate)
            .ThenByDescending(x => x.StartTime)
            .ToListAsync(ct);

        return items.Select(ToTimeEntryJson).ToList();
    }

    public async Task<object> CreateTimeEntry(
        Guid employeeId,
        DateOnly workDate,
        TimeOnly startTime,
        TimeOnly endTime,
        string? note,
        string source,
        CancellationToken ct)
    {
        await EnsureEmployeeExists(employeeId, ct);

        var durationHours = ComputeDurationHours(startTime, endTime);
        var now = DateTime.UtcNow;
        var entity = new PosEmployeeTimeEntry
        {
            Id = Guid.NewGuid(),
            EmployeeId = employeeId,
            WorkDate = workDate,
            StartTime = startTime,
            EndTime = endTime,
            DurationHours = (decimal)durationHours,
            Source = string.IsNullOrWhiteSpace(source) ? "manual" : source.Trim().ToLowerInvariant(),
            Note = string.IsNullOrWhiteSpace(note) ? null : note.Trim(),
            CreatedAt = now,
            UpdatedAt = now,
        };

        db.EmployeeTimeEntries.Add(entity);
        await SaveWithConflictHandling(ct);
        return ToTimeEntryJson(entity);
    }

    public async Task<object?> UpdateTimeEntry(
        Guid id,
        DateOnly? workDate,
        TimeOnly? startTime,
        TimeOnly? endTime,
        string? note,
        bool hasNote,
        CancellationToken ct)
    {
        var entity = await db.EmployeeTimeEntries.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return null;
        }

        if (workDate.HasValue)
        {
            entity.WorkDate = workDate.Value;
        }

        if (startTime.HasValue)
        {
            entity.StartTime = startTime.Value;
        }

        if (endTime.HasValue)
        {
            entity.EndTime = endTime.Value;
        }

        if (hasNote)
        {
            entity.Note = string.IsNullOrWhiteSpace(note) ? null : note.Trim();
        }

        entity.DurationHours = (decimal)ComputeDurationHours(entity.StartTime, entity.EndTime);
        entity.UpdatedAt = DateTime.UtcNow;

        await SaveWithConflictHandling(ct);
        return ToTimeEntryJson(entity);
    }

    public async Task<bool> DeleteTimeEntry(Guid id, CancellationToken ct)
    {
        var entity = await db.EmployeeTimeEntries.FirstOrDefaultAsync(x => x.Id == id, ct);
        if (entity is null)
        {
            return false;
        }

        db.EmployeeTimeEntries.Remove(entity);
        await db.SaveChangesAsync(ct);
        return true;
    }

    public async Task<object?> ImportCsv(Guid employeeId, string csvContent, CancellationToken ct)
    {
        await EnsureEmployeeExists(employeeId, ct);

        var candidates = new List<CsvCandidate>();
        var errors = new List<object>();
        var inCsv = new HashSet<string>(StringComparer.Ordinal);
        using var reader = new StringReader(csvContent);

        var lineNo = 0;
        while (reader.ReadLine() is { } line)
        {
            lineNo++;
            var raw = line.Trim();
            if (string.IsNullOrWhiteSpace(raw))
            {
                continue;
            }

            if (lineNo == 1 && IsHeaderRow(raw))
            {
                continue;
            }

            var row = raw;
            if (row.StartsWith('[') && row.EndsWith(']') && row.Length >= 2)
            {
                row = row[1..^1];
            }

            var parts = row.Split(',', StringSplitOptions.TrimEntries);
            if (parts.Length != 3)
            {
                errors.Add(new { line = lineNo, row = raw, error = "Expected format: date,start,end" });
                continue;
            }

            if (!DateOnly.TryParse(parts[0], out var date))
            {
                errors.Add(new { line = lineNo, row = raw, error = "Invalid date" });
                continue;
            }

            if (!TryParseTime(parts[1], out var startTime))
            {
                errors.Add(new { line = lineNo, row = raw, error = "Invalid start time" });
                continue;
            }

            if (!TryParseTime(parts[2], out var endTime))
            {
                errors.Add(new { line = lineNo, row = raw, error = "Invalid end time" });
                continue;
            }

            double durationHours;
            try
            {
                durationHours = ComputeDurationHours(startTime, endTime);
            }
            catch
            {
                errors.Add(new { line = lineNo, row = raw, error = "Invalid time span" });
                continue;
            }

            var key = BuildEntryKey(date, startTime, endTime);
            if (!inCsv.Add(key))
            {
                continue;
            }

            candidates.Add(new CsvCandidate(date, startTime, endTime, durationHours));
        }

        var created = 0;
        var skipped = 0;
        if (candidates.Count > 0)
        {
            var minDate = candidates.Min(x => x.Date);
            var maxDate = candidates.Max(x => x.Date);

            var existingRows = await db.EmployeeTimeEntries
                .AsNoTracking()
                .Where(x => x.EmployeeId == employeeId && x.WorkDate >= minDate && x.WorkDate <= maxDate)
                .Select(x => new { x.WorkDate, x.StartTime, x.EndTime })
                .ToListAsync(ct);

            var existing = existingRows
                .Select(x => BuildEntryKey(x.WorkDate, x.StartTime, x.EndTime))
                .ToHashSet(StringComparer.Ordinal);

            var now = DateTime.UtcNow;
            foreach (var row in candidates)
            {
                var key = BuildEntryKey(row.Date, row.StartTime, row.EndTime);
                if (existing.Contains(key))
                {
                    skipped++;
                    continue;
                }

                db.EmployeeTimeEntries.Add(new PosEmployeeTimeEntry
                {
                    Id = Guid.NewGuid(),
                    EmployeeId = employeeId,
                    WorkDate = row.Date,
                    StartTime = row.StartTime,
                    EndTime = row.EndTime,
                    DurationHours = (decimal)row.DurationHours,
                    Source = "csv",
                    Note = null,
                    CreatedAt = now,
                    UpdatedAt = now,
                });

                existing.Add(key);
                created++;
            }

            if (created > 0)
            {
                await SaveWithConflictHandling(ct);
            }
        }

        return new
        {
            created,
            skipped,
            errors,
            parsed = candidates.Count,
        };
    }

    public async Task<object?> GetPayrollSummary(DateOnly start, DateOnly end, Guid? employeeId, CancellationToken ct)
    {
        if (start > end)
        {
            throw new InvalidOperationException("BAD_DATE_RANGE");
        }

        if (employeeId.HasValue)
        {
            var employeeExists = await db.Employees.AsNoTracking().AnyAsync(x => x.Id == employeeId.Value, ct);
            if (!employeeExists)
            {
                return null;
            }
        }

        var rows = await (
            from entry in db.EmployeeTimeEntries.AsNoTracking()
            join emp in db.Employees.AsNoTracking() on entry.EmployeeId equals emp.Id
            where entry.WorkDate >= start && entry.WorkDate <= end
                  && (!employeeId.HasValue || entry.EmployeeId == employeeId.Value)
            select new PayrollRow
            {
                EmployeeId = emp.Id,
                EmployeeName = emp.Name,
                PayRate = emp.PayRate,
                OvertimeModifier = emp.OvertimeModifier,
                OvertimeThresholdHours = emp.OvertimeThresholdHours,
                WorkDate = entry.WorkDate,
                StartTime = entry.StartTime,
                EndTime = entry.EndTime,
                DurationHours = entry.DurationHours,
            }).ToListAsync(ct);

        var summaries = rows
            .GroupBy(x => new
            {
                x.EmployeeId,
                x.EmployeeName,
                x.PayRate,
                x.OvertimeModifier,
                x.OvertimeThresholdHours,
            })
            .Select(group =>
            {
                var regularHours = group.Sum(x => Math.Min((double)x.DurationHours, (double)x.OvertimeThresholdHours));
                var overtimeHours = group.Sum(x => Math.Max((double)x.DurationHours - (double)x.OvertimeThresholdHours, 0));
                var totalHours = regularHours + overtimeHours;
                var payRate = (double)group.Key.PayRate;
                var overtimeModifier = (double)group.Key.OvertimeModifier;
                var basePay = regularHours * payRate;
                var overtimePay = overtimeHours * payRate * overtimeModifier;
                var grossPay = basePay + overtimePay;
                var shiftCount = group.Count();
                var dayCount = group.Select(x => x.WorkDate).Distinct().Count();
                var overnightCount = group.Count(x => x.EndTime <= x.StartTime);
                var longestShift = group.Max(x => (double)x.DurationHours);

                return new PayrollSummaryItem
                {
                    EmployeeId = group.Key.EmployeeId,
                    EmployeeName = group.Key.EmployeeName,
                    PayRate = payRate,
                    OvertimeModifier = overtimeModifier,
                    OvertimeThresholdHours = (double)group.Key.OvertimeThresholdHours,
                    ShiftCount = shiftCount,
                    DayCount = dayCount,
                    OvernightShiftCount = overnightCount,
                    TotalHours = totalHours,
                    RegularHours = regularHours,
                    OvertimeHours = overtimeHours,
                    AverageShiftHours = shiftCount == 0 ? 0 : totalHours / shiftCount,
                    LongestShiftHours = longestShift,
                    BasePay = basePay,
                    OvertimePay = overtimePay,
                    GrossPay = grossPay,
                };
            })
            .OrderBy(x => x.EmployeeName)
            .ToList();

        var items = summaries.Select(x => (object)new
        {
            employeeId = x.EmployeeId,
            employeeName = x.EmployeeName,
            payRate = x.PayRate,
            overtimeModifier = x.OvertimeModifier,
            overtimeThresholdHours = x.OvertimeThresholdHours,
            shiftCount = x.ShiftCount,
            dayCount = x.DayCount,
            overnightShiftCount = x.OvernightShiftCount,
            totalHours = x.TotalHours,
            regularHours = x.RegularHours,
            overtimeHours = x.OvertimeHours,
            averageShiftHours = x.AverageShiftHours,
            longestShiftHours = x.LongestShiftHours,
            basePay = x.BasePay,
            overtimePay = x.OvertimePay,
            grossPay = x.GrossPay,
        }).ToList();

        var totalHoursAll = summaries.Sum(x => x.TotalHours);
        var regularHoursAll = summaries.Sum(x => x.RegularHours);
        var overtimeHoursAll = summaries.Sum(x => x.OvertimeHours);
        var grossPayAll = summaries.Sum(x => x.GrossPay);
        var shiftsAll = summaries.Sum(x => x.ShiftCount);

        return new
        {
            start = start.ToString("yyyy-MM-dd"),
            end = end.ToString("yyyy-MM-dd"),
            totals = new
            {
                employees = items.Count,
                shifts = shiftsAll,
                totalHours = totalHoursAll,
                regularHours = regularHoursAll,
                overtimeHours = overtimeHoursAll,
                grossPay = grossPayAll,
            },
            items,
        };
    }

    private static object ToEmployeeJson(PosEmployee x)
    {
        return new
        {
            id = x.Id,
            name = x.Name,
            payRate = (double)x.PayRate,
            overtimeModifier = (double)x.OvertimeModifier,
            overtimeThresholdHours = (double)x.OvertimeThresholdHours,
            note = x.Note,
            isActive = x.IsActive,
            createdAt = x.CreatedAt,
            updatedAt = x.UpdatedAt,
        };
    }

    private static object ToTimeEntryJson(PosEmployeeTimeEntry x)
    {
        return new
        {
            id = x.Id,
            employeeId = x.EmployeeId,
            date = x.WorkDate.ToString("yyyy-MM-dd"),
            start = x.StartTime.ToString("HH':'mm"),
            end = x.EndTime.ToString("HH':'mm"),
            durationHours = (double)x.DurationHours,
            source = x.Source,
            note = x.Note,
            createdAt = x.CreatedAt,
            updatedAt = x.UpdatedAt,
            isOvernight = x.EndTime <= x.StartTime,
        };
    }

    private static string BuildEntryKey(DateOnly date, TimeOnly start, TimeOnly end)
    {
        return $"{date:yyyy-MM-dd}|{start:HH\\:mm\\:ss}|{end:HH\\:mm\\:ss}";
    }

    private static bool IsHeaderRow(string line)
    {
        var lower = line.Trim().ToLowerInvariant();
        return lower is "date,start,end" or "[date,start,end]";
    }

    private static bool TryParseTime(string raw, out TimeOnly time)
    {
        if (TimeOnly.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.None, out time))
        {
            return true;
        }

        var trimmed = raw.Trim();
        if (trimmed.Length == 4 &&
            int.TryParse(trimmed[..2], out var hour) &&
            int.TryParse(trimmed[2..], out var minute) &&
            hour >= 0 && hour <= 23 &&
            minute >= 0 && minute <= 59)
        {
            time = new TimeOnly(hour, minute);
            return true;
        }

        return false;
    }

    private static double ComputeDurationHours(TimeOnly start, TimeOnly end)
    {
        var startMinutes = start.Hour * 60 + start.Minute + (start.Second / 60d);
        var endMinutes = end.Hour * 60 + end.Minute + (end.Second / 60d);
        if (endMinutes <= startMinutes)
        {
            endMinutes += 24 * 60;
        }

        var durationHours = (endMinutes - startMinutes) / 60d;
        if (durationHours <= 0 || durationHours >= 24)
        {
            throw new InvalidOperationException("BAD_TIME_SPAN");
        }

        return Math.Round(durationHours, 2, MidpointRounding.AwayFromZero);
    }

    private static void ValidateEmployeeInputs(double payRate, double overtimeModifier, double overtimeThresholdHours)
    {
        if (payRate <= 0)
        {
            throw new InvalidOperationException("BAD_PAY_RATE");
        }

        if (overtimeModifier < 1)
        {
            throw new InvalidOperationException("BAD_OVERTIME_MODIFIER");
        }

        if (overtimeThresholdHours <= 0 || overtimeThresholdHours > 24)
        {
            throw new InvalidOperationException("BAD_OVERTIME_THRESHOLD");
        }
    }

    private async Task EnsureEmployeeExists(Guid employeeId, CancellationToken ct)
    {
        var exists = await db.Employees.AsNoTracking().AnyAsync(x => x.Id == employeeId, ct);
        if (!exists)
        {
            throw new InvalidOperationException("EMPLOYEE_NOT_FOUND");
        }
    }

    private async Task SaveWithConflictHandling(CancellationToken ct)
    {
        try
        {
            await db.SaveChangesAsync(ct);
        }
        catch (DbUpdateException ex) when (ex.InnerException is PostgresException pg && pg.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            throw new InvalidOperationException("CONFLICT");
        }
    }

    private sealed record CsvCandidate(DateOnly Date, TimeOnly StartTime, TimeOnly EndTime, double DurationHours);

    private sealed class PayrollRow
    {
        public Guid EmployeeId { get; set; }
        public string EmployeeName { get; set; } = string.Empty;
        public decimal PayRate { get; set; }
        public decimal OvertimeModifier { get; set; }
        public decimal OvertimeThresholdHours { get; set; }
        public DateOnly WorkDate { get; set; }
        public TimeOnly StartTime { get; set; }
        public TimeOnly EndTime { get; set; }
        public decimal DurationHours { get; set; }
    }

    private sealed class PayrollSummaryItem
    {
        public Guid EmployeeId { get; set; }
        public string EmployeeName { get; set; } = string.Empty;
        public double PayRate { get; set; }
        public double OvertimeModifier { get; set; }
        public double OvertimeThresholdHours { get; set; }
        public int ShiftCount { get; set; }
        public int DayCount { get; set; }
        public int OvernightShiftCount { get; set; }
        public double TotalHours { get; set; }
        public double RegularHours { get; set; }
        public double OvertimeHours { get; set; }
        public double AverageShiftHours { get; set; }
        public double LongestShiftHours { get; set; }
        public double BasePay { get; set; }
        public double OvertimePay { get; set; }
        public double GrossPay { get; set; }
    }
}
