from decimal import Decimal
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional
from src.utils.clickhouse_client import get_client
from src.utils.constants import Constants
from src.utils.query_helper import RevenueQueryHelper


class KPIAdjustmentCalculator:
    def __init__(self, constants: Constants):
        self.client = get_client()
        self.constants = constants
        self.revenue_helper = RevenueQueryHelper()
    
    def get_avg_rev_normal_day_30_days(self) -> Decimal:
        return self.revenue_helper.get_avg_rev_normal_day_30_days()
    
    def calculate_eom(self, target_year: int, target_month: int) -> Optional[Decimal]:
        sum_actual = self.revenue_helper.get_daily_actual_sum_for_eom_calculation(target_year, target_month)
        
        if sum_actual == 0:
            return None
        
        actual_dates = self.revenue_helper.get_actual_dates(target_year, target_month)
        print(f"DEBUG: actual_dates = {actual_dates}")
        
        if not actual_dates:
            return None
        
        today = date.today()
        
        if target_month == 12:
            last_day_of_month = date(target_year, 12, 31)
        else:
            next_month = date(target_year, target_month + 1, 1)
            last_day_of_month = next_month - timedelta(days=1)
        
        start_date = None
        if today > last_day_of_month:
            remaining_days_by_label = {}
            print(f"DEBUG: today = {today}, month has ended, remaining_days_by_label = {remaining_days_by_label}")
        else:
            if today.year == target_year and today.month == target_month:
                start_date = today
            else:
                start_date = date(target_year, target_month, 1)
            
            remaining_days_count_query = f"""
                SELECT 
                    date_label,
                    COUNT(*) as day_count
                FROM dim_date
                WHERE year = {target_year}
                  AND month = {target_month}
                  AND calendar_date >= '{start_date}'
                  AND calendar_date <= '{last_day_of_month}'
                  AND NOT (
                      (month = 6 AND day = 6) OR
                      (month = 9 AND day = 9) OR
                      (month = 11 AND day = 11) OR
                      (month = 12 AND day = 12)
                  )
                GROUP BY date_label
            """
            
            remaining_days_result = self.client.query(remaining_days_count_query)
            remaining_days_by_label = {row[0]: int(row[1]) for row in remaining_days_result.result_rows}
            print(f"DEBUG: today = {today}, start_date = {start_date}, remaining_days_by_label = {remaining_days_by_label}")
        
        avg_total_normal_day = self.get_avg_rev_normal_day_30_days()
        metadata_query = f"""
            SELECT 
                date_label,
                uplift,
                row_number() OVER (
                    PARTITION BY year, month, date_label
                    ORDER BY updated_at DESC
                ) AS rn
            FROM hskcdp.kpi_day_metadata
            WHERE year = {target_year}
              AND month = {target_month}
        """
        
        metadata_result = self.client.query(metadata_query)
        metadata_by_label = {}
        
        for row in metadata_result.result_rows:
            if row[2] == 1:
                date_label = row[0]
                uplift = Decimal(str(row[1]))
                metadata_by_label[date_label] = uplift
        
        sum_rev_eom = Decimal('0')
        for date_label, day_count in remaining_days_by_label.items():
            uplift = metadata_by_label.get(date_label, Decimal('1.0'))
            rev_eom_for_label = Decimal(str(day_count)) * avg_total_normal_day * uplift
            sum_rev_eom += rev_eom_for_label
        
        print(f"DEBUG EOM calculation for month {target_month}:")
        print(f"  - Sum(actual) = {sum_actual}")
        print(f"  - Today = {today}")
        if today <= last_day_of_month:
            print(f"  - Start date (remaining from) = {start_date}")
        print(f"  - Avg total normal day (last 30 days) = {avg_total_normal_day}")
        print(f"  - Remaining days by label: {remaining_days_by_label}")
        print(f"  - Sum(rev eom) = {sum_rev_eom}")
        print(f"  - EOM = {sum_actual + sum_rev_eom}")
        
        eom = sum_actual + sum_rev_eom
        
        return eom
    
    def create_new_version_from_day_26(self, target_year: int, target_month: int) -> None:
        today = date.today()
        if today.day < 26 or today.month != target_month or today.year != target_year:
            return
        
        current_version = f"Thang {target_month}"

        # Determine next version
        if target_month == 12:
            next_version_number = 1
            next_year = target_year + 1
        else:
            next_version_number = target_month + 1
            next_year = target_year
        
        next_version = f"Thang {next_version_number}"
        
        print(f"DEBUG: Closing on day >= 26 - Creating new version")
        print(f"  - Current version: {current_version}")
        print(f"  - Next version: {next_version} (year {next_year})")
        
        # Get latest kpi_adjustment from current version for 12 months
        current_version_query = f"""
            SELECT
                month,
                kpi_adjustment
            FROM (
                SELECT
                    month,
                    kpi_adjustment,
                    row_number() OVER (
                        PARTITION BY year, month, version
                        ORDER BY updated_at DESC
                    ) AS rn
                FROM hskcdp.kpi_month FINAL
                WHERE year = {target_year}
                  AND version = '{current_version}'
            )
            WHERE rn = 1
            ORDER BY month
        """
        
        current_version_result = self.client.query(current_version_query)
        current_kpi_adjustments = {int(row[0]): float(row[1]) for row in current_version_result.result_rows}
        
        # If current version doesn't have all 12 months, get missing months from baseline "Thang 1"
        if len(current_kpi_adjustments) < 12:
            print(f"  - WARNING: Version '{current_version}' does not have all 12 months (has {len(current_kpi_adjustments)} months)")
            print(f"  - Getting missing months from baseline 'Thang 1'")
            
            baseline_query = f"""
                SELECT
                    month,
                    kpi_adjustment
                FROM (
                    SELECT
                        month,
                        kpi_adjustment,
                        row_number() OVER (
                            PARTITION BY year, month, version
                            ORDER BY updated_at DESC
                        ) AS rn
                    FROM hskcdp.kpi_month FINAL
                    WHERE year = {target_year}
                      AND version = 'Thang 1'
                )
                WHERE rn = 1
                ORDER BY month
            """
            baseline_result = self.client.query(baseline_query)
            baseline_kpi_adjustments = {int(row[0]): float(row[1]) for row in baseline_result.result_rows}
            
            # Fill missing months from baseline
            for month in range(1, 13):
                if month not in current_kpi_adjustments:
                    if month in baseline_kpi_adjustments:
                        current_kpi_adjustments[month] = baseline_kpi_adjustments[month]
                    else:
                        raise ValueError(f"Cannot find kpi_adjustment for month {month} in both version '{current_version}' and baseline 'Thang 1'")
        
        print(f"  - Getting kpi_adjustment from version '{current_version}' for 12 months:")
        for month in sorted(current_kpi_adjustments.keys()):
            print(f"    Month {month}: {current_kpi_adjustments[month]}")
        
        # Create new version with kpi_initial = kpi_adjustment from old version
        now = datetime.now()
        data = []
        
        for month in range(1, 13):
            kpi_initial = current_kpi_adjustments[month]
            
            data.append([
                next_version,
                next_year,
                month,
                kpi_initial,  # kpi_initial = kpi_adjustment from old version
                None,  # actual
                None,  # gap
                None,  # eom
                kpi_initial,  # kpi_adjustment (initially = kpi_initial)
                now,  # created_at
                now   # updated_at
            ])
        
        columns = [
            'version', 'year', 'month', 'kpi_initial', 'actual', 'gap',
            'eom', 'kpi_adjustment', 'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_month", data, column_names=columns)
        print(f"  - Created version '{next_version}' with kpi_initial from version '{current_version}'")
    
    def create_version_manually(
        self,
        source_month: int,
        target_year: int = None,
        force: bool = False
    ) -> None:
        """
        Args:
            source_month: Source month (e.g., 1 to create version 2 from version 1)
            target_year: Year (default is KPI_YEAR_2026)
            force: If True, will overwrite if target version already exists. If False, will raise error if version exists.
        """
        if target_year is None:
            target_year = self.constants.KPI_YEAR_2026
        
        source_version = f"Thang {source_month}"
        
        # Determine target version
        if source_month == 12:
            next_version_number = 1
            next_year = target_year + 1
        else:
            next_version_number = source_month + 1
            next_year = target_year
        
        next_version = f"Thang {next_version_number}"
        
        print(f"\n=== MANUALLY CREATING NEW VERSION ===")
        print(f"  - Source version: {source_version} (month {source_month})")
        print(f"  - Target version: {next_version} (month {next_version_number}, year {next_year})")
        
        # Check if target version already exists
        check_target_version_query = f"""
            SELECT COUNT(*) as cnt
            FROM hskcdp.kpi_month FINAL
            WHERE year = {next_year}
              AND version = '{next_version}'
        """
        check_target_result = self.client.query(check_target_version_query)
        target_version_exists = check_target_result.result_rows[0][0] > 0 if check_target_result.result_rows else False
        
        if target_version_exists and not force:
            raise ValueError(
                f"Version '{next_version}' already exists in database. "
                f"If you want to overwrite, use flag --force."
            )
        
        if target_version_exists and force:
            print(f"  - WARNING: Version '{next_version}' already exists, will overwrite due to --force flag")
        
        # Get latest kpi_adjustment from source version for 12 months
        source_version_query = f"""
            SELECT
                month,
                kpi_adjustment
            FROM (
                SELECT
                    month,
                    kpi_adjustment,
                    row_number() OVER (
                        PARTITION BY year, month, version
                        ORDER BY updated_at DESC
                    ) AS rn
                FROM hskcdp.kpi_month FINAL
                WHERE year = {target_year}
                  AND version = '{source_version}'
            )
            WHERE rn = 1
            ORDER BY month
        """
        
        source_version_result = self.client.query(source_version_query)
        source_kpi_adjustments = {int(row[0]): float(row[1]) for row in source_version_result.result_rows}
        
        # If source version doesn't have all 12 months, get missing months from baseline "Thang 1"
        if len(source_kpi_adjustments) < 12:
            print(f"  - WARNING: Version '{source_version}' does not have all 12 months (has {len(source_kpi_adjustments)} months)")
            print(f"  - Getting missing months from baseline 'Thang 1'")
            
            baseline_query = f"""
                SELECT
                    month,
                    kpi_adjustment
                FROM (
                    SELECT
                        month,
                        kpi_adjustment,
                        row_number() OVER (
                            PARTITION BY year, month, version
                            ORDER BY updated_at DESC
                        ) AS rn
                    FROM hskcdp.kpi_month FINAL
                    WHERE year = {target_year}
                      AND version = 'Thang 1'
                )
                WHERE rn = 1
                ORDER BY month
            """
            baseline_result = self.client.query(baseline_query)
            baseline_kpi_adjustments = {int(row[0]): float(row[1]) for row in baseline_result.result_rows}
            
            # Fill missing months from baseline
            for month in range(1, 13):
                if month not in source_kpi_adjustments:
                    if month in baseline_kpi_adjustments:
                        source_kpi_adjustments[month] = baseline_kpi_adjustments[month]
                    else:
                        raise ValueError(f"Cannot find kpi_adjustment for month {month} in both version '{source_version}' and baseline 'Thang 1'")
        
        print(f"  - Getting kpi_adjustment from version '{source_version}' for 12 months:")
        for month in sorted(source_kpi_adjustments.keys()):
            print(f"    Month {month}: {source_kpi_adjustments[month]}")
        
        # Create new version with kpi_initial = kpi_adjustment from old version
        # kpi_adjustment initially = kpi_initial (no actual yet, so not calculated)
        now = datetime.now()
        data = []
        
        for month in range(1, 13):
            kpi_initial = source_kpi_adjustments[month]
            
            data.append([
                next_version,
                next_year,
                month,
                kpi_initial,
                None,
                None,
                None,
                kpi_initial,
                now,
                now
            ])
        
        columns = [
            'version', 'year', 'month', 'kpi_initial', 'actual', 'gap',
            'eom', 'kpi_adjustment', 'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_month", data, column_names=columns)
        print(f"  - Created version '{next_version}' with kpi_initial from version '{source_version}'")
        print(f"=== FINISHED CREATING NEW VERSION ===\n")
    
    def get_sum_gap_from_version(self, version: str, target_year: int) -> Decimal:
        """
        Get total gap of a version.
        """
        query = f"""
            SELECT
                SUM(gap) as sum_gap
            FROM (
                SELECT
                    gap,
                    row_number() OVER (
                        PARTITION BY year, month, version
                        ORDER BY updated_at DESC
                    ) AS rn
                FROM hskcdp.kpi_month FINAL
                WHERE year = {target_year}
                  AND version = '{version}'
            )
            WHERE rn = 1
        """
        
        result = self.client.query(query)
        if result.result_rows and result.result_rows[0][0] is not None:
            return Decimal(str(result.result_rows[0][0]))
        return Decimal('0')
    
    def get_kpi_initial_from_version(self, version: str, month: int, target_year: int) -> float:
        """
        Get kpi_initial of a month from a version.
        """
        query = f"""
            SELECT
                kpi_initial
            FROM (
                SELECT
                    kpi_initial,
                    row_number() OVER (
                        PARTITION BY year, month, version
                        ORDER BY updated_at DESC
                    ) AS rn
                FROM hskcdp.kpi_month FINAL
                WHERE year = {target_year}
                  AND version = '{version}'
                  AND month = {month}
            )
            WHERE rn = 1
            LIMIT 1
        """
        
        result = self.client.query(query)
        if result.result_rows and result.result_rows[0][0] is not None:
            return float(result.result_rows[0][0])
        raise ValueError(f"kpi_initial not found for version '{version}', month {month}, year {target_year}")
    
    def recalculate_version_after_marketing_adjustment(
        self,
        version: str,
        adjusted_month: int,
        new_kpi_initial: float,
        target_year: int = None
    ) -> None:
        if target_year is None:
            target_year = self.constants.KPI_YEAR_2026
        
        baseline_version = "Thang 1"
        
        print(f"\n=== RECALCULATING VERSION AFTER MARKETING ADJUSTMENT ===")
        print(f"  - Version: {version}")
        print(f"  - Adjusted month: {adjusted_month}")
        print(f"  - New kpi_initial: {new_kpi_initial}")
        
        # Validation: Check if version exists
        check_version_query = f"""
            SELECT COUNT(*) as cnt
            FROM hskcdp.kpi_month FINAL
            WHERE year = {target_year}
              AND version = '{version}'
        """
        check_version_result = self.client.query(check_version_query)
        version_exists = check_version_result.result_rows[0][0] > 0 if check_version_result.result_rows else False
        
        if not version_exists:
            raise ValueError(
                f"Version '{version}' does not exist in database. "
                f"Please close numbers first (run on day 26) to create this version."
            )
        
        # Validation: Check if version has all 12 months
        check_months_query = f"""
            SELECT COUNT(DISTINCT month) as cnt
            FROM (
                SELECT
                    month,
                    row_number() OVER (
                        PARTITION BY year, month, version
                        ORDER BY updated_at DESC
                    ) AS rn
                FROM hskcdp.kpi_month FINAL
                WHERE year = {target_year}
                  AND version = '{version}'
            )
            WHERE rn = 1
        """
        check_months_result = self.client.query(check_months_query)
        months_count = check_months_result.result_rows[0][0] if check_months_result.result_rows else 0
        
        if months_count != 12:
            raise ValueError(
                f"Version '{version}' does not have all 12 months (currently has {months_count} months). "
                f"Please ensure version has been created completely."
            )
        
        # Validation: Check if adjusted_month is valid (must be next month)
        # Example: Currently in month 1 → can only adjust month 2
        today = date.today()
        current_month = today.month
        expected_adjusted_month = current_month + 1 if current_month < 12 else 1
        
        if adjusted_month != expected_adjusted_month:
            print(f"  WARNING: Adjusted month ({adjusted_month}) is not the next month ({expected_adjusted_month})")
            print(f"  Continuing calculation with month {adjusted_month}...")
        
        # 1. Get kpi_initial of adjusted month from version 1 (baseline)
        kpi_initial_version1_adjusted = self.get_kpi_initial_from_version(
            baseline_version, adjusted_month, target_year
        )
        
        # 2. Calculate difference of adjusted month
        adjusted_month_diff = Decimal(str(new_kpi_initial)) - Decimal(str(kpi_initial_version1_adjusted))
        
        # 3. Get Sum(gap) of version 1
        sum_gap_version1 = self.get_sum_gap_from_version(baseline_version, target_year)
        
        # 4. Calculate new total gap after adjustment
        total_adjusted_gap = sum_gap_version1 + adjusted_month_diff
        
        # 5. Calculate remaining months (only months AFTER adjusted month)
        # Example: adjust month 2 → only recalculate months 3-12
        # Months BEFORE adjusted month (1 to adjusted_month-1) remain unchanged
        remaining_months = [m for m in range(adjusted_month + 1, 13)]

        # 6. Calculate gap distribution for each remaining month
        if len(remaining_months) > 0:
            gap_per_remaining_month = total_adjusted_gap / Decimal(str(len(remaining_months)))
        else:
            gap_per_remaining_month = Decimal('0')
        
        # 7. Recalculate kpi_initial for remaining months and update to database
        now = datetime.now()
        data_to_update = []
        
        # Get created_at for adjusted month
        get_created_at_adjusted_query = f"""
            SELECT created_at
            FROM (
                SELECT
                    created_at,
                    row_number() OVER (
                        PARTITION BY year, month, version
                        ORDER BY updated_at DESC
                    ) AS rn
                FROM hskcdp.kpi_month FINAL
                WHERE year = {target_year}
                  AND version = '{version}'
                  AND month = {adjusted_month}
            )
            WHERE rn = 1
            LIMIT 1
        """
        created_at_adjusted_result = self.client.query(get_created_at_adjusted_query)
        created_at_adjusted = created_at_adjusted_result.result_rows[0][0] if created_at_adjusted_result.result_rows else now
        
        # Update adjusted month (keep marketing provided value)
        data_to_update.append([
            version,
            target_year,
            adjusted_month,
            new_kpi_initial,  # new kpi_initial from marketing
            None,  # actual (keep unchanged, will be recalculated later)
            None,  # gap (keep unchanged, will be recalculated later)
            None,  # eom (keep unchanged, will be recalculated later)
            new_kpi_initial,  # kpi_adjustment (temporarily = kpi_initial, will be recalculated later)
            created_at_adjusted,  # created_at (keep unchanged)
            now    # updated_at
        ])
        
        # Recalculate kpi_initial for months AFTER adjusted month
        print(f"\n  - Recalculating kpi_initial for months after month {adjusted_month}:")
        for month in remaining_months:
            kpi_initial_version1 = self.get_kpi_initial_from_version(
                baseline_version, month, target_year
            )
            kpi_initial_new = float(Decimal(str(kpi_initial_version1)) - gap_per_remaining_month)
            
            print(f"    Month {month}: {kpi_initial_version1} - {gap_per_remaining_month} = {kpi_initial_new}")
            
            # Get current created_at to keep unchanged
            get_created_at_query = f"""
                SELECT created_at
                FROM (
                    SELECT
                        created_at,
                        row_number() OVER (
                            PARTITION BY year, month, version
                            ORDER BY updated_at DESC
                        ) AS rn
                    FROM hskcdp.kpi_month FINAL
                    WHERE year = {target_year}
                      AND version = '{version}'
                      AND month = {month}
                )
                WHERE rn = 1
                LIMIT 1
            """
            created_at_result = self.client.query(get_created_at_query)
            created_at = created_at_result.result_rows[0][0] if created_at_result.result_rows else now
            
            data_to_update.append([
                version,
                target_year,
                month,
                kpi_initial_new,  # new kpi_initial
                None,  # actual (keep unchanged, will be recalculated later)
                None,  # gap (keep unchanged, will be recalculated later)
                None,  # eom (keep unchanged, will be recalculated later)
                kpi_initial_new,  # kpi_adjustment (temporarily = kpi_initial, will be recalculated later)
                created_at,  # created_at (keep unchanged)
                now    # updated_at
            ])
        
        # 8. Update to database
        columns = [
            'version', 'year', 'month', 'kpi_initial', 'actual', 'gap',
            'eom', 'kpi_adjustment', 'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_month", data_to_update, column_names=columns)
        print(f"\n  - Updated {len(data_to_update)} records to version '{version}'")
        print(f"=== FINISHED RECALCULATION ===\n")
    
    def calculate_kpi_adjustment(self, target_month: Optional[int] = None) -> List[Dict]:
        if target_month is None:
            today = date.today()
            if today.year == self.constants.KPI_YEAR_2026:
                target_month = today.month
            
        version = f"Thang {target_month}"

        # Check if current version already has kpi_initial
        current_version_query = f"""
            SELECT
                month,
                kpi_initial
            FROM (
                SELECT
                    month,
                    kpi_initial,
                    row_number() OVER (
                        PARTITION BY year, month, version
                        ORDER BY updated_at DESC
                    ) AS rn
                FROM hskcdp.kpi_month FINAL
                WHERE year = {self.constants.KPI_YEAR_2026}
                  AND version = '{version}'
            )
            WHERE rn = 1
            ORDER BY month
        """
        current_version_result = self.client.query(current_version_query)
        current_version_kpi = {int(row[0]): float(row[1]) for row in current_version_result.result_rows}
        
        if len(current_version_kpi) == 12:
            print(f"DEBUG: Version '{version}' already has kpi_initial, keeping current values")
            base_kpi = {}
            for month in range(1, 13):
                kpi_initial = current_version_kpi[month]
                base_kpi[month] = {'year': self.constants.KPI_YEAR_2026, 'month': month, 'kpi_initial': kpi_initial}
        else:
            baseline_version = "Thang 1"
            baseline_query = f"""
                SELECT
                    month,
                    kpi_initial
                FROM hskcdp.kpi_month FINAL
                WHERE year = {self.constants.KPI_YEAR_2026}
                  AND version = '{baseline_version}'
                ORDER BY month
            """
            baseline_result = self.client.query(baseline_query)
            baseline_kpi = {int(row[0]): float(row[1]) for row in baseline_result.result_rows}

            missing_months = [m for m in range(1, 13) if m not in baseline_kpi]
            if missing_months:
                raise ValueError(
                    f"Missing kpi_initial for version '{baseline_version}' in months: {missing_months}. "
                    f"Please seed all 12 months before running pipeline."
                )

            base_kpi = {}
            for month in range(1, 13):
                kpi_initial = baseline_kpi[month]
                base_kpi[month] = {'year': self.constants.KPI_YEAR_2026, 'month': month, 'kpi_initial': kpi_initial}
        
        actuals_month = self.revenue_helper.get_monthly_actual(self.constants.KPI_YEAR_2026)
        
        eoms = {}
        actuals_day = {}
        gaps = {}
        total_gap = Decimal('0')
        
        # Only calculate for months <= target_month
        # Example: version 1 (target_month=1) → only calculate month 1
        #          version 2 (target_month=2) → calculate months 1, 2
        for month in range(1, target_month + 1):
            kpi_initial = Decimal(str(base_kpi[month]['kpi_initial']))
            
            eom = self.calculate_eom(self.constants.KPI_YEAR_2026, month)
            
            if eom is not None:
                eoms[month] = eom
                gap = eom - kpi_initial
                gaps[month] = gap
                
                actuals_day[month] = self.revenue_helper.get_daily_actual_sum(
                    self.constants.KPI_YEAR_2026, month
                )
                
                total_gap += gap
            elif month in actuals_month:
                actual = Decimal(str(actuals_month[month]))
                gap = actual - kpi_initial
                gaps[month] = gap
                total_gap += gap
        
        # Gap distribution logic:
        # - For months <= target_month: distribute gap among months without actual
        # - For months > target_month: distribute total gap from months <= target_month
        months_with_actual = set(eoms.keys()) | set([m for m in actuals_month.keys() if m <= target_month])
        remaining_months_in_range = [m for m in range(1, target_month + 1) if m not in months_with_actual]
        remaining_months_count = len(remaining_months_in_range)
        
        # Gap per month for months <= target_month without actual
        if remaining_months_count > 0:
            gap_per_remaining_month = total_gap / Decimal(str(remaining_months_count))
        else:
            gap_per_remaining_month = Decimal('0')
        
        # Gap per month for months > target_month (distribute total gap from months <= target_month)
        months_after_target = 12 - target_month
        if months_after_target > 0 and total_gap != 0:
            gap_per_future_month = total_gap / Decimal(str(months_after_target))
        else:
            gap_per_future_month = Decimal('0')
        
        print(f"DEBUG Gap distribution:")
        print(f"  - Total gap (from months <= {target_month}) = {total_gap}")
        print(f"  - Months with actual (EOM or monthly): {sorted(months_with_actual)}")
        print(f"  - Remaining months in range (<= {target_month}) = {remaining_months_count}")
        print(f"  - Gap per remaining month (<= {target_month}) = {gap_per_remaining_month}")
        print(f"  - Months after target_month (> {target_month}) = {months_after_target}")
        print(f"  - Gap per future month (> {target_month}) = {gap_per_future_month}")
        
        
        results = []
        # Create results for all 12 months
        # - Months <= target_month: calculate actual/eom/gap if available
        # - Months > target_month: only kpi_adjustment from gap distribution (no actual/eom/gap)
        for month in range(1, 13):
            kpi_initial = Decimal(str(base_kpi[month]['kpi_initial']))
            
            if month <= target_month:
                # Calculate actual/eom/gap for months <= target_month
                if month in eoms:
                    eom_value = eoms[month]
                    gap = gaps[month]
                    kpi_adjustment = eom_value
                    actual = actuals_day[month]
                elif month in actuals_month:
                    actual = Decimal(str(actuals_month[month]))
                    gap = gaps[month]
                    kpi_adjustment = actual
                else:
                    # Month <= target_month but no actual yet → use gap distribution
                    actual = None
                    gap = None
                    kpi_adjustment = kpi_initial - gap_per_remaining_month
            else:
                # Months > target_month: no actual/eom/gap, only kpi_adjustment from gap distribution
                actual = None
                gap = None
                kpi_adjustment = kpi_initial - gap_per_future_month
            
            results.append({
                'version': version,
                'year': self.constants.KPI_YEAR_2026,
                'month': month,
                'kpi_initial': float(kpi_initial),
                'actual': float(actual) if actual is not None else None,
                'gap': float(gap) if gap is not None else None,
                'eom': float(eoms[month]) if month in eoms else None,
                'kpi_adjustment': float(kpi_adjustment)
            })
        
        return results
    
    def save_kpi_adjustment(self, target_month: Optional[int] = None) -> List[Dict]:
        if target_month is None:
            today = date.today()
            if today.year == self.constants.KPI_YEAR_2026:
                target_month = today.month
        
        results = self.calculate_kpi_adjustment(target_month)
        
        now = datetime.now()
        
        version = results[0]['version'] if results else None
        existing_created_at_query = f"""
            SELECT month, created_at
            FROM hskcdp.kpi_month FINAL
            WHERE year = {self.constants.KPI_YEAR_2026}
              AND version = '{version}'
        """
        existing_created_at_result = self.client.query(existing_created_at_query)
        existing_created_at = {row[0]: row[1] for row in existing_created_at_result.result_rows}
        
        data = []
        for row in results:
            month = row['month']
            created_at = existing_created_at.get(month, now)
            
            data.append([
                row['version'],
                row['year'],
                row['month'],
                row['kpi_initial'],
                row['actual'],
                row['gap'],
                row['eom'],
                row['kpi_adjustment'],
                created_at,
                now
            ])
        
        columns = [
            'version', 'year', 'month', 'kpi_initial', 'actual', 'gap',
            'eom', 'kpi_adjustment', 'created_at', 'updated_at'
        ]
        
        self.client.insert("hskcdp.kpi_month", data, column_names=columns)
        
        today = date.today()
        if today.day >= 26 and today.month == target_month and today.year == self.constants.KPI_YEAR_2026:
            print(f"Create new version from day 26")
            self.create_new_version_from_day_26(self.constants.KPI_YEAR_2026, target_month)
            print(f"Successfully created new version")
        
        return results


if __name__ == "__main__":
    import sys
    
    constants = Constants()
    calculator = KPIAdjustmentCalculator(constants)
    
    if len(sys.argv) > 1 and sys.argv[1] == "--create-version-manually":
        if len(sys.argv) < 4:
            print("Missing parameters!")
            print("Usage: python -m src.etl.kpi_month --create-version-manually --source-month <month> [--force]")
            print("Example: python -m src.etl.kpi_month --create-version-manually --source-month 1")
            print("Example: python -m src.etl.kpi_month --create-version-manually --source-month 1 --force")
            sys.exit(1)
        
        source_month = None
        force = False
        
        i = 2
        while i < len(sys.argv):
            if sys.argv[i] == "--source-month" and i + 1 < len(sys.argv):
                source_month = int(sys.argv[i + 1])
                i += 2
            elif sys.argv[i] == "--force":
                force = True
                i += 1
            else:
                i += 1
        
        if source_month is None:
            print("Missing parameter --source-month!")
            print("Usage: python -m src.etl.kpi_month --create-version-manually --source-month <month> [--force]")
            sys.exit(1)
        
        if source_month < 1 or source_month > 12:
            print(f"Source month must be between 1 and 12, received: {source_month}")
            sys.exit(1)
        
        calculator.create_version_manually(
            source_month=source_month,
            force=force
        )

    elif len(sys.argv) > 1 and sys.argv[1] == "--recalculate-version":
        if len(sys.argv) < 6:
            print("Missing parameters!")
            print("Usage: python -m src.etl.kpi_month --recalculate-version <version> --month <month> --new-kpi-initial <value>")
            print("Example: python -m src.etl.kpi_month --recalculate-version 'Thang 2' --month 2 --new-kpi-initial 15")
            sys.exit(1)
        
        version = sys.argv[2]
        month = int(sys.argv[4])
        new_kpi_initial = float(sys.argv[6])
        
        print(f"Recalculating version after marketing adjustment:")
        print(f"  - Version: {version}")
        print(f"  - Month: {month}")
        print(f"  - New kpi_initial: {new_kpi_initial}\n")
        
        calculator.recalculate_version_after_marketing_adjustment(
            version=version,
            adjusted_month=month,
            new_kpi_initial=new_kpi_initial
        )
    else:
        # Check if target-month is specified
        target_month = None
        i = 1
        while i < len(sys.argv):
            if sys.argv[i] == "--target-month" and i + 1 < len(sys.argv):
                target_month = int(sys.argv[i + 1])
                if target_month < 1 or target_month > 12:
                    print(f"Target month must be between 1 and 12, received: {target_month}")
                    sys.exit(1)
                break
            i += 1
        
        if target_month:
            print(f"Calculating KPI adjustment for version 'Thang {target_month}'...")
        else:
            print("Calculating KPI adjustment...")
        
        result = calculator.save_kpi_adjustment(target_month=target_month)
        
        print(f"Successfully saved {len(result)} records to kpi_month")
