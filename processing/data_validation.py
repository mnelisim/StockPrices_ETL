import pandas as pd
from utils.logger import log_message

class DataValidator:

    def validate_stock_data(self, df, run_id=None):

        try:
            log_message("Starting data validation", level="INFO", run_id=run_id)

            # Missing values check
            null_count = df.isnull().sum().sum()

            if null_count > 0:
                log_message(
                    f"Found {null_count} missing values",
                    level="WARNING",
                    run_id=run_id
                )

            # Duplicate rows check
            duplicates = df.duplicated().sum()

            if duplicates > 0:
                log_message(
                    f"Found {duplicates} duplicate rows",
                    level="WARNING",
                    run_id=run_id
                )

            # Business rule validation (Stock prices should be > 0)
            price_columns = [col for col in df.columns if "Close" in col]

            for col in price_columns:
                if (df[col] <= 0).any():
                    raise ValueError(f"Invalid price values found in {col}")

            log_message(
                "Data validation passed successfully",
                level="INFO",
                run_id=run_id
            )

            return True

        except Exception as error:
            log_message(
                f"Validation failed: {error}",
                level="ERROR",
                run_id=run_id
            )
            raise