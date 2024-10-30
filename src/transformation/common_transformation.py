from datetime import datetime
import pandas as pd
import os
from ..config.config import CONFIG_DIR
from ..utils.file_util import hash_row, load_known_vendors, load_category_list, extract_csv_data, load_from_json, load_from_yaml
from ..utils.logger import logger


class Transformer:
    def __init__(self, source, transform_rules, schema_mapping):
        self.source = source
        self.transform_rules = transform_rules
        self.source_schema_mapping = schema_mapping.get(source, None)

        if not self.source_schema_mapping:
            raise ValueError(f"No schema mapping found for source: {self.source}")
        print(f"Loaded transformation rules for source: {source}")

    # Apply schema mapping to normalize source data to the target schema
    def normalize_schema(self, df):
        # Remove any columns that are not in the target schema
        # source_schema_mapping = self.source_schema_mapping
        df.drop(
            columns=[col for col in df.columns if col not in self.source_schema_mapping],
            inplace=True,
        )
        # Rename columns based on the source schema mapping
        df = df.rename(columns=self.source_schema_mapping)

        return df

    # Apply transformation based on rules
    def apply_transformation(self, df):
        for rule in self.transform_rules["transformations"]:
            column = rule["column"]
            operation = rule["operation"]
            params = rule["params"]

            if column in df.columns:
                if operation == "truncate":
                    df[column] = df[column].apply(
                        lambda x: self._safe_apply(self.truncate_string,x, params["max_length"])
                    )
                elif operation == "validate_unique":
                    self.validate_unique(df, column)
                elif operation == "validate_value":
                    df[column] = df[column].apply(
                        lambda x: self._safe_apply(self.validate_value,
                            x, params["allowed_values"]
                        )
                    )
                elif operation == "format_currency":
                    df[column] = df[column].apply(
                        lambda x: self._safe_apply(self.format_currency,
                            x, params["format"], 0
                        )
                    )
                elif operation == "format_date":
                    df[column] = df[column].apply(
                        lambda x: self._safe_apply(self.format_date,
                            x, params["format"]
                        )
                    )
                elif operation == "assign_category":
                    df[column] = df.apply(
                        lambda x: self._safe_apply(self.assign_category,x["vendor_long"], x["category"]),
                        axis=1,
                    )
            elif params["required"]:
                # Required column is missiing
                try:
                    if operation == "assign_category":
                        df[column] = df.apply(
                            lambda x: self.assign_category(x["vendor_long"]), axis=1
                        )
                    elif operation == "validate_value":
                        df[column] = df.apply(
                            lambda x: self.validate_value(
                                None, params["allowed_values"]
                            ),
                            axis=1,
                        )
                    elif operation == "hash_row":
                        df[column] = df.apply(lambda x: hash_row(x, df.columns), axis=1)
                    elif operation == "add_source":
                        df[column] = self.truncate_string(self.source, params["max_length"])
                    else:
                        df[column] = df.apply(lambda x: pd.NA)

                except ValueError as e:
                    logger.error(f"Error processing column {column}: {e}")
                    df[column] = df.apply(lambda x: pd.NA)

        return df
    
    def _safe_apply(self, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ValueError as e:
            logger.error(f"{e}")
            return pd.NA

    # Truncate a string to a maximum length
    def truncate_string(self, value, max_length):
        if pd.isna(value):
            return value
        return str(value)[:max_length]

    # Validate that a column contains unique values
    def validate_unique(self, df, column):
        if not df[column].is_unique:
            raise ValueError(f"{column} contains duplicate values.")
        
    # Validate that a value is in the allowed values
    def validate_value(self, value, allowed_values):
        if pd.isna(value):
            raise ValueError("Missing required value for column.")
        if value in allowed_values:
            return value
        else:
            raise ValueError(
                f"Invalid value: {value} at row {None}. Allowed values are: {allowed_values}" #Todo: add row number
            )
        
    # Format a currency value (e.g., ensure it's positive and formatted to 2 decimal places)
    def format_currency(self, value, format_str, min_value):
        if pd.isna(value):
            raise ValueError("Missing required currency value.")
        elif value < min_value:
            raise ValueError(
                f"Currency value {value} is below the minimum allowed: {min_value}."
            )
        else:
            try:
                return f"{value:.2f}" #Todo: use format_str
            except ValueError as e:
                raise e

    # Format a date value to a specific format (e.g., YYYY-MM-DD)
    def format_date(self, value, format_str):
        if pd.isna(value):
            raise ValueError("Missing required date value.")

        possible_formats = [
            "%Y-%m-%d",  # e.g., 2023-10-22
            "%m/%d/%Y",  # e.g., 10/22/2023
            "%m/%d/%y",  # e.g., 10/22/23
            "%d-%m-%Y",  # e.g., 22-10-2023
            "%Y.%m.%d",  # e.g., 2023.10.22
            # Add more formats as needed based on the expected input
        ]

        date = None

        for fmt in possible_formats:
            try:
                date = datetime.strptime(value, fmt)
            except ValueError:
                pass

        if date:
            return date.strftime(format_str)
        else:
            raise ValueError(f"Invalid date format for {value}.")

    # Assign a category if vendor is known
    def assign_category(self, vendor, category=None):
        known_vendors = load_known_vendors()

        if vendor in known_vendors:
            return known_vendors[vendor]
        
        if category and category in load_category_list():
            return category

        raise ValueError(f"Unknown vendor: {vendor}, category: {category}")

    # Main function to load data and apply transformations
    def transform_data(self, input_data):
        df_normalized = self.normalize_schema(input_data)
        df_transformed = self.apply_transformation(df_normalized)

        return df_transformed


class SourceToStandardTransformer(Transformer):
    def __init__(self, source):
        # rules = load_from_yaml(os.path.join(CONFIG_DIR,"standard_transformation_rules.yaml"))
        # schema_mapping = load_from_json(os.path.join(CONFIG_DIR,"source_schema_mapping.json"))
        super().__init__(
            source, 
            load_from_yaml(os.path.join(CONFIG_DIR,"standard_transformation_rules.yaml")),
            load_from_json(os.path.join(CONFIG_DIR,"source_schema_mapping.json"))
        )


class StandardToFinalTransformer(Transformer):
    def __init__(self, destination):
        # rules = load_from_yaml(os.path.join(CONFIG_DIR,"target_transformation_rules.yaml"))
        # schema_mapping = load_from_json(os.path.join(CONFIG_DIR,"source_schema_mapping.json"))
        super().__init__(
            destination,
            load_from_yaml(os.path.join(CONFIG_DIR,"target_transformation_rules.yaml")),
            load_from_json(os.path.join(CONFIG_DIR,"source_schema_mapping.json"))
        )


# Example usage
if __name__ == "__main__":
    schema_map = load_from_json(os.path.join(CONFIG_DIR,"source_schema_mapping.json"))
    rules = load_from_yaml(os.path.join(CONFIG_DIR,"standard_transformation_rules.yaml"))

    t = Transformer("source_a", rules, schema_map)

    input_df = extract_csv_data("transactions.csv")
    transformed_data = t.transform_data(input_df)

    print(transformed_data.head(25))