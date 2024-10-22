from datetime import datetime
import pandas as pd
import yaml
import json
import os
from config.config import CATEGORY_CONFIG_FILE, CONFIG_DIR, INPUT_DIR
from utils.file_util import hash_row, load_known_vendors, load_category_list


class Transformer:
    def __init__(self, source, transform_rules_file_name, schema_mapping_file_name):
        self.transform_rules = self.load_transformation_rules(os.path.join(CONFIG_DIR,transform_rules_file_name))
        self.schema_mapping = self.load_schema_mapping(os.path.join(CONFIG_DIR,schema_mapping_file_name))
        self.source = source
        self.known_categroies = load_category_list()


    # Load source schema mapping from JSON file
    def load_schema_mapping(self, schema_mapping_file):
        with open(schema_mapping_file, 'r') as file:
            mapping = json.load(file)
        if self.source in mapping:
            return mapping[self.source]
        else:
            raise ValueError(f"No schema mapping found for source: {self.source}")
        

    # Load transformation rules from the YAML file
    def load_transformation_rules(self, rules_file):
        with open(rules_file, 'r') as file:
            return yaml.safe_load(file)
        
    # Load the target schema columns from the transformation rules
    def load_target_schema_columns(self):
        target_columns = set()

        # Extract target column mappings
        for rule in self.transformation_rules['transformations']:
            target_field = rule['column']
            target_columns.add(target_field)

        return list(target_columns)
        

    # Apply schema mapping to normalize source data to the target schema
    def normalize_schema(self, df, source_schema_mapping):   
        # Remove any columns that are not in the target schema
        df.drop(columns=[col for col in df.columns if col not in source_schema_mapping], inplace=True)
        df = df.rename(columns=source_schema_mapping)

        return df


    # Apply transformation based on rules
    def apply_transformation(self, df, rules):
        for rule in rules['transformations']:
            column = rule['column']
            operation = rule['operation']
            params = rule['params']

            if column == 'source':
                df['source'] = self.source

            if column in df.columns:
                if operation == 'truncate':
                    df[column] = df[column].apply(lambda x: self.truncate_string(x, params['max_length']))
                elif operation == 'validate_unique':
                    self.validate_unique(df, column, params['required'])
                elif operation == 'validate_value':
                    df[column] = df[column].apply(lambda x: self.validate_value(x, params['allowed_values'], params['required']))
                elif operation == 'format_currency':
                    df[column] = df[column].apply(lambda x: self.format_currency(x, params['format'], 0, params['required']))
                elif operation == 'format_date':
                    df[column] = df[column].apply(lambda x: self.format_date(x, params['format'], params['required']))
                elif operation == 'assign_category':
                    df[column] = df.apply(lambda x: self.assign_category(x['vendor_long'], x['category']), axis=1) 
            elif params['required']:
                if operation == 'assign_category':
                    df[column] = df.apply(lambda x: self.assign_category(x['vendor_long']), axis=1)
                elif operation == 'validate_value':
                    # TODO - handle this better
                    df[column] = df.apply(lambda x: self.validate_value(x[column], params['allowed_values'], params['required']), axis=1)
                elif operation == 'hash_row':
                    df[column] = df.apply(lambda x: hash_row(x, df.columns), axis=1)
                else:
                    df[column] = df.apply(lambda x: pd.NA)

        return df
    

    # Truncate a string to a maximum length
    def truncate_string(self, value, max_length):
        if pd.isna(value):
            return None
        return str(value)[:max_length]

    # Validate that a column contains unique values
    def validate_unique(self, df, column, required):
        if required and df[column].isna().any():
            raise ValueError(f"{column} contains null values but is required.")
        if not df[column].is_unique:
            raise ValueError(f"{column} contains duplicate values.")

    # Validate that a value is in the allowed values
    def validate_value(self, value, allowed_values, required):
        if pd.isna(value):
            if required:
                raise ValueError(f"Missing required value for column.")
            return value
        if value not in allowed_values:
            raise ValueError(f"Invalid value: {value}. Allowed values are: {allowed_values}")
        return value

    # Format a currency value (e.g., ensure it's positive and formatted to 2 decimal places)
    def format_currency(self, value, format_str, min_value, required):
        if pd.isna(value):
            if required:
                raise ValueError("Missing required currency value.")
            return None
        if value < min_value:
            raise ValueError(f"Currency value {value} is below the minimum allowed: {min_value}.")
        return f"{value:.2f}"

    # Format a date value to a specific format (e.g., YYYY-MM-DD)
    def format_date(self, value, format_str, required):
        if pd.isna(value):
            if required:
                raise ValueError("Missing required date value.")
            return None
        
        # TODO: possible to automate this list
        possible_formats = [
            "%Y-%m-%d",   # e.g., 2023-10-22
            "%m/%d/%Y",   # e.g., 10/22/2023
            "%m/%d/%y",   # e.g., 10/22/23
            "%d-%m-%Y",   # e.g., 22-10-2023
            "%Y.%m.%d",   # e.g., 2023.10.22
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

        # logger.info(f"Unknown vendor: {vendor}, category: {category}")

        return pd.NA

    # Main function to load data and apply transformations
    def transform_data(self, input_file_name):
        df = pd.read_csv(os.path.join(INPUT_DIR, input_file_name))
        df_normalized = self.normalize_schema(df, self.schema_mapping)
        rules = self.load_transformation_rules()
        df_transformed = self.apply_transformation(df_normalized, rules)

        return df_transformed


class SourceToStandardTransformer(Transformer):
    def __init__(self, source):
        super().__init__(source,'standard_transformation_rules.yaml','source_schema_mapping.json')

class StandardToFinalTransformer(Transformer):
    def __init__(self, destination):
        super().__init__(destination,'target_transformation_rules.yaml','destination_schema_mapping.json')



# Example usage
if __name__ == "__main__":
    # t = SourceToStandardTransformer('capital_one_checking', INPUT_DIR+'/checking_sample.csv')
    t = StandardToFinalTransformer('tmoap')

    transformed_data = t.transform_data('transactions_final.csv')
    print(transformed_data.head(25))


