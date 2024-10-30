# import pytest
# from src.transformation.transform_sources import CheckingTransformer
# from src.main import process_source_data, archive_input_file
# from src.utils.file_util import find_duplicate_rows
# from src.utils.file_util import find_duplicate_rows
from data_processing_project.src.transformation.transform_sources import CheckingTransformer, CreditCardATransformer
from data_processing_project.src.utils.file_util import load_known_vendors, extract_csv_data, find_duplicate_rows
from data_processing_project.src.config.config import INPUT_DIR
import os


# import sys
# import os
# sys.path.insert(0, "/Users/josephheflin/Desktop/dev/personal-finance/data_processing_project/src")

def process_source_data(transformer, input_data):
    # Use transformation instance to transform data
    df = transformer.transform_data(input_data)
    print(df.head())

    # Identify rows with missing required fields
    missing_data_df = df[df.isnull().any(axis=1)]
    completed_df = df.dropna()

    print(f"Found {len(missing_data_df)} transactions with missing data.")
    print(f"Completed processing {len(completed_df)} transactions.")

    # # Append missing data to pending file
    # logger.info(f"Saving {len(missing_data_df)} transactions to pending file.")
    # save_transactions_to_file(missing_data_df, PENDING_FILE)
    # logger.info(f"Saving {len(completed_df)} transactions to final file.")
    # save_transactions_to_file(completed_df, FINAL_FILE)

    # Return completed transactions (fully processed)
    return completed_df
def test_load_known_vendors():
    known_vendors = load_known_vendors()
    assert known_vendors["Test Vendor"] == "Category A"

def test_checking_transformer():
    transformer = CheckingTransformer("checking_sample.csv")
    assert transformer.source == "Checking"
    # Add more assertions to test the functionality


def test_main():
    input_file_names = ["credit_card_sample_test.csv"]
    transformer_classes = [CreditCardATransformer]

    for input_file_name, transformer_class in zip(
        input_file_names, transformer_classes
    ):
        print(input_file_name, transformer_class)

        # Process input data and identify incomplete transactions
        input_file = os.path.join(INPUT_DIR, input_file_name)
        if input_file:
            t = transformer_class()
            input_data = extract_csv_data(input_file)
            completed_transactions = process_source_data(t, input_data)
            duplicates = find_duplicate_rows(completed_transactions, "id")
            assert len(duplicates) == 0
            assert len(completed_transactions) == 71

        else:
            assert(False)

if __name__ == "__main__":
    test_main()
    # test_checking_transformer()