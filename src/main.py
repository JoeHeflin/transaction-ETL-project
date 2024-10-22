import pandas as pd
import os
from transformation.transform_source_a import CheckingTransformer, CreditCardATransformer
import manual_processing.manual_processor as manual_processor
from data_processing_project.src.config.config_example import ARCHIVE_DIR, INPUT_DIR, FINAL_FILE, PENDING_FILE
from utils.logger import logger
from utils.file_util import find_duplicate_rows


def process_source_data(transformer):
    # Use transformation instance to transform data
    df = transformer.transform_data()
    print(df.head())
    
    # Identify rows with missing required fields
    missing_data_df = df[df.isnull().any(axis=1)]
    completed_df = df.dropna()

    print(f"Found {len(missing_data_df)} transactions with missing data.")
    print(f"Completed processing {len(completed_df)} transactions.")

    # Append missing data to pending file
    logger.info(f"Saving {len(missing_data_df)} transactions to pending file.")
    save_transactions_to_file(missing_data_df, PENDING_FILE)
    logger.info(f"Saving {len(completed_df)} transactions to final file.")
    save_transactions_to_file(completed_df, FINAL_FILE)

    # Return completed transactions (fully processed)
    return completed_df

def save_transactions_to_file(df, file_name):
    if not df.empty:
        if os.path.exists(file_name):
            df_existing = pd.read_csv(file_name)
            df = pd.concat([df_existing, df], ignore_index=True)
        df.to_csv(file_name, index=False)

def find_duplicate_rows_in_file(file_path, column):
    df = pd.read_csv(file_path)
    return find_duplicate_rows(df, column)

def archive_input_file(input_file_name):
    input_file = os.path.join(INPUT_DIR, input_file_name)
    if not os.path.exists(ARCHIVE_DIR):
        os.makedirs(ARCHIVE_DIR)
    os.rename(input_file, os.path.join(ARCHIVE_DIR, input_file_name))

if __name__ == "__main__":
    # Names of input files in INPUT_PATH
    input_file_names = ['credit_card_sample.csv', 'checking_sample.csv']
    transformers = [CreditCardATransformer, CheckingTransformer]

    for input_file_name, transformer in zip(input_file_names, transformers):
        print(input_file_name, transformer)

        # Process input data and identify incomplete transactions
        if os.path.exists(os.path.join(INPUT_DIR, input_file_name)):
            logger.info(f"Processing {input_file_name}...")
            t = transformer(input_file_name)
            completed_transactions = process_source_data(t)
            duplicates = find_duplicate_rows(completed_transactions, 'id')
            if not duplicates.empty:
                logger.warning(f"Found duplicate transactions in {input_file_name}.")
                logger.warning(duplicates)

            archive_input_file(input_file_name)
        else:
            logger.info(f"No input file found: {input_file_name}")

    if os.path.exists(PENDING_FILE):
        # Guide user to process pending transactions
        choice = str(input("Would you like to process pending transactions now? (Y/N)")).lower()
        while choice not in ['y', 'n']:
            choice = str(input("Invalid choice. Please enter 'Y' or 'N'.")).lower()

        if choice == 'y':
            manual_processor.process_pending_transactions()
        elif choice == 'n':
            print("Exiting program.")
    else:
        logger.info("No pending transactions to process.")

    d = find_duplicate_rows_in_file(FINAL_FILE, 'id')
    if not d.empty:
        logger.warning(f"Found {len(d)} duplicate transactions in final file.")
        logger.warning(d)
    else:
        logger.info("No duplicate transactions found in final file.")

    print("Program completed.")