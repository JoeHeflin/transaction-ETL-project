import pandas as pd
import os
import signal
import json
from utils.logger import logger
from data_processing_project.src.config.config_example import PENDING_DIR, FINAL_DIR, CATEGORY_CONFIG_FILE, PENDING_FILE, FINAL_FILE
from utils.file_util import load_known_vendors, load_category_list


TIMEOUT = 60  # seconds

categories = load_category_list()

# Signal handler for timeout
def timeout_handler(signum, frame):
    raise TimeoutError("User input timed out.")

# Request user input for missing fields
def request_user_input(row):
    for column in row.index:
        if pd.isna(row[column]):
            logger.info(f"Missing value for {column}: {row['vendor_long']}")
            print(f"Missing value for {column}: {row['vendor_long']}")
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(TIMEOUT)
            if column == 'category':
                for idx, category in enumerate(categories, start=1):
                    print(f"{idx}. {category}")
                choice = int(input("Enter the number corresponding to the category: "))                    
                while not (1 <= choice <= len(categories)):
                    try:
                        choice = int(input("Invalid selection. Please choose a number from the list."))
                    except ValueError:
                        choice = -1 # Invalid input
                        print("Invalid selection. Please choose a number from the list.")
                value = str(categories[choice - 1])
                update_pending_transactions(value, category)
                save_known_vendors(value, category)
            else:

                try:
                    value = input(f"Please provide a value for {column} (required): ")
                except ValueError:
                    value = None
                    print("Invalid input. Please provide a valid value.")
            signal.alarm(0)  # Disable the alarm
            row[column] = value
    return row


def save_known_vendors(description, category):
    known_vendors = load_known_vendors()
    known_vendors[description] = category
    category_file = json.load(open(CATEGORY_CONFIG_FILE))
    category_file['known_vendors'] = known_vendors
    with open(CATEGORY_CONFIG_FILE, 'w') as file:
        json.dump(category_file, file)

def update_pending_transactions(description, category):
    df_pending = pd.read_csv(PENDING_FILE)
    df_pending.loc[df_pending['vendor_long'] == description, 'category'] = category
    df_pending.to_csv(PENDING_FILE, index=False)

# Process pending transactions
def process_pending_transactions():
    if not os.path.exists(PENDING_FILE):
        logger.info("No pending transactions to process.")
        return
    
    df_pending = pd.read_csv(PENDING_FILE)
    logger.info(f"Loaded {len(df_pending)} pending transactions.")
    completed_transactions = []
    
    for index, row in df_pending.iterrows():
        logger.debug(f"Processing transaction {index + 1}/{len(df_pending)}: {row.to_dict()}")
        try:
            # Get user input for missing fields
            updated_row = request_user_input(row) #TODO
            # Update data frame with user input
            df_pending.loc[index] = updated_row
        except TimeoutError as e:
            logger.info(f"\nExiting data review due to timeout ({TIMEOUT} seconds)")
            break
        except KeyboardInterrupt:
            logger.info("\nExiting data review due to keyboard interrupt")
            break
            
        if updated_row.isnull().sum() == 0:
            # Move to final if no missing fields
            completed_transactions.append(updated_row)
    
    # Save remaining pending transactions back to pending file
    df_pending = df_pending[df_pending.isnull().any(axis=1)]
    df_pending.to_csv(PENDING_FILE, index=False)
    
    # Save completed transactions to final file
    # TODO: generate new final file each time
    if completed_transactions:
        df_final = pd.DataFrame(completed_transactions)        
        if os.path.exists(FINAL_FILE):
            df_final_existing = pd.read_csv(FINAL_FILE)
            df_final = pd.concat([df_final_existing, df_final], ignore_index=True)
        df_final.to_csv(FINAL_FILE, index=False)

    if len(df_pending) > 0:
        logger.warning(f"Exiting data review. {len(df_pending)} transactions still need to be processed.")

    logger.info(f"Successfully processed {len(completed_transactions)} transactions.")

if __name__ == "__main__":
    print("Starting manual transaction processing...")
    process_pending_transactions()