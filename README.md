# Personal Finance Data Processing Project

This project is designed to process and transform personal finance transaction data from various sources. It includes functionality for handling pending transactions, transforming data, and logging.

## Table of Contents

- [Setup](#setup)
- [Usage](#usage)
- [Configuration](#configuration)
- [Logging](#logging)
- [Contributing](#contributing)
- [License](#license)

## Setup

### Prerequisites

- Python 3.6 or higher
- `pip` (Python package installer)

### Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/yourusername/personal-finance-data-processing.git
    cd personal-finance-data-processing
    ```

2. Create a virtual environment:

    ```sh
    python -m venv venv
    ```

3. Activate the virtual environment:

    - On Windows:

        ```sh
        venv\Scripts\activate
        ```

    - On macOS and Linux:

        ```sh
        source venv/bin/activate
        ```

4. Install the required packages:

    ```sh
    pip install -r requirements.txt
    ```

## Usage

### Running the Data Processing Script

1. Ensure that your input data files are placed in the [`INPUT_PATH`](command:_github.copilot.openSymbolFromReferences?%5B%22%22%2C%5B%7B%22uri%22%3A%7B%22scheme%22%3A%22file%22%2C%22authority%22%3A%22%22%2C%22path%22%3A%22%2FUsers%2Fjosephheflin%2FDesktop%2Fdev%2Fpersonal-finance%2Fdata_processing_project%2Fsrc%2Fmain.py%22%2C%22query%22%3A%22%22%2C%22fragment%22%3A%22%22%7D%2C%22pos%22%3A%7B%22line%22%3A4%2C%22character%22%3A66%7D%7D%5D%2C%22b5f61563-4d0b-479b-85cf-be7df9c9852a%22%5D "Go to definition") directory as specified in the configuration.

2. Run the main script:

    ```sh
    python main.py
    ```

### Example

To process data using a specific transformer, you can modify the [`main.py`](command:_github.copilot.openRelativePath?%5B%7B%22scheme%22%3A%22file%22%2C%22authority%22%3A%22%22%2C%22path%22%3A%22%2FUsers%2Fjosephheflin%2FDesktop%2Fdev%2Fpersonal-finance%2Fdata_processing_project%2Fsrc%2Fmain.py%22%2C%22query%22%3A%22%22%2C%22fragment%22%3A%22%22%7D%2C%22b5f61563-4d0b-479b-85cf-be7df9c9852a%22%5D "/Users/josephheflin/Desktop/dev/personal-finance/data_processing_project/src/main.py") script to use the desired transformer class. For example:

```python
from transformation.transform_source_a import CheckingTransformer

transformer = CheckingTransformer()
completed_transactions = process_data(transformer)