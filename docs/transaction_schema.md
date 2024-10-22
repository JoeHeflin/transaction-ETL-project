## Target Schema Overview

The **target schema** is the unified format into which all input sources are transformed. This ensures consistent data output, enabling seamless integration and analysis. The target schema is defined in [target_schema.yaml](config/target_schema.yaml) and outlines the fields, their data types, and validation rules.

### Target Schema Fields

| Field        | Type    | Description                             | Validation               |
|--------------|---------|-----------------------------------------|--------------------------|
| id           | Integer | Unique identifier for the transaction   | Non-null, unique         |
| source       | String  | Name of the source account              | Non-null, max length: 255|
| type         | String  | Type of transaction                     | "Debit" or "Credit"      |
| amount       | Float   | Amount transacted (USD)                 | Non-null, positive value, format: 0.00|
| vendor_long  | String  | Full description of the vendor          | Non-null, max length: 255|
| vendor_short | String  | Readable vendor description             | Non-null, max length: 255|
| date         | Date    | Date of the transaction                 | Non-null, format: YYYY-MM-DD|
| category     | String  | Category of the transaction             | Non-null, max length: 255|
| balance      | Float   | Source account balance (USD)            | Optional, format: 0.00   |
| notes        | String  | Additional notes                        | Optional, max length: 1024|