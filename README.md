# Sales Data Analysis

## Overview
Sales Performance Analysis is a data-driven project that processes sales input data and customer-related information to reward top-performing employees and customers. The project utilizes Google Cloud Storage (GCS) for data storage, MySQL for customer and store information, and BigQuery for data analysis.

## Project Structure
The project follows a modular structure for better organization and scalability:

```
sales_data_analysis
│   README.md
|   requirements.txt
|   main.py
│
├── configs
│   ├── .env
│   ├── dev_config.cfg
│   ├── prod_config.cfg
│   ├── gcp_creds.json
│ 
├── services
│   ├── bucket_service.py
│   ├── db_service.py
│   ├── email_service.py
│   ├── logger_service.py
│   ├── runtime_env_config_service.py
│
├── utility
│   ├── schemas
│   │   ├── dataframes_schemas.py
│   │   │
│   |
│   |
├── ├── transformations
│   │   ├── spark_operations.py
│   │   │   
│   |
│
├── extras
│   ├── big_query
│   │   ├── big_query_tables.sql
│   │   │   
│   |
│   ├── data_generate_scripts
│   │   ├── extra_column_csv_generated_data.py
│   │   ├── generate_customer_table_data.py
│   │   ├── generate_datewise_sales_data.py
│   │   ├── less_column_csv_generated_data.py
|   |   |
|   |
│   ├── sql_tables
│   │   ├── sql_tables_creation.py
│   │   ├── table_scripts.sql
│   │   │   
│   |
|
```
- **main.py**: Entry point for the project.
  
- **configs**: Configuration files for different environments and sensitive information.
  - `.env`: Environment variables.
  - `dev_config.cfg`: Development configuration.
  - `prod_config.cfg`: Production configuration.
  - `gcp_creds.json`: Google Cloud Platform credentials.

- **services**: Reusable services for various functionalities.
  - `bucket_service.py`: Service for interacting with GCS buckets.
  - `db_service.py`: Service for MySQL database operations.
  - `email_service.py`: Service for email-related functionality to send alerts and status of PySpark Job.
  - `logger_service.py`: Logging service.
  - `runtime_env_config_service.py`: Service for managing runtime environment configurations.

- **utility**: Utility scripts and modules.
  - `schemas`: Dataframe schemas for structured data.
  - `transformations`: Data transformation operations.
    - `spark_operations.py`: Spark-based operations for data transformation.

- **extras**: Additional scripts and resources.
  - **big_query**: SQL scripts for creating BigQuery tables.
  - **data_generate_scripts**: Scripts for generating sample data.
  - **sql_tables**: Scripts for creating SQL tables.

## Bucket Structure
```
sales-data-621/
├── ├── archived/
|   |   ├── sales/
│   │   |   ├── input_file_after_processing.csv
│   
├── ├── customer_partitioned_data/
│   │   ├── sales_date_month=2023-08/
|   |   |   ├── processed_file1.parquet
|   |   |   ├── processed_file2.parquet
|   |   |   
│   │   ├── sales_date_month=2023-09/
|   |   |   ├── processed_file1.parquet
|   |   |   ├── processed_file2.parquet
|   |   |   
│   │   ├── sales_date_month=2023-10/
|   |   |   ├── processed_file1.parquet
|   |   |   ├── processed_file2.parquet
|   |   |   
|
├── ├── sales_defective_data/
│   │   ├── validation_and_schema_failed_files.csv
|
├── ├── sales_input_data/
│   │   ├── sales_input_files.csv
|
├── ├── sales_partitioned_data/
│   │   ├── sales_date_month=2023-08/
|   |   |   ├── store_id=121/
|   |   |   |   ├── processed_file1.parquet
|   |   |   |   ├── processed_file2.parquet
|   |   |   |
|   |   |   ├── store_id=122/
|   |   |   |   ├── processed_file1.parquet
|   |   |   |   ├── processed_file2.parquet
|   |   |   |
|   |   |   ├── store_id=122/
|   |   |   |   ├── processed_file1.parquet
|   |   |   |   ├── processed_file2.parquet
|   |   |   |
```

## How to Use
1. Set up the required environment variables in `.env` file.
2. Configure the necessary credentials in `gcp_creds.json`.
3. Modify configuration files (`dev_config.cfg` and `prod_config.cfg`) based on the environment.
4. Run `main.py` to execute the data processing and analysis pipeline.

## Dependencies
- Python 3.x
- Spark (for transformations)
- Google Cloud SDK
- MySQL

## External Tools
- BigQuery: External tables are created for efficient analytical queries.

## Additional Notes
- Ensure proper access controls and security measures for GCS, MySQL, and BigQuery.
- Monitor and manage resources to avoid unnecessary costs.
- [Jar File](https://mvnrepository.com/artifact/com.mysql/mysql-connector-j) for connecting PySpark with MySQL

## Future Improvements
- Consider implementing automated testing.
- Enhance error handling and logging mechanisms.
- Explore further optimizations for large-scale data processing.

Feel free to reach out for any questions or feedback!
