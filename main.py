import os
from argparse import ArgumentParser
from dotenv import load_dotenv
import sys
project_path = os.getcwd()
sys.path.append(project_path)


def main_execution():
    try:
        bucket_obj = BucketOps()

        files_to_process, error = bucket_obj.list_files()
        if error:
            raise Exception(error)

        if not files_to_process:
            message = "No files to process for sales project. Terminating the process."
            send_mail(message)
            logger.info(message)
            sys.exit(0)


        spark = SparkOps()

        valid_files, invalid_files = [], []

        # Schema Validation
        for file in files_to_process:
            is_valid_schema = spark.input_file_schema_validation_check(file)
            if is_valid_schema:
                valid_files.append(file)
            else:
                invalid_files.append(file)


        # Moving the invalid files to faulty_files folder in bucket
        if invalid_files:
            for invalid_file in invalid_files:
                error = bucket_obj.move_file(file_to_move=invalid_file,folder_path=faulty_files_folder)
                if error:
                    # send failure email where we failed to remove the faulty files and continue the process
                    send_mail(error)


        if not valid_files:
            error = "All input files failed in schema validation. Terminating the process."
            logger.error(error)
            raise Exception(error)
        
        # Reading different data related to customers, stores, products and sales_teams from SQL tables
        customer_df = spark.dataframe_from_db(table_name=customers_tbl)
        customer_df.show(1)

        stores_df = spark.dataframe_from_db(table_name=stores_tbl)
        stores_df.show(1)

        sales_team_df = spark.dataframe_from_db(table_name=sales_team_tbl)
        sales_team_df.show(1)
            
        for valid_file in valid_files:

            sales_df, error = spark.input_data_df([valid_file])
            if error:
                raise Exception(error)

            # Creating enriched dataframe by joining all the dimension dataframes with input sales data frame
            enriched_df, error = spark.enriched_data_df(sales_df, customer_df, stores_df, sales_team_df)

            if error:
                raise Exception(error)

            enriched_df.show(1)
            # customer_processed data write
            error = spark.customer_processed_data_write(enriched_df)

            if error:
                raise Exception(error)


            # sales_team_processed data write
            error = spark.sales_team_processed_data_write(enriched_df)

            if error:
                raise Exception(error)

            # Moving the file to archived folder after processing for keep track of processed files and use as checkpoint
            error = bucket_obj.move_file(file_to_move=valid_file,folder_path=archived_sales_folder)
            if error:
                send_mail(error)


    except Exception as error:
        send_mail(error)
        sys.exit(-1)

    finally:
        try:
            spark.close_session()
        except:
            logger.info("Failed to close the spark session")
        


if __name__=="__main__":

    load_dotenv("./configs/.env")
    arg_parser = ArgumentParser()
    arg_parser.add_argument("--env",help="Environment Configurations to use",default="PRODUCTION")
    args = arg_parser.parse_args()
    environment = args.env
    os.environ["ENVIRONMENT"] = environment

    from services.logger_service import logger
    from services.runtime_env_config_service import read_configs
    from services.email_service import send_mail
    from services.bucket_service import BucketOps
    from utility.transformations.spark_operations import SparkOps

    # Configurations
    configs = read_configs()
    customers_tbl = configs["SQLDB"]["sql.table.customer"]
    stores_tbl = configs["SQLDB"]["sql.table.store"]
    products_tbl = configs["SQLDB"]["sql.table.product"]
    sales_team_tbl = configs["SQLDB"]["sql.table.sales_team"]

    faulty_files_folder = configs["STORAGE"]["cloud.faulty_files"]
    archived_sales_folder = configs["STORAGE"]["cloud.archived.sales"]

    main_execution()
    