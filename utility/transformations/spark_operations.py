from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
import traceback
from services.runtime_env_config_service import read_configs
from services.logger_service import logger
# from utility.schemas.dataframes_schemas import *


configs = read_configs()
HOST = configs["SQLDB"]["sql.host"]
PORT = configs["SQLDB"]["sql.port"]
USER = configs["SQLDB"]["sql.user"]
PASSWORD = configs["SQLDB"]["sql.password"]
DATABASE = configs["SQLDB"]["sql.database"]
SERVER = configs["SQLDB"]["sql.server"]
SQL_PROPERTIES = { "driver": configs["SQLDB"]["sql.spark.driver"] }
SQL_URL = f"jdbc:{SERVER}://{HOST}:{PORT}/{DATABASE}?user={USER}&password={PASSWORD}"

CLOUD_SERVICE = configs["STORAGE"]["cloud.service"]
JAR_FILES = f'{CLOUD_SERVICE}://{configs["JARS"]["sql.jar"]}'
BUCKET = configs["STORAGE"]["cloud.bucket_name"]
CLOUD_PATH = F"{CLOUD_SERVICE}://{BUCKET}/"
CUSTOMER_PROCESSED_LOC = CLOUD_PATH+configs["STORAGE"]["cloud.processed.customer_partitioned"]
SALES_TEAM_PROCESSED_LOC = CLOUD_PATH+configs["STORAGE"]["cloud.processed.sales_partitioned"]

SALES_FILE_MANDATORY_COLUMNS = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]
SALES_FILE_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_columns", MapType(StringType(),StringType()),True)
])

CUSTOMER_PROCESSED_DF_SCHEMA = {
    "customer_id": IntegerType(),
    "customer_name": StringType(),
    "customer_address": StringType(),
    "customer_pincode": StringType(),
    "customer_phone_number": StringType(),
    "sales_date_day": IntegerType(),
    "total_purchase": FloatType(),
    "sales_date_month": StringType()
}

SALES_TEAM_PROCESSED_DF_SCHEMA = {
    "store_id":IntegerType(),
    "sales_person_id": IntegerType(),
    "sales_person_name": StringType(),
    "store_manager_name": StringType(),
    "manager_id":IntegerType(),
    "is_manager": StringType(),
    "sales_person_address": StringType(),
    "sales_person_pincode":StringType(),
    "sales_date_day": IntegerType(),
    "total_sales": FloatType(),
    "sales_date_month": StringType(),
}

class SparkOps:
    def __init__(self):
        try:
            self.spark = SparkSession.builder\
                            .appName("sales_data_processing")\
                            .config("spark.jars",JAR_FILES)\
                            .getOrCreate()
            logger.info("Successfully created the spark session")
        except:
            error = f"Failed to create the spark session.\nError: {traceback.format_exc()}"
            logger.error(error)
            raise Exception(error)

    def get_client(self):
        return self.spark
    

    def dataframe_from_db(self, table_name):
        try:
            df = self.spark.read.option("inferSchema","true").option("ignoreLeadingWhiteSpace", "true").option("ignoreTrailingWhiteSpace", "true").jdbc(url=SQL_URL, properties=SQL_PROPERTIES, table=table_name)
            logger.info(f"Successfully created spark dataframe and read data from table: {table_name}")
            return df
        except:
            error = f"Failed to create the spark dataframe using table {table_name}.\nError: {traceback.format_exc()}"
            logger.error(error)
            raise Exception(error)
    

    def input_file_schema_validation_check(self, file:str, format:str="csv", header:bool=True):
        file = CLOUD_PATH+file
        df_columns = self.spark.read.format(format).option("header",header).option("ignoreLeadingWhiteSpace","true").option("ignoreTrailingWhiteSpace","true").load(file).columns
        logger.info(f"Columns in input file: {file} is:\n{df_columns}")
        missing_columns = set(SALES_FILE_MANDATORY_COLUMNS) - set(df_columns)
        if missing_columns:
            logger.error(f"""Schema validation failed for file: {file}\nMandatory Columns are: {SALES_FILE_MANDATORY_COLUMNS}\nMissing Columns are: {list(missing_columns)}""")
            return False
        logger.info(f"Schema Validation is successful for file: {file}")
        return True
    

    def input_data_df(self, files:list, format:str="csv", header:bool=True):
        error, final_df = "", []
        try:
            final_df = self.spark.createDataFrame(data=[],schema=SALES_FILE_SCHEMA)
            for file in files:
                file = CLOUD_PATH+file
                logger.info(f"Reading data from file: {file}")
                input_df = self.spark.read.format(format).option("header",header).option("ignoreLeadingWhiteSpace","true").option("ignoreTrailingWhiteSpace","true").load(file)                

                additional_columns = [column for column in input_df.columns if column not in SALES_FILE_MANDATORY_COLUMNS]
                if additional_columns:
                    logger.info(f"Found additional columns for file: {file}\nAdditional Columns: [{additional_columns}]")
                    json_columns = to_json(struct(additional_columns))
                    input_df = input_df.withColumn("additional_columns",json_columns).drop(*additional_columns)
                else:
                    input_df = input_df.withColumn("additional_columns",lit(None))

                final_df = final_df.union(input_df)
                logger.info(f"Successfully appended data in final dataframe for file: {file}") 
            logger.info("Successfully created final dataframe to process")           
        
        except:
            error = f"Failed to create the final dataframe using files: {files}.\nError: {traceback.format_exc()}"
            logger.error(error)

        return final_df, error

    
    def enriched_data_df(self, sales_df: DataFrame, customer_df: DataFrame, stores_df: DataFrame,  sales_team_df: DataFrame):
        error, final_enriched_df = "", []
        try:
            logger.info("Joining customer_df,stores_df and sales_team_df dimension tables dataframes with sales_df to create enriched final dataframe")

            sales_customer_df = sales_df.alias("sales").join(customer_df.alias("cust"), on="customer_id",how="inner")
            sales_customer_df = sales_customer_df.withColumnRenamed("pincode","customer_pincode").withColumnRenamed("phone_number","customer_phone_number")\
                                                 .withColumn("customer_name", concat_ws(" ",col("first_name"),col("last_name")))\
                                                 .withColumn("customer_address",col("cust.address"))
            
            sale_cust_drop_columns = ["product_name","price","quantity", "address", "additional_columns", "first_name","last_name","customer_joining_date"]
            sales_customer_df = sales_customer_df.drop(*sale_cust_drop_columns)
            
            sales_customer_df.show(1)
            logger.info("Successfully joined sales and customer dataframes")

            sales_customer_store_df = sales_customer_df.alias("sc").join(stores_df.alias("stores"), col("sc.store_id")==col("stores.id"),"inner")
            scs_drop_columns = ["id","store_opening_date","reviews","address"]
            sales_customer_store_df = sales_customer_store_df.withColumn("store_address",col("stores.address"))\
                                                             .withColumnRenamed("pincode","store_pincode")\
                                                             .drop(*scs_drop_columns)
            
            sales_customer_store_df.show(1)
            logger.info("Successfully joined sales_customer and stores dataframes")
            
            final_enriched_df = sales_customer_store_df.alias("scs").join(sales_team_df.alias("st"), col("scs.sales_person_id")==col("st.id"),"inner")
            final_df_drop_columns = ["id","first_name","last_name","address","pincode"]
            final_enriched_df = final_enriched_df.withColumn("sales_person_name", concat_ws(" ",col("st.first_name"),col("st.last_name")))\
                                                .withColumn("sales_person_address",col("st.address")).withColumn("sales_person_pincode",col("st.pincode"))\
                                                .drop(*final_df_drop_columns)
            
            logger.info("Successfully joined sales_customer_store and sales_team dataframes")
            logger.info("Final Enriched Dataframe is ready for further processing")

        except:
            error = f"Failed to create the enriched final dataframe by joining dimension tables dataframes.\nError: {traceback.format_exc()}"
            logger.error(error)

        return final_enriched_df,error
    

    def customer_processed_data_write(self,enriched_df:DataFrame):
        error = ""
        try:            
            customer_processed_df = enriched_df.withColumn("sales_date_month", date_format(col("sales_date"),"yyyy-MM"))\
                                                .withColumn("sales_date_day", date_format(col("sales_date"),"dd"))\
                                                .withColumnRenamed("total_cost","total_purchase")\
                                                .select("customer_id","customer_name","customer_address","customer_pincode","customer_phone_number",
                                                        "sales_date_day","total_purchase","sales_date_month")
            
            # Correcting the schema
            for column, datatype in CUSTOMER_PROCESSED_DF_SCHEMA.items():
                customer_processed_df = customer_processed_df.withColumn(column,col(column).cast(datatype))

            customer_processed_df.write.format("parquet").mode("append").option("header","true").partitionBy("sales_date_month").save(CUSTOMER_PROCESSED_LOC)
            logger.info(f"Successfully written the customer processed data in bucket at location {CUSTOMER_PROCESSED_LOC} in partitioned format")
        except Exception as e:
            error = f"Failed to write the customers processed data in bucket at {CUSTOMER_PROCESSED_LOC}.\nError: {traceback.format_exc()}"
            logger.error(error)
        return error
    
    
    def sales_team_processed_data_write(self,enriched_df: DataFrame):
        error = ""
        try:
            sales_team_processed_df = enriched_df.withColumn("sales_date_month", date_format(col("sales_date"),"yyyy-MM"))\
                                                .withColumn("sales_date_day", date_format(col("sales_date"),"dd"))\
                                                .withColumnRenamed("total_cost","total_sales")\
                                                .select("store_id","sales_person_id","sales_person_name","store_manager_name","manager_id","is_manager",
                                                        "sales_person_address","sales_person_pincode","sales_date_day","total_sales","sales_date_month" )
            # Correcting the schema
            for column, datatype in SALES_TEAM_PROCESSED_DF_SCHEMA.items():
                sales_team_processed_df = sales_team_processed_df.withColumn(column,col(column).cast(datatype))
            sales_team_processed_df.write.format("parquet").mode("append").option("header","true").partitionBy("sales_date_month","store_id").save(SALES_TEAM_PROCESSED_LOC)
            logger.info(f"Successfully written the sales team processed data in bucket at location {SALES_TEAM_PROCESSED_LOC} in partitioned format")
        except:
            error = f"Failed to write the sales_team processed data in bucket at {SALES_TEAM_PROCESSED_LOC}.\nError: {traceback.format_exc()}"
            logger.error(error)
        return error


    def close_session(self):
        self.spark.stop()
        logger.info("Successfully closed the spark session")

