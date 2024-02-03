from pyspark.sql.types import *

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
("customer_id","customer_name","customer_address","customer_pincode","customer_phone_number",
                                                        "sales_date_day","total_purchase","sales_date_month")

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


