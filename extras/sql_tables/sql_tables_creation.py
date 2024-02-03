import sys
sys.path.append("/home/slim5/myPractice/DE_Project/youtube_de_project1-master/de-project/")
from services.db_service import DbOps
from services.logger_service import logger
import traceback

## Tables creation commands ##
sales_input_files_table = """CREATE TABLE IF NOT EXISTS sales_input_files (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            file_name VARCHAR(255),
                            file_location VARCHAR(255),
                            created_date TIMESTAMP ,
                            updated_date TIMESTAMP ,
                            status ENUM("Processed","Processing")
                        );
                        """

customer_table = """CREATE TABLE IF NOT EXISTS customer (
                    customer_id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    address VARCHAR(255),
                    pincode VARCHAR(10),
                    phone_number VARCHAR(20),
                    customer_joining_date DATE
                );
                """

store_table = """CREATE TABLE IF NOT EXISTS store (
                    id INT PRIMARY KEY,
                    address VARCHAR(255),
                    store_pincode VARCHAR(10),
                    store_manager_name VARCHAR(100),
                    store_opening_date DATE,
                    reviews TEXT
                );
                """

product_table = """CREATE TABLE IF NOT EXISTS product (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255),
                    current_price DECIMAL(10, 2),
                    old_price DECIMAL(10, 2),
                    created_date TIMESTAMP ,
                    updated_date TIMESTAMP ,
                    expiry_date DATE
                );
                """

sales_team_table = """CREATE TABLE IF NOT EXISTS sales_team (
                id INT AUTO_INCREMENT PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                manager_id INT,
                is_manager CHAR(1),
                address VARCHAR(255),
                pincode VARCHAR(10),
                joining_date DATE
            );
            """

customer_purchase_mart_table = """CREATE TABLE IF NOT EXISTS customers_purchase_info (
                        customer_id INT ,
                        full_name VARCHAR(100),
                        address VARCHAR(200),
                        phone_number VARCHAR(20),
                        sales_date_month DATE,
                        total_sales DECIMAL(10, 2)
                    );
                    """

sales_performance_mart_table ="""CREATE TABLE IF NOT EXISTS sales_team_sale_performance (
                    store_id INT,
                    sales_person_id INT,
                    full_name VARCHAR(255),
                    sales_month VARCHAR(10),
                    total_sales DECIMAL(10, 2),
                    incentive DECIMAL(10, 2)
                );
                """


## Insert commands for tables ##

insert_command_customer_table ="""INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES 
                                ("Saanvi", "Krishna", "Delhi", "122009", "9173121081", "2021-01-20"), 
                                ("Dhanush", "Sahni", "Delhi", "122009", "9155328165", "2022-03-27"), 
                                ("Yasmin", "Shan", "Delhi", "122009", "9191478300", "2023-04-08")   , 
                                ("Vidur", "Mammen", "Delhi", "122009", "9119017511", "2020-10-12"),
                                ("Shamik", "Doctor", "Delhi", "122009", "9105180499", "2022-10-30"), 
                                ("Ryan", "Dugar", "Delhi", "122009", "9142616565", "2020-08-10"),
                                ("Romil", "Shanker", "Delhi", "122009", "9129451313", "2021-10-29"),
                                ("Krish", "Tandon", "Delhi", "122009", "9145683399", "2020-01-08"),
                                ("Divij", "Garde", "Delhi", "122009", "9141984713", "2020-11-10"),
                                ("Hunar", "Tank", "Delhi", "122009", "9169808085", "2023-01-27"),
                                ("Zara", "Dhaliwal", "Delhi", "122009", "9129776379", "2023-06-13"),
                                ("Sumer", "Mangal", "Delhi", "122009", "9138607933", "2020-05-01"),
                                ("Rhea", "Chander", "Delhi", "122009", "9103434731", "2023-08-09"),
                                ("Yuvaan", "Bawa", "Delhi", "122009", "9162077019", "2023-02-18"),
                                ("Sahil", "Sabharwal", "Delhi", "122009", "9174928780", "2021-03-16"),
                                ("Tiya", "Kashyap", "Delhi", "122009", "9105126094", "2023-03-23"),
                                ("Kimaya", "Lala", "Delhi", "122009", "9115616831", "2021-03-14"),
                                ("Vardaniya", "Jani", "Delhi", "122009", "9125068977", "2022-07-19"),
                                ("Indranil", "Dutta", "Delhi", "122009", "9120667755", "2023-07-18"),
                                ("Kavya", "Sachar", "Delhi", "122009", "9157628717", "2022-05-04"),
                                ("Manjari", "Sule", "Delhi", "122009", "9112525501", "2023-02-12"),
                                ("Akarsh", "Kalla", "Delhi", "122009", "9113226332", "2021-03-05"),
                                ("Miraya", "Soman", "Delhi", "122009", "9111455455", "2023-07-06"),
                                ("Shalv", "Chaudhary", "Delhi", "122009", "9158099495", "2021-03-14"),
                                ("Jhanvi", "Bava", "Delhi", "122009", "9110074097", "2022-07-14");
                            """

insert_command_store_table = """INSERT INTO store (id, address, store_pincode, store_manager_name, store_opening_date, reviews) VALUES
                                (121,"Delhi", "122009", "Manish", "2022-01-15", "Great store with a friendly staff."),
                                (122,"Delhi", "110011", "Nikita", "2021-08-10", "Excellent selection of products."),
                                (123,"Delhi", "201301", "vikash", "2023-01-20", "Clean and organized store."),
                                (124,"Delhi", "400001", "Rakesh", "2020-05-05", "Good prices and helpful staff.");
                            """

insert_command_product_table = """INSERT INTO product (name, current_price, old_price, created_date, updated_date, expiry_date) VALUES
                                    ("quaker oats", 212, 212, "2022-05-15", NULL, "2025-01-01"),
                                    ("sugar", 50, 50, "2021-08-10", NULL, "2025-01-01"),
                                    ("maida", 20, 20, "2023-03-20", NULL, "2025-01-01"),
                                    ("besan", 52, 52, "2020-05-05", NULL, "2025-01-01"),
                                    ("refined oil", 110, 110, "2022-01-15", NULL, "2025-01-01"),
                                    ("clinic plus", 1.5, 1.5, "2021-09-25", NULL, "2025-01-01"),
                                    ("dantkanti", 100, 100, "2023-07-10", NULL, "2025-01-01"),
                                    ("nutrella", 40, 40, "2020-11-30", NULL, "2025-01-01");
                                """

insert_command_sales_team_table = """INSERT INTO sales_team (first_name, last_name, manager_id, is_manager, address, pincode, joining_date) VALUES
                                    ("Rahul", "Verma", 10, "N", "Delhi", "122007", "2020-05-01"),
                                    ("Priya", "Singh", 10, "N", "Delhi", "122007", "2020-05-01"),
                                    ("Amit", "Sharma", 10, "N", "Delhi", "122007", "2020-05-01"),
                                    ("Sneha", "Gupta", 10, "N", "Delhi", "122007", "2020-05-01"),
                                    ("Neha", "Kumar", 10, "N", "Delhi", "122007", "2020-05-01"),
                                    ("Vijay", "Yadav", 10, "N", "Delhi", "122007", "2020-05-01"),
                                    ("Anita", "Malhotra", 10, "N", "Delhi", "122007", "2020-05-01"),
                                    ("Alok", "Rajput", 10, "N", "Delhi", "122007", "2020-05-01"),
                                    ("Monica", "Jain", 10, "N", "Delhi", "122007", "2020-05-01"),
                                    ("Rajesh", "Gupta", 10, "Y", "Delhi", "122007", "2020-05-01");
                                """

create_table_commands = [sales_input_files_table, customer_table, store_table, product_table, sales_team_table, customer_purchase_mart_table, sales_performance_mart_table]

insert_data_commands = [insert_command_customer_table, insert_command_store_table, insert_command_product_table, insert_command_sales_team_table]

s1 = "select * from sales_team limit 5"
select_commands = [s1]

mysql_obj = DbOps()

# for command in create_table_commands:
#     try:
#         output = mysql_obj.execute_command(command)
#     except Exception as e:
#         logger.info(f"Fail to create the table for given command: {command}")
#         logger.info(f"Error: {e}\nTrackeback: {traceback.format_exc()}")

# for insert_command in insert_data_commands:
#     try:
#         output = mysql_obj.execute_command(insert_command)
#     except Exception as e:
#         logger.info(f"Fail to insert the data in table for given command: {insert_command}")
        # logger.info(f"Error: {e}\nTrackeback: {traceback.format_exc()}")

for select_command in select_commands:
    try:
        output = mysql_obj.execute_command(select_command)
        print(output)
    except Exception as e:
        logger.info(f"Fail to insert the data in table for given command: {select_command}")
        logger.info(f"Error: {e}\nTrackeback: {traceback.format_exc()}")

mysql_obj.close_session()