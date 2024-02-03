"""
This file is created to add commands to perform below operations after transformation for analysis purpose:
* External Table creation for analysis on sales teams to find out top performing employee in terms of sales amount from each store 
* External Table creation for tracking and analysing the customers purchasing behaviour
"""

create external table `spark-project-409004.data_mart.customer_purchase_data` 
(
    customer_id INTEGER ,
    customer_name STRING,
    customer_address STRING,
    customer_pincode STRING,
    customer_phone_number STRING,
    sales_date_day INT64,
    total_purchase FLOAT64    
)
with partition columns(sales_date_month STRING)
OPTIONS(
    FORMAT = 'PARQUET',
    uris = ['gs://sales-data-621/customer_partitioned_data/sales_date_month=*'],
    hive_partition_uri_prefix = 'gs://sales-data-621/customer_partitioned_data/'

);


create or replace external table `spark-project-409004.data_mart.sales_team_sale_performance` 
(
    sales_person_id INT64,
    sales_person_name STRING,
    store_manager_name STRING,
    manager_id INT64,
    is_manager STRING,
    sales_person_address STRING,
    sales_person_pincode STRING,
    sales_date_day INT64,
    total_sales FLOAT64
)
with partition columns(sales_date_month STRING,    store_id STRING)
OPTIONS(
    FORMAT = 'PARQUET',
    uris = ['gs://sales-data-621/sales_partitioned_data/sales_date_month=*'],
    hive_partition_uri_prefix = 'gs://sales-data-621/sales_partitioned_data/'

);

-- Q. Write a query to find out the top 3 customers who had purchased most per month to reward them with a coupon of 5% discount on next month's 1st purchase
with max_purchase_amount_per_month as (
    select sales_date_month as purchase_month, customer_id,  sum(total_purchase) as monthly_spend,
        dense_rank() over(partition by sales_date_month order by sum(total_purchase) desc) as top_purchaser_ranking
    from `data_mart.customer_purchase_data` group by sales_date_month,customer_id
 )
select 
    purchase_month, customer_id, monthly_spend,top_purchaser_ranking 
from max_purchase_amount_per_month 
where top_purchaser_ranking<=3 
order by top_purchaser_ranking;


-- Q. Find out the best employee per store along with total sales amount
with employee_sales_per_store as (
    select 
        store_id, sales_date_month as sales_month, sales_person_id, sum(total_sales) as total_sales_amount,
        dense_rank() over(partition by store_id, sales_date_month order by sum(total_sales) desc) as ranking 
    from `spark-project-409004.data_mart.sales_team_sale_performance` 
    group by store_id, sales_date_month , sales_person_id
)
select store_id, sales_month, sales_person_id, total_sales_amount from employee_sales_per_store where ranking=1;


-- Q. For top 3 performing employess per store in terms of sales amount, reward bonus of 5,3 and 1.5 percent of total sales done by them for 1,2 and 3 ranking and 0 % for other employess.
with employee_sales_per_store as (
    select 
        store_id, sales_date_month as sales_month, sales_person_id,sum(total_sales) as total_sales_amount, 
        dense_rank() over(partition by store_id, sales_date_month order by sum(total_sales) desc) as ranking 
    from `spark-project-409004.data_mart.sales_team_sale_performance` 
    group by store_id, sales_date_month , sales_person_id
)
select 
    store_id, sales_month, sales_person_id,
    case 
        when ranking=1 then round((total_sales_amount)*(0.05),2)
        when ranking=2 then round((total_sales_amount)*(0.03),2) 
        when ranking=3 then round((total_sales_amount)*(0.015),2) 
        else 0 
    end as bonus 
from employee_sales_per_store;

