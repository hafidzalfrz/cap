from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.hive_operator import HiveOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    default_args=default_args,
    dag_id = 'green_taxi',
    description='DAG of green_taxi',
    schedule_interval='@daily',
)as dag:
    create_table_hive = HiveOperator(
        task_id = 'create_table',
        hql = '''
        CREATE EXTERNAL TABLE IF NOT EXISTS green_taxi (
            VendorID BIGINT,
            lpep_pickup_datetime BIGINT,
            lpep_dropoff_datetime BIGINT,
            store_and_fwd_flag STRING,
            RatecodeID DOUBLE,
            PULocationID BIGINT,
            DOLocationID BIGINT,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            ehail_fee DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            payment_type DOUBLE,
            trip_type DOUBLE,
            congestion_surcharge DOUBLE
        )
        STORED AS PARQUET LOCATION 'hdfs://localhost:9000/user/hafidzalfrz/staging_taxi' tblproperties ("skip.header.line.count"="1")
        ''',
        hive_cli_conn_id='hiveserver2_default',  # Set your Hive connection ID here
    )
    load_data_hive = HiveOperator(
        task_id = 'load_data',
        hql = '''
        LOAD DATA LOCAL INPATH '/home/hafidzalfrz/cap/download/*.parquet' OVERWRITE INTO TABLE green_taxi
        ''',
        hive_cli_conn_id= 'hiveserver2_default',  # Set your Hive connection ID here
    )
    create_dwh = HiveOperator(
        task_id = 'create_dwh',
        hql = '''
        create database if not exists dwh
        ''',
        hive_cli_conn_id= 'hiveserver2_default',

    )
    transform = BashOperator(
        task_id = 'transform', 
        bash_command = '/home/hafidzalfrz/spark/bin/spark-submit --master local /home/hafidzalfrz/cap/spark_transform.py',
    )
    
create_table_hive >> load_data_hive >> create_dwh >> transform



# from pyspark.sql import SparkSession


# Create a SparkSession
# spark = SparkSession.builder \
#     .appName("HiveExample") \
#     .config("spark.sql.catalogImplementation", "hive") \
#     .config("hive.metastore.uris", "thrift://localhost:9083") \
#     .enableHiveSupport() \
#     .getOrCreate()

    # create_table_dwh_vendor = HiveOperator(
    #     task_id = 'create_dwh_vendor',
    #     hql = '''
    #     create external table if not exists dwh.dim_vendor(
    #     VendorID int,
    #     vendor_name string
    #     )
    #     STORED AS PARQUET LOCATION 'hdfs://localhost:9000/user/hafidzalfrz/dwh_taxi' tblproperties ("skip.header.line.count"="1")
    #     ''',
    #     hive_cli_conn_id= 'hiveserver2_default',
    # )

# create_table_hive >> load_data_hive >> create_dwh >> create_table_dwh_vendor >> dim_vendor

# spark.sql(create_table_query)
# spark.sql(load_data_query)

# # Execute HiveQL query
# spark.sql("select * from green_taxi limit 5").show()