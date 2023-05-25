from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

## Transformasi timestamp
df = spark.sql("""select * from default.green_taxi""")
df = df.withColumn("lpep_dropoff_datetime", from_unixtime(col("lpep_dropoff_datetime") / 1000000).cast("timestamp"))
df = df.withColumn("lpep_pickup_datetime", from_unixtime(col("lpep_pickup_datetime") / 1000000).cast("timestamp"))

## Drop null column
df = df.drop("ehail_fee")

## Create SKVendor
df = df.withColumn('VendorName', when(df.VendorID == 1, 'Creative Mobile Technologies')\
                    .when(df.VendorID == 2, 'VeriFone Inc')\
                    .otherwise('Unknown'))
df = df.withColumn('SKVendor', when(df.VendorID == 1, 1)
                    .when(df.VendorID == 2, 2)
                    .otherwise(3))

## Create SKFlag
df = df.withColumn('FlagDesc', when(df.store_and_fwd_flag == "Y", 'store and forward trip')\
                    .when(df.store_and_fwd_flag == "N", 'not a store and forward trip')\
                    .otherwise('Unknown'))
df = df.withColumn('SKFlag', when(df.store_and_fwd_flag == "Y", 1)\
                    .when(df.store_and_fwd_flag == "N", 2)\
                    .otherwise(3))

## Create SKRatecode
df = df.withColumn('RatecodeType', when(df.RatecodeID == 1, 'Standard rate')\
                    .when(df.RatecodeID == 2, 'JFK')\
                    .when(df.RatecodeID == 3, 'Newark')\
                    .when(df.RatecodeID == 4, 'Nassau or Westchester')\
                    .when(df.RatecodeID == 5, 'Negotiated fare')\
                    .when(df.RatecodeID == 6, 'Group Ride')\
                    .otherwise('Unknown'))
df = df.withColumn('SKRatecode', when(df.RatecodeID == 1, 1)\
                    .when(df.RatecodeID == 2, 2)\
                    .when(df.RatecodeID == 3, 3)\
                    .when(df.RatecodeID == 4, 4)\
                    .when(df.RatecodeID == 5, 5)\
                    .when(df.RatecodeID == 6, 6)\
                    .otherwise(7))

## Create SKPayment
df = df.withColumn('PaymentType', when(df.payment_type == 1, 'Credit Card')\
                    .when(df.payment_type == 2, 'Cash')\
                    .when(df.payment_type == 3, 'No Charge')\
                    .when(df.payment_type == 4, 'Dispute')\
                    .otherwise('Unknown'))
df = df.withColumn('SKPaymentType', when(df.payment_type == 1, 1)\
                    .when(df.payment_type == 2, 2)\
                    .when(df.payment_type == 3, 3)\
                    .when(df.payment_type == 4, 4)\
                    .otherwise(5))

## Create SKTripType
df = df.withColumn('TripType', when(df.trip_type == 1, 'Street-hail')\
                    .when(df.trip_type == 2, 'Dispatch')\
                    .otherwise('Unknown'))
df = df.withColumn('SKTripType', when(df.trip_type == 1, 1)\
                    .when(df.trip_type == 2, 2)\
                    .otherwise(3))

fact = df.selectExpr('lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'SKVendor', 'SKFlag', 'SKRatecode', 'SKPaymentType', 'SKTripType').orderBy(['lpep_pickup_datetime', 'lpep_dropoff_datetime'])
dim_trip_type = df.selectExpr('SKTripType', 'trip_type', 'TripType').distinct().orderBy('SKTripType')
dim_payment_type = df.selectExpr('SKPaymentType', 'payment_type', 'PaymentType').distinct().orderBy('SKPaymentType').dropna()
dim_ratecode = df.selectExpr('SKRatecode', 'RatecodeID', 'RatecodeType').distinct().orderBy('SKRatecode').dropna()
dim_flag = df.selectExpr('SKFlag', 'store_and_fwd_flag', 'FlagDesc').distinct().orderBy('SKFlag')
dim_vendor = df.selectExpr('SKVendor', 'VendorID', 'VendorName').distinct().orderBy('SKVendor')

dim_trip_type.write.mode("overwrite").saveAsTable("dwh.dim_trip_type")
dim_payment_type.write.mode("overwrite").saveAsTable("dwh.dim_payment_type")
dim_ratecode.write.mode("overwrite").saveAsTable("dwh.dim_ratecode")
dim_flag.write.mode("overwrite").saveAsTable("dwh.dim_flag")
dim_vendor.write.mode("overwrite").saveAsTable("dwh.dim_vendor")
fact.write.mode("overwrite").saveAsTable("dwh.fact")