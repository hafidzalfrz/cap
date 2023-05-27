# End to End mini project (Celerates Acceleration Program)

## Overview
Create data pipeline from data source to Datawarehouse.
![image](https://github.com/hafidzalfrz/cap/assets/37131558/623dd333-04f0-4cfd-82ae-c7ffe4d73573)

Data source:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

In this project i choose Green Taxi Trip Records. You can use the script download_script.py to download all the parquet files in 2022 Green Taxi Trip Records.
After that, run the Airflow scheduler to trigger (once) the pipeline. Don't forget to place your DAG file in Airflow dags directory.

ps: Since this pipeline runs on top of hadoop environment, you must run the hadoop, hive, and airflow first to run the scheduler. Check the requirements too in requirements.txt
