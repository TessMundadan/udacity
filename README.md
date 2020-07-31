#Context


The project is to find relationship between US immigration, the weather in the city and the demography of the city. 
Source files are read from s3 bucket.Use spark on Amazon EMR to extract, tranform and load the source files to Parquet. The parquet files are written to s3.

#Database Schema Design


Used a star schema.The dimension tables have details of each dimension. Fact table contain the ids of the dimension table and the metrics which are being analyzed<br>

Dimension Tables<br>
* immigration_table<br>
* temperature_table<br>
* demographics_table<br>
* time_table<br>
* airport_table<br>

Fact table<br>
* us_city_immigration


#ETL Pipeline Design

process_temperature_data()<br>
Reads temperature files from the S3 and cleanse , transform and write as temperature parquet file to s3.

process_airport_data()<br>
Reads airport files from the S3 and cleanse , transform and write as airport parquet file to s3.

process_immigration_data()<br>
Reads airport files from the S3 and cleanse , transform and write as immigration and time parquet file to s3.

process_demographics_data()<br>
Reads demographics file from the S3 and cleanse , transform and write as demographics parquet file to s3.

load_us_city_immigration_fact_table()<br>
Read dimension tables from parquet files and load to us_city_immigration_fact_table and write the table as parquet file to s3.

#Run etl.py
Create a EMR cluster.
ssh to master node of the cluster
scp dl.py and etl.py to master node.
submit spark job.

