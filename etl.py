import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import datetime
from pyspark.sql.types import TimestampType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import split
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
from pyspark.sql.functions import datediff, to_date
from pyspark.sql.functions import year, month
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import boto3
from botocore.exceptions import NoCredentialsError

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
#    """
#
#   The purpose of this function is to create spark session
#
#    Args:
#    None
#
#    Returns:
#    spark: spark session.
#
#    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.1.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport()\
        .getOrCreate()
    return spark


def process_temperature_data(spark, input_data, output_data):
#    """
#
#   The purpose of this function is to read temperature csv files from S3 bucket create temperture table and write back to S3.
#
#    Args:
#    spark: spark session.
#    input_data: S3 bucket for input data.
#    output_data: S3 bucket for output data.
#
#    Returns:
#    None
#
#    """
    # get filepath to temperature data file
    temperature_data_read_path = os.path.join(input_data, 'GlobalLandTemperaturesByCity.csv')

    # read temperature data file
    try:
        temperature_df =  spark.read.csv(temperature_data_read_path,header='true')
    except:
        print('Error  reading source')
   
    
    # extract columns to create temperature table
    temperature_table = temperature_df.drop('Latitude','Longitude')\
                        .filter("Country == 'United States' AND dt > '2011-12-01' AND dt < '2013-01-01'")\
                        .withColumn('month',month(to_date(col('dt'))))\
                        .select(col('dt'),col('AverageTemperature').alias('average_temperature')\
                        ,col('AverageTemperatureUncertainty').alias('average_temperature_uncertainty')\
                        ,col('City').alias('city'),col('Country').alias('country'),col('month'))
                 
   # write temperatures table to parquet files partitioned by City
    temperature_table.write.partitionBy("city").parquet(os.path.join(output_data,'temperature_table'),'overwrite')


def process_airport_data(spark, input_data, output_data):
#    """
#
#   The purpose of this function is to read airport csv file from S3 bucket create airport parquet file
#   and write back to S3.
#
#    Args:
#    spark: spark session.
#    input_data: S3 bucket for input data.
#    output_data: S3 bucket for output data.
#
#    Returns:
#    None
#
#    """
    # get filepath to airport data file
    airport_data_read_path = os.path.join(input_data, 'airport-codes_csv.csv')
   
    # read airport data file
    try:
        airport_df = spark.read.csv(airport_data_read_path,header='true')
    except:
        print('Error reading source')
    
    # drop unwanted columns, remove Nulls and duplicates, seperate country and state from iso region(eg:US-CA)
    # split the municipality (eg:Raleigh/Durham)
    airport_table = airport_df.drop('coordinates','local_code','gps_code','elevation_ft')\
                    .dropna(how = "any", subset = ["municipality","iata_code"])\
                    .filter("iso_country == 'US'")\
                    .withColumn("state",split(col("iso_region"),"-").getItem(1))\
                    .withColumn("new_municipality",split(col("municipality"),"/").getItem(0)).distinct()

   
    # write users table to parquet files
    airport_table.write.parquet(os.path.join(output_data,'airport_table'),'overwrite')

def process_city_data(spark, input_data, output_data):
#    """
#
#   The purpose of this function is to read demographics csv file from S3 bucket create city parquet file
#   and write back to S3.
#
#    Args:
#    spark: spark session.
#    input_data: S3 bucket for input data.
#    output_data: S3 bucket for output data.
#
#    Returns:
#    None
#
#    """
 
    # get filepath to city data file
    city_data_read_path = os.path.join(input_data, 'us-cities-demographics.csv')

    # read city data file
    try:
        city_df = spark.read.option("sep",";").csv(city_data_read_path,header='true')
    except:
        print('Error reading source')
    
    # Rename Columns, Remove Nulls and Duplicates
    demographics_table=city_df.select(col('City').alias('city'),col('State').alias('state'),col('Total Population').alias('total_population')\
               ,col('Foreign-born').alias('foreign_born'),col('State Code').alias('state_code'))\
               .dropna().distinct()

    # write users table to parquet files
    demographics_table.write.parquet(os.path.join(output_data,'demographics_table'),'overwrite')  

    
def process_immigration_data(spark, input_data, output_data, bucket, prefix):
#    """
#
#   The purpose of this function is to read immigration SAS files from S3 bucket create city parquet file
#   and write back to S3.
#
#    Args:
#    spark: spark session.
#    input_data: S3 bucket for input data.
#    output_data: S3 bucket for output data.
#    bucket: S3 bucket name
#    prefix :S3 prefix
#    Returns:
#    None
#
#    """
    
    
    ACCESS_KEY =  os.environ['AWS_ACCESS_KEY_ID']
    SECRET_KEY =  os.environ['AWS_SECRET_ACCESS_KEY']
    
    #function to convert sas date format
    get_date = udf(lambda x:datetime.datetime(1960,1,1) + datetime.timedelta(days=int(x)),TimestampType())
    
    #create s3 client
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)

    #Get all the SAS files for immigration and create a single spark dataframe
    #result = s3.list_objects_v2(Bucket='immigration-data')
    result = s3.list_objects_v2(Bucket=bucket,Prefix=prefix)
    
    for indx,item in enumerate(result['Contents']):
        file = item['Key']
        s3_file = input_data+file
        if indx == 0:
            try:
                df_spark  = spark.read.format('com.github.saurfang.sas.spark').load(s3_file)
            except:
                print('Error reading source')
            df_spark = df_spark.withColumn('arrival_date', get_date(df_spark.arrdate))\
                       .dropna(how = "any", subset = ["i94mode","i94addr","i94bir"])\
                       .select('cicid','i94yr','i94mon','i94cit','i94res','i94port','arrdate',\
                               'i94mode','i94addr','i94bir','i94visa','biryear','visatype','arrival_date')
            df_immigration = df_spark
        else:
            try:
                df_spark =spark.read.format('com.github.saurfang.sas.spark').load(s3_file)
            except:
                print('Error reading source')
            df_spark = df_spark.withColumn('arrival_date', get_date(df_spark.arrdate))\
                       .dropna(how = "any", subset = ["i94mode","i94addr","i94bir"])\
                       .select('cicid','i94yr','i94mon','i94cit','i94res','i94port','arrdate',\
                               'i94mode','i94addr','i94bir','i94visa','biryear','visatype','arrival_date')

            df_immigration=df_immigration.union(df_spark)

    # write time table to parquet files partitioned by i94yr, i94mon and i94port
    df_immigration.write.partitionBy("i94yr","i94mon","i94port").parquet(os.path.join(output_data,'immigration_table'),'overwrite')
   
    # extract columns to create time table
    time_table = df_immigration.select(col('arrival_date').alias('date'),\
              hour(col('arrival_date')).alias('hour'),\
              dayofmonth(col('arrival_date')).alias('day'),\
              weekofyear(col('arrival_date')).alias('week'),\
              month(col('arrival_date')).alias('month'),\
              year(col('arrival_date')).alias('year'),\
              date_format(col('arrival_date'),"EEEE").alias('weekday'),\
             ).dropDuplicates()
 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(os.path.join(output_data,'time_table'),'overwrite')

    
def load_us_city_immigration_fact_table(spark, output_data):
#    """
#
#   The purpose of this function is to create us_city_immigration parquet file
#   and write back to S3.
#
#    Args:
#    spark: spark session.
#    output_data: S3 bucket for outputput data.
#
#    Returns:
#    None
#
#    """
    # Read dimension table data
    immigration_table=spark.read.parquet(os.path.join(output_data,'immigration_table'))
   
    temperature_table=spark.read.parquet(os.path.join(output_data,"temperature_table"))
    
    airport_table=spark.read.parquet(os.path.join(output_data,"airport_table"))
    
    demographics_table=spark.read.parquet(os.path.join(output_data,"demographics_table"))
    
    time_table=spark.read.parquet(os.path.join(output_data,"time_table"))
    
    #Check if the dimension table are empty
    if immigration_table.count() == 0:
        print("Temperature data is empty")
        raise ValueError
        
    if temperature_table.count() == 0:
        print("Airport data is empty")
        raise ValueError
        
    if airport_table.count() == 0:
        print("Demography data is empty")
        raise ValueError

    if demographics_table.count() == 0:
        print("Time data is empty")
        raise ValueError

    if time_table.count() == 0:
        print("Immigration data is empty")
        raise ValueError

    # Create thefact table from the dimension table
    us_city_immigration_table=immigration_table.join(airport_table,[immigration_table.i94port == airport_table.iata_code],how='inner')\
.select(immigration_table.arrival_date,immigration_table.cicid,immigration_table.i94mon,airport_table.new_municipality,airport_table.state,airport_table.ident)\
            .join(temperature_table,[airport_table.new_municipality == temperature_table.city,immigration_table.i94mon == temperature_table.month.cast("double")], how='inner')\
            .join(demographics_table,[temperature_table.city == demographics_table.city,airport_table.state == demographics_table.state_code],how ='inner')\
            .join(time_table,[immigration_table.arrival_date == time_table.date],how='inner')\
            .withColumn("fact_id", monotonically_increasing_id())\
            .select(col('fact_id'),time_table.date,immigration_table.cicid,airport_table.ident,temperature_table.month,\
                      temperature_table.city,demographics_table.state,temperature_table.average_temperature,demographics_table.foreign_born)
    
    # Check if fact table is empty
    if  us_city_immigration_table.count() == 0:
        print("No data in the fact table")
        raise ValueError

    # Check if fact table columns are Null
    if us_city_immigration_table.filter("date is null or cicid is null or ident is null or month is null or city is null or state is null").count() != 0:
        print("Pk column cannot be null")
        raise ValueError
        
    if us_city_immigration_table.filter("average_temperature is null or foreign_born is null or city is null").count() != 0:
        print("Column cannot be null")
        raise ValueError

    # write us_city_immigration_table to a parquet file 
    us_city_immigration_table.write.partitionBy('month','city').parquet(os.path.join(output_data,'us_city_immigration_table'),'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://capstone-input-data/"
    output_data = "s3a://capstone-output-data/"
    bucket = "capstone-input-data"
    prefix = "immigration/i94"
   
    #Read temperature data and write it to a parquet file
    process_temperature_data(spark, input_data, output_data) 
    
    #Read airport data and write it to a parquet file
    process_airport_data(spark, input_data, output_data)
    
    #Read city data and write it to a parquet file
    process_city_data(spark, input_data, output_data)
    
    #Read immigration data and write it to a parquet file
    process_immigration_data(spark, input_data, output_data, bucket, prefix)
    
    #Read dimension table and create us_city_iimigration fact table and write it to a parquet file
    load_us_city_immigration_fact_table(spark, output_data)


if __name__ == "__main__":
    main()

    
    