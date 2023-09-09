import findspark
findspark.init("C:\Program Files\spark\spark-3.3.2-bin-hadoop2")

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType


'''
Initiate spark session
'''
def create_spark_session():
    spark = SparkSession.builder.appName('Bank_ATM_ETL').getOrCreate()
    return spark

'''
Laod data with custom schema
'''
def getDF(spark, path):
    Schema = StructType([StructField('year', IntegerType(), True),
                            StructField('month', StringType(), True),
                            StructField('day', IntegerType(), True),
                            StructField('weekday', StringType(), True),
                            StructField('hour', IntegerType(), True),
                            StructField('atm_status', StringType(), True),
                            StructField('atm_id', StringType(), True),
                            StructField('atm_manufacturer', StringType(), True),
                            StructField('atm_location', StringType(), True),
                            StructField('atm_streetname', StringType(), True),
                            StructField('atm_street_number', IntegerType(), True),
                            StructField('atm_zipcode', IntegerType(), True),
                            StructField('atm_lat', DoubleType(), True),
                            StructField('atm_lon', DoubleType(), True),
                            StructField('currency', StringType(), True),
                            StructField('card_type', StringType(), True),
                            StructField('transaction_amount', IntegerType(), True),
                            StructField('service', StringType(), True),
                            StructField('message_code', StringType(), True),
                            StructField('message_text', StringType(), True),
                            StructField('weather_lat', DoubleType(), True),
                            StructField('weather_lon', DoubleType(), True),
                            StructField('weather_city_id', IntegerType(), True),
                            StructField('weather_city_name', StringType(), True),
                            StructField('temp', DoubleType(), True),
                            StructField('pressure', IntegerType(), True),
                            StructField('humidity', IntegerType(), True),
                            StructField('wind_speed', IntegerType(), True),
                            StructField('wind_deg', IntegerType(), True),
                            StructField('rain_3h', DoubleType(), True),
                            StructField('clouds_all', IntegerType(), True),
                            StructField('weather_id', IntegerType(), True),
                            StructField('weather_main', StringType(), True),
                            StructField('weather_description', StringType(), True)])

    df = spark.read.format("csv")\
                    .option("header", "false")\
                    .option("inferSchema", "false")\
                    .schema(Schema)\
                    .option("mode", "FAILFAST")\
                    .load(path)
    
    return df

'''
processing location dimension and storing it in S3
'''
def process_FACT_DIM(df):
    # creating dim_location
    location = df.select("atm_location", "atm_streetname", "atm_street_number", "atm_zipcode", "atm_lat", "atm_lon").distinct()
    dim_location = location.select(row_number().over(Window.orderBy(location[0])).alias("location_id"), "*")
    DIM_LOCATION = dim_location.withColumnRenamed('atm_location','location')\
                                .withColumnRenamed('atm_streetname','streetname')\
                                .withColumnRenamed('atm_street_number','street_number')\
                                .withColumnRenamed('atm_zipcode','zipcode')\
                                .withColumnRenamed('atm_lat','lat')\
                                .withColumnRenamed('atm_lon','lon')
    pushtos3(DIM_LOCATION,'dim_location')
    print('DIM_LOCATION created. !')

    # creating dim_card_type
    card_type =  df.select('card_type').distinct()
    DIM_CARD_TYPE = card_type.select(row_number().over(Window.orderBy(card_type[0])).alias("card_type_id"), "*")
    pushtos3(DIM_CARD_TYPE,'dim_card_type')
    print('DIM_CARD_TYPE created. !')

    # creating dim_atm
    atm = df.select(col('atm_id').alias('atm_number'), 'atm_manufacturer', 'atm_lat', 'atm_lon')
    atm = atm.join(dim_location, on = ['atm_lat', 'atm_lon'], how = "left")
    dim_atm = atm.select('atm_number', 'atm_manufacturer', 'location_id').distinct()
    DIM_ATM = dim_atm.select(row_number().over(Window.orderBy(dim_atm[0])).alias('atm_id'), 'atm_number', 'atm_manufacturer', col('location_id').alias('atm_location_id'))
    pushtos3(DIM_ATM,'dim_atm')
    print('DIM_ATM created. !')

    # creating dim_date
    date = df.select('year', 'month', 'day', 'hour', 'weekday')
    date = date.withColumn('full_date_time', to_timestamp(concat(date.year, lit('-'), date.month, lit('-'),date.day, lit(' '), date.hour), 'yyyy-MMMM-d H'))
    date = date.select('full_date_time', 'year', 'month', 'day', 'hour', 'weekday').distinct()
    DIM_DATE = date.select(row_number().over(Window.orderBy(date[0])).alias('date_id'), '*')
    pushtos3(DIM_DATE,'dim_date')
    print('DIM_DATE created. !')

    # Creation of transaction Fact table
    fact_loc = df.withColumnRenamed('atm_location','location')\
        .withColumnRenamed('atm_streetname','streetname')\
        .withColumnRenamed('atm_street_number','street_number')\
        .withColumnRenamed('atm_zipcode','zipcode')\
        .withColumnRenamed('atm_lat','lat')\
        .withColumnRenamed('atm_lon','lon')

    # joining original dataframe with DIM_LOCTION
    fact_loc = fact_loc.join(DIM_LOCATION, on = ['location', 'streetname', 'street_number', 'zipcode', 'lat', 'lon'], how = "left")
    fact_loc = fact_loc.withColumnRenamed('atm_id', 'atm_number').withColumnRenamed('location_id', 'atm_location_id')
    # joining the dataframe with DIM_ATM
    fact_atm = fact_loc.join(DIM_ATM, on = ['atm_number', 'atm_manufacturer', 'atm_location_id'], how = "left")
    # performing necessary transformations, same as done to atm table
    fact_atm = fact_atm.withColumnRenamed('atm_location_id', 'weather_loc_id')
    # joining the dataframe with DIM_DATE
    fact_date = fact_atm.join(DIM_DATE, on = ['year', 'month', 'day', 'hour', 'weekday'], how = "left")
    # joining the dataframe with DIM_CARD_TYPE
    fact_atm_trans = fact_date.join(DIM_CARD_TYPE, on = ['card_type'], how = "left")
    # creating primary key of fact table
    FACT_ATM_TRANS = fact_atm_trans.withColumn("trans_id", row_number().over(Window.orderBy('date_id')))
    # selecting and arranging only the required columns according to the target model
    FACT_ATM_TRANS = FACT_ATM_TRANS.select('trans_id', 'atm_id', 'weather_loc_id', 'date_id', 'card_type_id', 
    'atm_status', 'currency', 'service', 'transaction_amount', 'message_code', 'message_text', 'rain_3h', 
    'clouds_all', 'weather_id', 'weather_main', 'weather_description')

    pushtos3(FACT_ATM_TRANS,'fact_atm_trans')
    print('FACT_ATM_TRANS created. !')

'''
saving Pyspark Dataframe to location
'''
def pushtos3(table_name,file_name):
    table_name.write.format('csv').option('header','false').save(f'./{file_name}', mode='overwrite')


def main():
    path = "D:/Study/UPGRAD_EPGDS/DE/ATM-ETL-assignment/kaggle/part-m-00000.csv"
    spark = create_spark_session()
    df = getDF(spark, path)
    process_FACT_DIM(df)
    spark.stop()


if __name__ == "__main__":
    main()
