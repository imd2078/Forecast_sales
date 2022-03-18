#!/usr/bin/env python
# coding: utf-8

# In[213]:


import os

import socket
import pyspark.sql
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, ceil, concat_ws, window, avg, sum, round
from pyspark.sql import Row
from pyspark.sql.types import StructType
from pyspark.streaming.kafka import KafkaUtils
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 pyspark-shell'

import math


# In[214]:


#longitude and latitude boundaries
#upper left boundaries
up_lft_long = -74.916578
up_lft_lat = 41.47718278

#lower right boundaries
lo_rght_long = -73.120778
lo_rght_lat = 40.12971598

# Longitude and latitude from the upper left corner of the grid, to help conversion
init_long = -74.916578
init_lat = 41.47718278
# Longitude and latitude from the lower right boundaries for filtering purposes
limit_long = -73.120778
limit_lat = 40.12971598
# In[215]:


def filtering(line):
    #split the line as the input is line by line with separation by commas
    splitted_line = line.split(',')
    #return boolean value telling us if the entry belongs in the grid
    fltr =  (
        (len(line) > 0) and \
        (float(splitted_line[6]) > up_lft_long) and \
        (float(splitted_line[6]) < lo_rght_long) and \
        (float(splitted_line[7]) > lo_rght_lat) and \
        (float(splitted_line[7]) < up_lft_lat) and \
        (float(splitted_line[8]) > up_lft_long) and \
        (float(splitted_line[8]) < lo_rght_long) and \
        (float(splitted_line[9]) > lo_rght_lat) and \
        (float(splitted_line[9]) < up_lft_lat) and \
        (float(splitted_line[5]) > 0) and \
        (float(splitted_line[4]) > 0) and \
        (float(splitted_line[16]) > 0)
        )
    return fltr


# In[216]:


# Returns every row as it was with the additional areas based on the locations
#the cell number each point belongs to is returned
def area(line, _type = "big"):
    
    # Split the line by a ,
    splitted_line = line.split(',')
    line = splitted_line
    
    # Longitude and latitude that correspond to a shift in 500 meters
    long_shift = 0.005986
    lat_shift = 0.004491556

    # Longitude and latitude that correspond to a shift in 250 meters
    if _type == "small":
        long_shift = long_shift / 2
        lat_shift = lat_shift / 2
    #returning every value with the cell numbers    
    return (
        line[0], line[1], line[2], line[3], line[4], line[5], line[6],
        line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16],
        str(math.ceil((float(line[6])-up_lft_long)/long_shift)) + "-" + str(math.ceil((up_lft_lat-float(line[7]))/lat_shift)),
        str(math.ceil((float(line[8])-up_lft_long)/long_shift)) + "-" + str(math.ceil((up_lft_lat-float(line[9]))/lat_shift))
        )


# In[217]:


# Defines the boundary like the filter function but does it in an optimizd way
#The input for the above filter function is line by line whereas the inpu to this function is the whole data
def filter_full_df(data):
    split_lines = split(data["value"], ",")
    data = data.filter(split_lines.getItem(6) < lo_rght_long)         .filter(split_lines.getItem(6) > up_lft_long)         .filter(split_lines.getItem(7) < up_lft_lat)         .filter(split_lines.getItem(7) > lo_rght_lat)         .filter(split_lines.getItem(8) < lo_rght_long)         .filter(split_lines.getItem(8) > up_lft_long)         .filter(split_lines.getItem(9) < up_lft_lat)         .filter(split_lines.getItem(9) > lo_rght_lat)         .filter(split_lines.getItem(5) > 0)         .filter(split_lines.getItem(4) > 0)         .filter(split_lines.getItem(16) > 0)
    return data


# In[218]:


#Getting the cells numbers for pickup and dropoff and concatenating them
def area_df(data, _type = "big"):
    
    # Longitude and latitude that correspond to a shift in 500 meters
    long_shift = 0.005986
    lat_shift = 0.004491556

    # Longitude and latitude that correspond to a shift in 250 meters
    if _type == "small":
        long_shift = long_shift / 2
        lat_shift = lat_shift / 2
        
    split_lines = split(data["value"], ",")
    
    data = data     .withColumn("cell_pickup_longitude", ceil((split_lines.getItem(6).cast("double") - init_long) / long_shift))     .withColumn("cell_pickup_latitude", -ceil((split_lines.getItem(7).cast("double") - init_lat) / lat_shift))     .withColumn("cell_dropoff_longitude", ceil((split_lines.getItem(8).cast("double") - init_long) / long_shift))     .withColumn("cell_dropoff_latitude", -ceil((split_lines.getItem(9).cast("double") - init_lat) / lat_shift))
    
    data = data         .withColumn("cell_pickup", concat_ws("-", data["cell_pickup_latitude"], data["cell_pickup_longitude"]))         .withColumn("cell_dropoff", concat_ws("-", data["cell_dropoff_latitude"], data["cell_dropoff_longitude"]))         .drop("cell_pickup_latitude", "cell_pickup_longitude", "cell_dropoff_latitude", "cell_dropoff_longitude")
    
    return data


# In[219]:


# The routes are obtained by concatenating the pickup and the drop off cell numbers
def paths(data):
    data = data.withColumn("route", concat_ws("/", data["cell_pickup"], data["cell_dropoff"]))
    return data


# In[220]:


# Function that does basic pre processing
def pre_process(data):
    # Filters empty rows
    data = data.na.drop(how="all")
    # Filters out all the data falling outside the grid range and 0 cvalues for fare, distance, time for travel
    data = filter_full_df(data)
    return data


# In[221]:


# Get only the specified columns
# Columns is an array with tuples inside, the tuples got the column name and the respective type
def get_columns(data, columns):
    # Array with columns for index
    whole_columns = ["medallion", "hack_license", "pickup_datetime", "dropoff_datetime", "trip_time",
                    "trip_distance", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
                    "payment_type", "fare_amount", "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount"]
    split_lines = split(data["value"], ",")
    for column in columns:
        data = data.withColumn(column[0], split_lines.getItem(whole_columns.index(column[0])).cast(column[1]))
    
    return data


# In[222]:


def dumpBatchDF(df, epoch_id):
    df.show(20, False)


# In[223]:


#Query1 - Most frequent routes


# In[224]:


# Get spark session instance
from pyspark import SparkConf
conf = SparkConf()

conf.set("spark.jars", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0")
spark = SparkSession     .builder     .appName("Kafka Pstr Project 1")     .config("spark.jars", "spark-streaming-kafka-0-8-assembly_2.11-2.4.8.jar")    .getOrCreate()


# In[225]:


#userSchema = StructType().add("medallion", "string").add("hack_license", "string").add("pickup_datetime","timestamp")     .add("dropoff_datetime","timestamp").add("trip_time_in_secs","double")    .add("trip_distance_miles","double").add("pickup_longitude","double")    .add("pickup_latitude","double").add("dropoff_longitude","double")    .add("dropoff_latitude","double").add("payment_type","string")    .add("fare_amount","double").add("surcharge","double").add("mta_tax","double")    .add("tip_amount","double").add("tolls_amount","double").add("total_amount","double")


# In[226]:


# Definestopic_name="ccass"
#KafkaUtils.createStream(spark,"localhost:2181",'spark-streaming',{topic_name:1})


# In[227]:


#spark.sql("set spark.sql.streaming.schemaInference=true")


#data = spark.readStream.format("csv").option("sep", ",").option("header",True).option("maxFilesPerTrigger",1).schema(userSchema).load(r"D:\sorted_data.csv")
data = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "ccass").option("startingOffsets", """{"ccass":{"0":15250000}}""").load()
data.printSchema()

# Apply filters
data = pre_process(data)

# Get specified columns only
columns = [("dropoff_datetime", "timestamp"), ("dropoff_longitude", "double"), ("dropoff_latitude", "double"),
          ("pickup_longitude", "double"), ("pickup_latitude", "double")]

data = get_columns(data, columns)

# Get the cells from locations
data = area(data)

# Join areas to form routes
data = paths(data)

# Count the occurrences for each route and present only the top 10 most frequent routes
frequent_routes = data.withWatermark("dropoff_datetime", "30 minutes").groupBy(window(data.dropoff_datetime, "30 minutes", "10 minutes"),"route").count().orderBy("window","count",ascending=False).limit(10)
# frequent_routes
#foreachBatch(dumpBatchDF)
query = frequent_routes.writeStream.format("console").trigger(processingTime="10 seconds").outputMode("complete").foreachBatch(dumpBatchDF).start()


query.awaitTermination(60)

query.stop()
spark.stop()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




