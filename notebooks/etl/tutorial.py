# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "6da4208d-5acf-496e-96a2-2c8c115d6dfa",
       "fs.azure.account.oauth2.client.secret": "-6gQwtE_HK/0G_2e3P-Bx5Rd/v3/L0Rw",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/cafe5856-f1cc-43b5-b041-4cfc98c266e7/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}
dbutils.fs.unmount("/mnt/flightdata")
dbutils.fs.mount(
source = "abfss://test@grifffruitvegsa.dfs.core.windows.net",
mount_point = "/mnt/flightdata",
extra_configs = configs)

# COMMAND ----------

# Use the previously established DBFS mount point to read the data.
# create a data frame to read data.

flightDF = spark.read.format('csv').options(
    header='true', inferschema='true').load("/mnt/flightdata/*.csv")

# read the airline csv file and write the output to parquet format for easy query.
flightDF.write.mode("append").parquet("/mnt/flightdata/parquet/flights")
print("Done")

# COMMAND ----------

import os.path
import IPython
from pyspark.sql import SQLContext
display(dbutils.fs.ls("/mnt/flightdata"))

# COMMAND ----------

dbutils.fs.put("/mnt/flightdata/1.txt", "Hello, World!", True)
dbutils.fs.ls("/mnt/flightdata/parquet/flights")

# COMMAND ----------

# Copy this into a Cmd cell in your notebook.
acDF = spark.read.format('csv').options(
    header='true', inferschema='true').load("/mnt/flightdata/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2019_1.csv")
acDF.write.parquet('/mnt/flightdata/parquet/airlinecodes')





# COMMAND ----------

# read the existing parquet file for the flights database that was created earlier
flightDF = spark.read.format('parquet').options(
    header='true', inferschema='true').load("/mnt/flightdata/parquet/flights")

# print the schema of the dataframes
acDF.printSchema()
flightDF.printSchema()

# print the flight database size
print("Number of flights in the database: ", flightDF.count())

# show the first 20 rows (20 is the default)
# to show the first n rows, run: df.show(n)
acDF.show(100, False)
flightDF.show(20, False)

# COMMAND ----------

# Display to run visualizations
# preferably run this in a separate cmd cell
display(flightDF)

# COMMAND ----------

# Run each of these queries, preferably in a separate cmd cell for separate analysis
# create a temporary sql view for querying flight information
FlightTable = spark.read.parquet('/mnt/flightdata/parquet/flights')
FlightTable.createOrReplaceTempView('FlightTable')

# COMMAND ----------

# create a temporary sql view for querying airline code information
AirlineCodes = spark.read.parquet('/mnt/flightdata/parquet/airlinecodes')
AirlineCodes.createOrReplaceTempView('AirlineCodes')

# COMMAND ----------

# using spark sql, query the parquet file to return total flights in January and February 2016
out1 = spark.sql("SELECT * FROM FlightTable WHERE Month=1 and Year= 2016")
NumJan2016Flights = out1.count()
out2 = spark.sql("SELECT * FROM FlightTable WHERE Month=2 and Year= 2016")
NumFeb2016Flights = out2.count()
print("Jan 2016: ", NumJan2016Flights, " Feb 2016: ", NumFeb2016Flights)
Total = NumJan2016Flights+NumFeb2016Flights
print("Total flights combined: ", Total)

# COMMAND ----------

# List out all the airports in Texas
out = spark.sql(
    "SELECT distinct(OriginCityName) FROM FlightTable where OriginStateName = 'Texas'")
print('Airports in Texas: ', out.show(100))

# COMMAND ----------

# find all airlines that fly from Texas
out1 = spark.sql(
    "SELECT distinct(Reporting_Airline) FROM FlightTable WHERE OriginStateName='Texas'")
print('Airlines that fly to/from Texas: ', out1.show(100, False))