# Databricks notebook source
# MAGIC %python
# MAGIC # Create Tables
# MAGIC # This notebook creates tables in Databricks from external files, primarily to make SQL access easier. 
# MAGIC # You can run this notebook yourself, but if you follow the lessons in order, one of the other notebooks will run this notebook for you.
# MAGIC None # suppress output

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pathlib2 import Path
# MAGIC 
# MAGIC filePeople = "/mnt/training/dataframes/people-10m.parquet"
# MAGIC fileNames = "dbfs:/mnt/training/ssn/names.parquet"
# MAGIC fileBlog = "dbfs:/mnt/training/databricks-blog.json"
# MAGIC fileCity = "dbfs:/mnt/training/City-Data.parquet"
# MAGIC filePlane = "dbfs:/mnt/training/asa/planes/plane-data.csv"
# MAGIC fileSmall = "dbfs:/mnt/training/asa/flights/small.csv"
# MAGIC fileGeo = "dbfs:/mnt/training/ip-geocode.parquet"
# MAGIC fileBikes = "dbfs:/mnt/training/bikeSharing/data-001/day.csv"
# MAGIC 
# MAGIC dbutils.fs.ls(filePeople)
# MAGIC dbutils.fs.ls(fileNames)
# MAGIC dbutils.fs.ls(fileBlog)
# MAGIC dbutils.fs.ls(fileCity)
# MAGIC dbutils.fs.ls(filePlane)
# MAGIC dbutils.fs.ls(fileSmall)
# MAGIC dbutils.fs.ls(fileGeo)
# MAGIC dbutils.fs.ls(fileBikes)
# MAGIC 
# MAGIC None # suppress output

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def deleteTables() :
# MAGIC   try:
# MAGIC     tables=spark.catalog.listTables("Databricks")
# MAGIC   except:
# MAGIC     return
# MAGIC   for table in tables:
# MAGIC       spark.sql("DROP TABLE {}.{}".format("Databricks", table.name))
# MAGIC       
# MAGIC # deleteTables()
# MAGIC       
# MAGIC None # suppress output

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC spark.sql("CREATE DATABASE IF NOT EXISTS Databricks")
# MAGIC spark.sql("USE Databricks")
# MAGIC 
# MAGIC None # suppress output

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC spark.sql("""
# MAGIC CREATE TABLE IF NOT EXISTS People10M
# MAGIC USING parquet
# MAGIC OPTIONS (
# MAGIC   path "{}"
# MAGIC )""".format(filePeople));
# MAGIC 
# MAGIC spark.sql("uncache TABLE People10M")
# MAGIC 
# MAGIC None # suppress output

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC spark.sql("""
# MAGIC CREATE TABLE IF NOT EXISTS SSANames
# MAGIC USING parquet
# MAGIC OPTIONS (
# MAGIC   path "{}"
# MAGIC )""".format(fileNames))
# MAGIC 
# MAGIC spark.sql("uncache TABLE SSANames")
# MAGIC 
# MAGIC None # suppress output

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC spark.sql("""
# MAGIC CREATE TABLE IF NOT EXISTS DatabricksBlog
# MAGIC USING json
# MAGIC OPTIONS (
# MAGIC   path "{}",
# MAGIC   inferSchema "true"
# MAGIC )""".format(fileBlog))
# MAGIC 
# MAGIC spark.sql("uncache TABLE DatabricksBlog")
# MAGIC 
# MAGIC None # suppress output

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC spark.sql("""
# MAGIC CREATE TABLE IF NOT EXISTS CityData
# MAGIC USING parquet
# MAGIC OPTIONS (
# MAGIC   path "{}"
# MAGIC )""".format(fileCity))
# MAGIC 
# MAGIC spark.sql("uncache TABLE CityData")
# MAGIC 
# MAGIC None # suppress output

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC spark.sql("""
# MAGIC CREATE TABLE IF NOT EXISTS AirlinePlane (`tailnum` STRING, `type` STRING, `manufacturer` STRING, `issue_date` STRING, `model` STRING, `status` STRING, `aircraft_type` STRING, `engine_type` STRING, `year` STRING)
# MAGIC USING csv 
# MAGIC OPTIONS (
# MAGIC   header "true",
# MAGIC   delimiter ",",
# MAGIC   inferSchema "false",
# MAGIC   path "{}"
# MAGIC )""".format(filePlane))
# MAGIC 
# MAGIC spark.sql("uncache TABLE AirlinePlane")
# MAGIC 
# MAGIC None # suppress output

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC spark.sql("""
# MAGIC CREATE TABLE IF NOT EXISTS AirlineFlight (`Year` INT, `Month` INT, `DayofMonth` INT, `DayOfWeek` INT, `DepTime` STRING, `CRSDepTime` INT, `ArrTime` STRING, `CRSArrTime` INT, `UniqueCarrier` STRING, `FlightNum` INT, `TailNum` STRING, `ActualElapsedTime` STRING, `CRSElapsedTime` INT, `AirTime` STRING, `ArrDelay` STRING, `DepDelay` STRING, `Origin` STRING, `Dest` STRING, `Distance` INT, `TaxiIn` INT, `TaxiOut` INT, `Cancelled` INT, `CancellationCode` STRING, `Diverted` INT, `CarrierDelay` STRING, `WeatherDelay` STRING, `NASDelay` STRING, `SecurityDelay` STRING, `LateAircraftDelay` STRING)
# MAGIC USING CSV OPTIONS (
# MAGIC   header="true", 
# MAGIC   delimiter=",", 
# MAGIC   inferSchema="true", 
# MAGIC   path="{}")
# MAGIC """.format(fileSmall))
# MAGIC 
# MAGIC spark.sql("uncache TABLE AirlineFlight")
# MAGIC 
# MAGIC None # suppress output

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC spark.sql("""
# MAGIC CREATE TABLE IF NOT EXISTS IPGeocode (`startingIP` DECIMAL(38,0), `endingIP` DECIMAL(38,0), `countryCode2` STRING, `countryCode3` STRING, `country` STRING, `stateProvince` STRING, `city` STRING, `latitude` DOUBLE, `longitude` DOUBLE)
# MAGIC USING parquet
# MAGIC OPTIONS (
# MAGIC   path "{}"
# MAGIC )
# MAGIC """.format(fileGeo))
# MAGIC 
# MAGIC spark.sql("uncache TABLE IPGeocode")
# MAGIC 
# MAGIC None # suppress output

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC spark.sql("""
# MAGIC CREATE TABLE IF NOT EXISTS BikeSharingDay
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   path "{}",
# MAGIC   inferSchema "true",
# MAGIC   header "true"
# MAGIC )""".format(fileBikes))
# MAGIC 
# MAGIC spark.sql("uncache TABLE BikeSharingDay")
# MAGIC 
# MAGIC None # suppress output

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC displayHTML("Created Database and Tables...") # suppress output