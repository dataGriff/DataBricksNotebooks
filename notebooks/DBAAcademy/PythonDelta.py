# Databricks notebook source
# MAGIC %md
# MAGIC Create appropriate database

# COMMAND ----------

username = "griff"
dbutils.widgets.text("username", username)
spark.sql(f"CREATE DATABASE IF NOT EXISTS dbacademy_{username}")
spark.sql(f"USE dbacademy_{username}")
health_tracker = f"/dbacademy/{username}/DLRS/healthtracker/"

# COMMAND ----------

# MAGIC %md
# MAGIC Setup number of partitions

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 8)

# COMMAND ----------

# MAGIC %md
# MAGIC Download Health Data

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_1.json
# MAGIC wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_2.json
# MAGIC wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_2_late.json
# MAGIC wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_3.json

# COMMAND ----------

# MAGIC %md
# MAGIC Verify the downloads

# COMMAND ----------

# MAGIC %sh ls

# COMMAND ----------

# MAGIC %md
# MAGIC Move to raw directory

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_1.json", 
              health_tracker + "raw/health_tracker_data_2020_1.json")
dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_2.json", 
              health_tracker + "raw/health_tracker_data_2020_2.json")
dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_2_late.json", 
              health_tracker + "raw/health_tracker_data_2020_2_late.json")
dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_3.json", 
              health_tracker + "raw/health_tracker_data_2020_3.json")

# COMMAND ----------

file_path = health_tracker + "raw/health_tracker_data_2020_1.json"
 
health_tracker_data_2020_1_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC TAke a look at the data

# COMMAND ----------

display(health_tracker_data_2020_1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We're going to process the files into parquet.
# MAGIC To ensure its idempotent we want to clear the processed directory first

# COMMAND ----------

dbutils.fs.rm(health_tracker + "processed", recurse=True)

# COMMAND ----------

# MAGIC %md 
# MAGIC Create a reusable function to be performed against each file then execute it against the dataframe we created from the file earlier

# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime
 
def process_health_tracker_data(dataframe):
  return (
    dataframe
    .withColumn("time", from_unixtime("time"))
    .withColumnRenamed("device_id", "p_device_id")
    .withColumn("time", col("time").cast("timestamp"))
    .withColumn("dte", col("time").cast("date"))
    .withColumn("p_device_id", col("p_device_id").cast("integer"))
    .select("dte", "time", "heartrate", "name", "p_device_id")
    )
  
processedDF = process_health_tracker_data(health_tracker_data_2020_1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the processed data frame to a processed parquet file partitioned by device id

# COMMAND ----------

(processedDF.write
 .mode("overwrite")
 .format("parquet")
 .partitionBy("p_device_id")
 .save(health_tracker + "processed"))

# COMMAND ----------

# MAGIC %md
# MAGIC Register this parquet table in the metastore

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS health_tracker_processed;
# MAGIC 
# MAGIC CREATE TABLE health_tracker_processed                        
# MAGIC USING PARQUET                
# MAGIC LOCATION "/dbacademy/$username/DLRS/healthtracker/processed"

# COMMAND ----------

# MAGIC %md
# MAGIC currently count shows no rows

# COMMAND ----------

health_tracker_processed = spark.read.table("health_tracker_processed")
health_tracker_processed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Need to register the partitions of the table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MSCK REPAIR TABLE health_tracker_processed

# COMMAND ----------

# MAGIC %md
# MAGIC Counting now the table is repaired will show right results

# COMMAND ----------

health_tracker_processed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Describe the processed parquet table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE DETAIL health_tracker_processed

# COMMAND ----------

# MAGIC %md
# MAGIC Convert parquet table to delta table

# COMMAND ----------

from delta.tables import DeltaTable

parquet_table = f"parquet.`{health_tracker}processed`"
partitioning_scheme = "p_device_id int"

DeltaTable.convertToDelta(spark, parquet_table, partitioning_scheme)

# COMMAND ----------

# MAGIC %md
# MAGIC Register delta table in the metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS health_tracker_processed;
# MAGIC 
# MAGIC CREATE TABLE health_tracker_processed
# MAGIC USING DELTA
# MAGIC LOCATION "/dbacademy/$username/DLRS/healthtracker/processed"

# COMMAND ----------

# MAGIC %md
# MAGIC Verify that the new table is now in delta format

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL health_tracker_processed

# COMMAND ----------

# MAGIC %md
# MAGIC As this is a delta table al lthe metadata is already up to date and is immediately querayable without needing to be repaired like the parquet file

# COMMAND ----------

health_tracker_processed = spark.read.table("health_tracker_processed")
health_tracker_processed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Remove files from directory to make this process idempotent

# COMMAND ----------



# COMMAND ----------

dbutils.fs.rm(health_tracker + "gold/health_tracker_user_analytics",
              recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Create aggregated gold data frame

# COMMAND ----------

from pyspark.sql.functions import col, avg, max, stddev

health_tracker_gold_user_analytics = (
  health_tracker_processed
  .groupby("p_device_id")
  .agg(avg(col("heartrate")).alias("avg_heartrate"),
       max(col("heartrate")).alias("max_heartrate"),
       stddev(col("heartrate")).alias("stddev_heartrate"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the data to a delta table

# COMMAND ----------

(health_tracker_gold_user_analytics.write
 .format("delta")
 .mode("overwrite")
 .save(health_tracker + "gold/health_tracker_user_analytics"))

# COMMAND ----------

# MAGIC %md
# MAGIC Register table in metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS health_tracker_gold_user_analytics;
# MAGIC 
# MAGIC CREATE TABLE health_tracker_gold_user_analytics
# MAGIC USING DELTA
# MAGIC LOCATION "/dbacademy/$username/DLRS/healthtracker/gold/health_tracker_user_analytics"

# COMMAND ----------

display(spark.read.table("health_tracker_gold_user_analytics"))

# COMMAND ----------

# MAGIC %md
# MAGIC Get next file into dataframe ready for loading

# COMMAND ----------

file_path = health_tracker + "raw/health_tracker_data_2020_2.json"
 
health_tracker_data_2020_2_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Process the file using the same function that was used for file 1

# COMMAND ----------

processedDF = process_health_tracker_data(health_tracker_data_2020_2_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Append the new file to the existing data

# COMMAND ----------

(processedDF.write
 .mode("append")
 .format("delta")
 .save(health_tracker + "processed"))

# COMMAND ----------

# MAGIC %md
# MAGIC Use time travel to see version 0 of the data before the insert above

# COMMAND ----------

(spark.read
 .option("versionAsOf", 0)
 .format("delta")
 .load(health_tracker + "processed")
 .count())

# COMMAND ----------

# MAGIC %md
# MAGIC Now count the most recent data with the new file added

# COMMAND ----------

health_tracker_processed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Count number of records per device

# COMMAND ----------

from pyspark.sql.functions import count

display(
  spark.read
  .format("delta")
  .load(health_tracker + "processed")
  .groupby("p_device_id")
  .agg(count("*"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Have a look at what records we are missing, which looks like device 4 for last few days

# COMMAND ----------

display(
  spark.read
  .format("delta")
  .load(health_tracker + "processed")
  .where(col("p_device_id").isin([3,4]))
)

# COMMAND ----------

# MAGIC %md 
# MAGIC We note that there are negative heart rates which must be fauty data

# COMMAND ----------

broken_readings = (
  health_tracker_processed
  .select(col("heartrate"), col("dte"))
  .where(col("heartrate") < 0)
  .groupby("dte")
  .agg(count("heartrate"))
  .orderBy("dte")
)
 
broken_readings.createOrReplaceTempView("broken_readings")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM broken_readings

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC SELECT SUM(`count(heartrate)`) FROM broken_readings

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have seen our broken data we want to fix it with an upsert

# COMMAND ----------

# MAGIC %md
# MAGIC For the broken values we're just going to get the most recent values before and after the broken ones using lag and lead into a data frame

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, lead
 
dteWindow = Window.partitionBy("p_device_id").orderBy("dte")
 
interpolatedDF = (
  spark.read
  .table("health_tracker_processed")
  .select(col("dte"),
          col("time"),
          col("heartrate"),
          lag(col("heartrate")).over(dteWindow).alias("prev_amt"),
          lead(col("heartrate")).over(dteWindow).alias("next_amt"),
          col("name"),
          col("p_device_id"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a dataframe of updates based on the interpolated values created above

# COMMAND ----------

updatesDF = (
  interpolatedDF
  .where(col("heartrate") < 0)
  .select(col("dte"),
          col("time"),
          ((col("prev_amt") + col("next_amt"))/2).alias("heartrate"),
          col("name"),
          col("p_device_id"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Compare schemas of destinaiton and update data frame

# COMMAND ----------

health_tracker_processed.printSchema()
updatesDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Have a look at the count in the update data frame

# COMMAND ----------

updatesDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC We also have late arriving data provide to us we can put into a dataframe

# COMMAND ----------

file_path = health_tracker + "raw/health_tracker_data_2020_2_late.json"
 
health_tracker_data_2020_2_late_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Insert this late data into a dataframe

# COMMAND ----------

insertsDF = process_health_tracker_data(health_tracker_data_2020_2_late_df)

# COMMAND ----------

# MAGIC %md
# MAGIC View the schema of the inserts dataframe and confirm same as the destination

# COMMAND ----------

insertsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Create the upsert data frame by unioning the update and insert data frame you created

# COMMAND ----------

upsertsDF = updatesDF.union(insertsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Again confirm the schema

# COMMAND ----------

upsertsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Perform the upsert

# COMMAND ----------

processedDeltaTable = DeltaTable.forPath(spark, health_tracker + "processed")

update_match = """
  health_tracker.time = upserts.time 
  AND 
  health_tracker.p_device_id = upserts.p_device_id
"""

update = { "heartrate" : "upserts.heartrate" }

insert = {
  "p_device_id" : "upserts.p_device_id",
  "heartrate" : "upserts.heartrate",
  "name" : "upserts.name",
  "time" : "upserts.time",
  "dte" : "upserts.dte"
}

(processedDeltaTable.alias("health_tracker")
 .merge(upsertsDF.alias("upserts"), update_match)
 .whenMatchedUpdate(set=update)
 .whenNotMatchedInsert(values=insert)
 .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC View the first version of the table

# COMMAND ----------

(spark.read
 .option("versionAsOf", 1)
 .format("delta")
 .load(health_tracker + "processed")
 .count())

# COMMAND ----------

# MAGIC %md 
# MAGIC count most recent version

# COMMAND ----------

health_tracker_processed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Check the history of the delta table

# COMMAND ----------

display(processedDeltaTable.history())

# COMMAND ----------

# MAGIC %md
# MAGIC Lets have a look how many broken readings we have now after the upsert as the new data might also have faulty data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(`count(heartrate)`) FROM broken_readings

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm these are new faulty records

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT SUM(`count(heartrate)`) FROM broken_readings WHERE dte < '2020-02-25'

# COMMAND ----------

# MAGIC %md
# MAGIC Check updates data frame count, its important to note the lazy transformation means this is automtically updated as it is just a pointer

# COMMAND ----------

updatesDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Perform upsert for the broken data again

# COMMAND ----------

upsertsDF = updatesDF

(processedDeltaTable.alias("health_tracker")
 .merge(upsertsDF.alias("upserts"), update_match)
 .whenMatchedUpdate(set=update)
 .whenNotMatchedInsert(values=insert)
 .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC Now look how many broken readings there are, should be none

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT SUM(`count(heartrate)`) FROM broken_readings

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC We are going to be amending a schema during next months load by adding new column

# COMMAND ----------

file_path = health_tracker + "raw/health_tracker_data_2020_3.json"

health_tracker_data_2020_3_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC To load this data with a new schema we need to redefine our loading function

# COMMAND ----------

def process_health_tracker_data(dataframe):
  return (
    dataframe
    .withColumn("time", from_unixtime("time"))
    .withColumnRenamed("device_id", "p_device_id")
    .withColumn("time", col("time").cast("timestamp"))
    .withColumn("dte", col("time").cast("date"))
    .withColumn("p_device_id", col("p_device_id").cast("integer"))
    .select("dte", "time", "device_type", "heartrate", "name", "p_device_id")
    )
  
processedDF = process_health_tracker_data(health_tracker_data_2020_3_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Append this new data with new schema to delta table

# COMMAND ----------

(processedDF.write
 .mode("append")
 .format("delta")
 .save(health_tracker + "processed"))

# COMMAND ----------

# MAGIC %md 
# MAGIC We get an error here due to the change in schema and delta format enforces schema

# COMMAND ----------

# MAGIC %md
# MAGIC So we need to have mergeschema option true

# COMMAND ----------

(processedDF.write
 .mode("append")
 .option("mergeSchema", True)
 .format("delta")
 .save(health_tracker + "processed"))

# COMMAND ----------

# MAGIC %md
# MAGIC Verify new data gone in by counting

# COMMAND ----------

health_tracker_processed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Due to compliance reasons we need to delete some of the devices data

# COMMAND ----------

processedDeltaTable.delete("p_device_id = 4")

# COMMAND ----------

# MAGIC %md
# MAGIC Turns out that data doesn't need to be deleted only masked, so we can use time travel to upsert the old data back into the current table

# COMMAND ----------

from pyspark.sql.functions import lit

upsertsDF = (
  spark.read
  .option("versionAsOf", 4)
  .format("delta")
  .load(health_tracker + "processed")
  .where("p_device_id = 4")
  .select("dte", "time", "device_type", "heartrate", lit(None).alias("name"), "p_device_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC We perform merge and have to declare schema again due to schema change in previous work

# COMMAND ----------

processedDeltaTable = DeltaTable.forPath(spark, health_tracker + "processed")

insert = {
  "dte" : "upserts.dte",
  "time" : "upserts.time",
  "device_type" : "upserts.device_type",
  "heartrate" : "upserts.heartrate",
  "name" : "upserts.name",
  "p_device_id" : "upserts.p_device_id"
}

(processedDeltaTable.alias("health_tracker")
 .merge(upsertsDF.alias("upserts"), update_match)
 .whenMatchedUpdate(set=update)
 .whenNotMatchedInsert(values=insert)
 .execute())

# COMMAND ----------

# MAGIC %md 
# MAGIC Count most recent version

# COMMAND ----------

health_tracker_processed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Query data to confirm data has been masked

# COMMAND ----------

display(health_tracker_processed.where("p_device_id = 4"))

# COMMAND ----------

# MAGIC %md
# MAGIC We note however using time travel we can still query the earlier non-compliant version of the data

# COMMAND ----------

display(
  spark.read
  .option("versionAsOf", 4)
  .format("delta")
  .load(health_tracker + "processed")
  .where("p_device_id = 4")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we attempt to vaccum the old files

# COMMAND ----------



# COMMAND ----------

processedDeltaTable.vacuum(0)

# COMMAND ----------

# MAGIC %md
# MAGIC Fails due to default retention check for files which needs to be overriden to work

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

processedDeltaTable.vacuum(0)

# COMMAND ----------

# MAGIC %md
# MAGIC now attempt to query older data post-vaccuum

# COMMAND ----------

display(
  spark.read
  .option("versionAsOf", 4)
  .format("delta")
  .load(health_tracker + "processed")
  .where("p_device_id = 4")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Fails as no longer exists