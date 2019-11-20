# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Data Ingestion via ADF - Data Transformations Notebook

# COMMAND ----------

# MAGIC %md 
# MAGIC This notebook performs the data munging and transformation tasks against the files ingested via ADF in the Data Ingestion notebook for this module. This notebook will be executed by a call from Azure Data Factory (ADF), and will automate the process of data munging and table creation for the data used within this module.
# MAGIC 
# MAGIC The following table will be created by this notebook:
# MAGIC 
# MAGIC - homicides_2016

# COMMAND ----------

# Create input widgets, which will accept parameters passed in via the ADF Databricks Notebook activity
dbutils.widgets.text("accountName", "", "Account Name")
dbutils.widgets.text("accountKey", "", "Account Key")
dbutils.widgets.text("containerName", "", "Container Name")

# COMMAND ----------

# Assign variables to the passed in values of the widgets
accountName = dbutils.widgets.get("accountName")
accountKey = dbutils.widgets.get("accountKey")
containerName = dbutils.widgets.get("containerName")

# Create connection string to use for accessing files in the storage account
connectionString = "wasbs://%(containerName)s@%(accountName)s.blob.core.windows.net/03.02" % locals()

# COMMAND ----------

# Create connection to the Azure Storage account
spark.conf.set("fs.azure.account.key." + accountName + ".blob.core.windows.net", accountKey)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create DataFrame for each city's crime data

# COMMAND ----------

bostonDf = spark.read.parquet("%(connectionString)s/Crime-Data-Boston-2016.parquet" % locals())
chicagoDf = spark.read.parquet("%(connectionString)s/Crime-Data-Chicago-2016.parquet" % locals())
dallasDf = spark.read.parquet("%(connectionString)s/Crime-Data-Dallas-2016.parquet" % locals())
losAngelesDf = spark.read.parquet("%(connectionString)s/Crime-Data-Los-Angeles-2016.parquet" % locals())
newOrleansDf = spark.read.parquet("%(connectionString)s/Crime-Data-New-Orleans-2016.parquet" % locals())
newYorkDf = spark.read.parquet("%(connectionString)s/Crime-Data-New-York-2016.parquet" % locals())
phillyDf = spark.read.parquet("%(connectionString)s/Crime-Data-Philadelphia-2016.parquet" % locals())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create normalized DataFrames for each city
# MAGIC 
# MAGIC For the upcoming aggregation, you need to alter the data sets to include a `month` column which can be computed from the various "Incident Date" columns using the `month()` function. Boston already has this column.
# MAGIC 
# MAGIC In this example, we use several functions in the `pyspark.sql.functions` library, and need to import:
# MAGIC 
# MAGIC * `col()` to return a column from a DataFrame, based on the given column name.
# MAGIC * `contains(mySubstr)` to indicate a string contains substring `mySubstr`.
# MAGIC * `lit()` to create a column from a literal value.
# MAGIC * `lower()` to convert text to lowercase.
# MAGIC * `month()` to extract the month from `reportDate` timestamp data type.
# MAGIC * `unix_timestamp()` to convert the Dallas date field into a timestamp format, so the month can be extracted using the `month()` function.
# MAGIC 
# MAGIC Also, note:
# MAGIC 
# MAGIC * We use  `|`  to indicate a logical `or` of two conditions in the `filter` method.
# MAGIC * We use `lit()` to create a new column in each DataFrame containing the name of the city for which the data is derived.

# COMMAND ----------

# Import required libraries
import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, lower, month, unix_timestamp, upper

# COMMAND ----------

homicidesBostonDf = (bostonDf.withColumn("city", lit("Boston"))
  .select("month", col("OFFENSE_CODE_GROUP").alias("offense"), col("city"))
  .filter(lower(col("OFFENSE_CODE_GROUP")).contains("homicide"))
)

# COMMAND ----------

homicidesChicagoDf = (chicagoDf.withColumn("city", lit("Chicago"))
  .select(month(col("date")).alias("month"), col("primaryType").alias("offense"), col("city"))
  .filter(lower(col("primaryType")).contains("homicide"))
)

# COMMAND ----------

homicidesDallasDf = (dallasDf.withColumn("city", lit("Dallas"))
   .select(month(unix_timestamp(col("callDateTime"),"M/d/yyyy h:mm:ss a").cast("timestamp")).alias("month"), col("typeOfIncident").alias("offense"), col("city"))
   .filter(lower(col("typeOfIncident")).contains("murder") | lower(col("typeOfIncident")).contains("manslaughter"))
)

# COMMAND ----------

homicidesLosAngelesDf = (losAngelesDf.withColumn("city", lit("Los Angeles"))
   .select(month(col("dateOccurred")).alias("month"), col("crimeCodeDescription").alias("offense"), col("city"))
   .filter(lower(col("crimeCodeDescription")).contains("homicide") | lower(col("crimeCodeDescription")).contains("manslaughter"))
)

# COMMAND ----------

homicidesNewOrleansDf = (newOrleansDf.withColumn("city", lit("New Orleans"))
   .select(month(col("Occurred_Date_Time")).alias("month"), col("Incident_Description").alias("offense"), col("city"))
   .filter(lower(col("Incident_Description")).contains("homicide") | lower(col("Incident_Description")).contains("murder"))
)

# COMMAND ----------

homicidesNewYorkDf = (newYorkDf.withColumn("city", lit("New York"))
  .select(month(col("reportDate")).alias("month"), col("offenseDescription").alias("offense"), col("city")) 
  .filter(lower(col("offenseDescription")).contains("murder") | lower(col("offenseDescription")).contains("homicide"))
)

# COMMAND ----------

homicidesPhillyDf = (phillyDf.withColumn("city", lit("Philadelphia"))
   .select(month(col("dispatch_date")).alias("month"), col("text_general_code").alias("offense"), col("city"))
   .filter(lower(col("text_general_code")).contains("homicide"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a single DataFrame
# MAGIC With the normalized homicide data for each city, combine the two by taking their union.

# COMMAND ----------

homicidesDf = homicidesNewYorkDf.union(homicidesBostonDf).union(homicidesChicagoDf).union(homicidesDallasDf).union(homicidesLosAngelesDf).union(homicidesNewOrleansDf).union(homicidesPhillyDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export the prepared data to a persistent table

# COMMAND ----------

homicidesDf.write.mode("overwrite").saveAsTable("homicides_2016")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Return OK status

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "message": "Cleaned data and created persistent table",
  "tables": ["homicides_2016"]
}))