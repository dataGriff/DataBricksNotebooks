# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", "6da4208d-5acf-496e-96a2-2c8c115d6dfa")
spark.conf.set("fs.azure.account.oauth2.client.secret", "-6gQwtE_HK/0G_2e3P-Bx5Rd/v3/L0Rw")
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/cafe5856-f1cc-43b5-b041-4cfc98c266e7/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://stream@grifffruitvegsa.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------


from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("spark-avro-json-sample") \
    .config('spark.hadoop.avro.mapred.ignore.inputs.without.extension', 'false') \
    .getOrCreate()

#storage->avro
avroDf = spark.read.format("com.databricks.spark.avro").load("abfss://stream@grifffruitvegsa.dfs.core.windows.net/fruitvegsepehns2/fruit/2019/11/09")

#avro->json
jsonRdd = avroDf.select(avroDf.Body.cast("string")).rdd.map(lambda x: x[0])
data = spark.read.json(jsonRdd) # in real world it's better to specify a schema for the JSON

# COMMAND ----------

data.show()

# COMMAND ----------

display(data.select("standardLog.*"))






# COMMAND ----------

# Read all files from one day. All PartitionIds are included. 
file_location = "abfss://stream@grifffruitvegsa.dfs.core.windows.net/fruitvegsepehns2/fruit/2019/11/09"
file_type = "avro"

reader = spark.read.format(file_type).option("inferSchema", "true")
raw = reader.load(file_location)



# COMMAND ----------

# Decode Body into strings
from pyspark.sql import functions as f

decodeElements = f.udf(lambda a: a.decode('utf-8'))

jsons = raw.select(
    raw['EnqueuedTimeUtc'],
    decodeElements(raw['Body']).alias("Json")
)



# COMMAND ----------

# Parse Json data
from pyspark.sql.functions import from_json

json_schema = spark.read.json(jsons.rdd.map(lambda row: row.Json)).schema
data = jsons.withColumn('Parsed', from_json('Json', json_schema)).drop('Json')

# COMMAND ----------

data.show()