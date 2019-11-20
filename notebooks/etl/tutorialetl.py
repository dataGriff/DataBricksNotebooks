# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", "6da4208d-5acf-496e-96a2-2c8c115d6dfa")
spark.conf.set("fs.azure.account.oauth2.client.secret", "-6gQwtE_HK/0G_2e3P-Bx5Rd/v3/L0Rw")
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/cafe5856-f1cc-43b5-b041-4cfc98c266e7/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://test2@grifffruitvegsa.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

# MAGIC %sh wget -P /tmp https://raw.githubusercontent.com/Azure/usql/master/Examples/Samples/Data/json/radiowebsite/small_radio_json.json

# COMMAND ----------

dbutils.fs.cp("file:///tmp/small_radio_json.json", "abfss://test2@grifffruitvegsa.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read.json("abfss://test2@grifffruitvegsa.dfs.core.windows.net/small_radio_json.json")

# COMMAND ----------

# MAGIC %scala
# MAGIC df.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC val specificColumnsDf = df.select("firstname", "lastname", "gender", "location", "level")
# MAGIC specificColumnsDf.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC val renamedColumnsDF = specificColumnsDf.withColumnRenamed("level", "subscription_type")
# MAGIC renamedColumnsDF.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC df.write.parquet("abfss://test2@grifffruitvegsa.dfs.core.windows.net/radio.parquet")

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read.parquet("abfss://test2@grifffruitvegsa.dfs.core.windows.net/radio.parquet")

# COMMAND ----------

# MAGIC %scala
# MAGIC df.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC val specificColumnsDf = df.select("firstname", "lastname", "gender", "location", "level")
# MAGIC specificColumnsDf.write.parquet("abfss://test2@grifffruitvegsa.dfs.core.windows.net/staging")

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read.parquet("abfss://test2@grifffruitvegsa.dfs.core.windows.net/staging")