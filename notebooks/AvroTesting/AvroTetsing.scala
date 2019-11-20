// Databricks notebook source
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", "6da4208d-5acf-496e-96a2-2c8c115d6dfa")
spark.conf.set("fs.azure.account.oauth2.client.secret", "-6gQwtE_HK/0G_2e3P-Bx5Rd/v3/L0Rw")
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/cafe5856-f1cc-43b5-b041-4cfc98c266e7/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://stream@grifffruitvegsa.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

// COMMAND ----------

import org.apache.avro.Schema

// COMMAND ----------

val df = spark.read.format("avro").load("abfss://stream@grifffruitvegsa.dfs.core.windows.net/fruitvegsepehns2/fruit/2019/11/02/09")
display(df)



// COMMAND ----------

