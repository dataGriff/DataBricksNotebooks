# Databricks notebook source
# MAGIC %md
# MAGIC # Understanding the SQL DW Connector

# COMMAND ----------

# MAGIC %md
# MAGIC You can access Azure SQL Data Warehouse (SQL DW) from Azure Databricks using the SQL Data Warehouse connector (referred to as the SQL DW connector), a data source implementation for Apache Spark that uses Azure Blob storage, and PolyBase in SQL DW to transfer large volumes of data efficiently between a Databricks cluster and a SQL DW instance.
# MAGIC 
# MAGIC Both the Databricks cluster and the SQL DW instance access a common Blob storage container to exchange data between these two systems. In Databricks, Spark jobs are triggered by the SQL DW connector to read data from and write data to the Blob storage container. On the SQL DW side, data loading and unloading operations performed by PolyBase are triggered by the SQL DW connector through JDBC.
# MAGIC 
# MAGIC The SQL DW connector is more suited to ETL than to interactive queries, because each query execution can extract large amounts of data to Blob storage. If you plan to perform several queries against the same SQL DW table, we recommend that you save the extracted data in a format such as Parquet.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Data Warehouse Pre-Requisites

# COMMAND ----------

# MAGIC %md
# MAGIC There are two pre-requisites for connecting Azure Databricks with SQL Data Warehouse that apply to the SQL Data Warehouse:
# MAGIC 1. You need to [create a database master key](https://docs.microsoft.com/en-us/sql/relational-databases/security/encryption/create-a-database-master-key) for the Azure SQL Data Warehouse. 
# MAGIC 
# MAGIC     **The key is encrypted using the password.**
# MAGIC 
# MAGIC     USE [databricks-sqldw];  
# MAGIC     GO  
# MAGIC     CREATE MASTER KEY ENCRYPTION BY PASSWORD = '980AbctotheCloud427leet';  
# MAGIC     GO
# MAGIC 
# MAGIC 2. You need to ensure that the [Firewall](https://docs.microsoft.com/en-us/azure/sql-database/sql-database-firewall-configure#manage-firewall-rules-using-the-azure-portal) on the Azure SQL Server that contains your SQL Data Warehouse is configured to allow Azure services to connect (e.g., Allow access to Azure services is set to On).
# MAGIC 
# MAGIC You should have already completed the above requirements in earlier steps of the lab.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Azure Storage Pre-Requisites

# COMMAND ----------

# MAGIC %md
# MAGIC Azure Storage blobs are used as the intermediary for the exchange of data between Azure Databricks and Azure SQL Data Warehouse. As a result of this, you will need:
# MAGIC 1. To create a general purpose Azure Storage account v1
# MAGIC 2. Acquire the Account Name and Account Key for that Storage Account 
# MAGIC 3. Create a container that will be used to store data used during the exchange, for example "dwtemp" (this must exists before you run an queries against SQL DW)
# MAGIC 
# MAGIC You should have already performed these steps in the setup steps for this lab. You will need to refer to the Account Name and Account Key you saved during setup for the section below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enabling access for a notebook session

# COMMAND ----------

# MAGIC %md
# MAGIC You can enable access for the lifetime of your notebook session to SQL Data Warehouse by executing the cell below. Be sure to replace the **"name-of-your-storage-account"** and **"your-storage-key"** values with your own before executing.

# COMMAND ----------

storage_account_name = "name-of-your-storage-account"
storage_account_key = "your-storage-key"
storage_container_name = "dwtemp"

temp_dir_url = "wasbs://{}@{}.blob.core.windows.net/".format(storage_container_name, storage_account_name)

spark_config_key = "fs.azure.account.key.{}.blob.core.windows.net".format(storage_account_name)
spark_config_value = storage_account_key

spark.conf.set(spark_config_key, spark_config_value)

# COMMAND ----------

# MAGIC %md
# MAGIC You will need the JDBC connection string for your Azure SQL Data Warehouse. You should copy this value exactly as it appears in the Azure Portal. 
# MAGIC 
# MAGIC Please replace the missing **`servername`**, **`databasename`**, and **`your-password`** values in the command below:

# COMMAND ----------

servername = "servername"
databasename = "databasename"
password = "your-password"

sql_dw_connection_string = "jdbc:sqlserver://{}.database.windows.net:1433;database={};user=dwlab@{};password={};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;".format(servername, databasename, servername, password)

# COMMAND ----------

print(sql_dw_connection_string)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accessing data from SQL Data Warehouse

# COMMAND ----------

# MAGIC %md
# MAGIC You can load data into your Azure Databricks environment by issuing a SQL query against the SQL Data Warehouse, as follows:

# COMMAND ----------

query = "SELECT * FROM EmployeeBasic"

df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", sql_dw_connection_string) \
  .option("tempdir", temp_dir_url) \
  .option("forward_spark_azure_storage_credentials", "true") \
  .option("query", query) \
  .load()
  
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Once you have the data in a DataFrame, you can query it just as you would any other DataFrame. This includes visualizing it as follows:

# COMMAND ----------

summary = df.select("EmployeeName")
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing new data to SQL Data Warehouse

# COMMAND ----------

# MAGIC %md
# MAGIC You can also go in the opposite direction, saving the contents of an existing DataFrame out to a new table in Azure SQL Data Warehouse.
# MAGIC 
# MAGIC First, let's load the `weblogs` Databricks table contents into a new DataFrame. As you recall, we created this table in the previous notebook.

# COMMAND ----------

from pyspark.sql.functions import *

purchasedWeblogDF = spark.sql("select * from weblogs where Action == 'Purchased'")

# COMMAND ----------

# MAGIC %md
# MAGIC Execute the cell below to add one additional column containing the transaction date/time. This column will be derived from the `TransactionDate` field by casting it to date type.

# COMMAND ----------

purchasedProductsDF = purchasedWeblogDF.select('*',to_date(unix_timestamp(purchasedWeblogDF.TransactionDate,'MM/dd/yyyy HH:mm:ss').cast("timestamp")).alias("TransactionDateTime"))

# COMMAND ----------

# MAGIC %md
# MAGIC Execute the cell below to filter out products purchased in month of May (05).

# COMMAND ----------

filteredProductsDF = purchasedProductsDF.where(month(purchasedProductsDF.TransactionDateTime)==5)

# COMMAND ----------

# MAGIC %md
# MAGIC Execute the cell below to select the required columns.

# COMMAND ----------

finalProductsDF = filteredProductsDF.select("SessionId","UserId","ProductId","Quantity","TransactionDateTime")
display(finalProductsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have our desired DataFrame, let's create a new table in the Azure SQL Data Warehouse database and populate it with the contents of the DataFrame.

# COMMAND ----------

new_table_name = "PurchasedProducts"

finalProductsDF.write \
  .format("com.databricks.spark.sqldw") \
  .option("url", sql_dw_connection_string) \
  .option("forward_spark_azure_storage_credentials", "true") \
  .option("dbtable", new_table_name) \
  .option("tempdir", temp_dir_url) \
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC To verify that we have indeed created a new table in the SQL DW database, write a query to select the data from the new table:

# COMMAND ----------

query = "SELECT * FROM PurchasedProducts"

df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", sql_dw_connection_string) \
  .option("tempdir", temp_dir_url) \
  .option("forward_spark_azure_storage_credentials", "true") \
  .option("query", query) \
  .load()
  
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC You may also open Azure Data Studio and refresh the Tables list to see the new PurchasedProducts table.
# MAGIC 
# MAGIC Run the following query in a new query window:
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT TOP (1000) [SessionId]
# MAGIC       ,[UserId]
# MAGIC       ,[ProductId]
# MAGIC       ,[Quantity]
# MAGIC       ,[TransactionDateTime]
# MAGIC   FROM [dbo].[PurchasedProducts]
# MAGIC ```
# MAGIC 
# MAGIC You should have an output similar to the following:
# MAGIC 
# MAGIC <img src="https://databricksdemostore.blob.core.windows.net/images/02-SQL-DW/sql-dw-new-table.png" style="border: 1px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>