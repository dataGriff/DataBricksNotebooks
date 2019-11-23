# Databricks notebook source
# MAGIC %md 
# MAGIC # Azure Cosmos DB - Connect using Key Vault
# MAGIC 
# MAGIC In an earlier lesson ([Key Vault-backed Secret Scopes]($./08-Key-Vault-backed-secret-scopes)), you created a Key Vault-backed secret scope for Azure Databricks, and securely stored your Azure Cosmos DB key within.
# MAGIC 
# MAGIC In this lesson, you will use the Azure Cosmos DB secrets that are securely stored within the Key Vault-backed secret scope to connect to your Azure Cosmos DB instance. Next, you will use the DataFrame API to execute SQL queries and control the parallelism of reads through the JDBC interface.
# MAGIC 
# MAGIC If you are running in an Azure Databricks environment that is already pre-configured with the libraries you need, you can skip to the next cell. To use this notebook in your own Databricks environment, you will need to create libraries, using the [Create Library](https://docs.azuredatabricks.net/user-guide/libraries.html) interface in Azure Databricks. Follow the steps below to attach the `azure-eventhubs-spark` library to your cluster:
# MAGIC 
# MAGIC 1. In the left-hand navigation menu of your Databricks workspace, select **Workspace**, select the down chevron next to **Shared**, and then select **Create** and **Library**.
# MAGIC 
# MAGIC   ![Create Databricks Library](https://databricksdemostore.blob.core.windows.net/images/08/03/databricks-create-library.png 'Create Databricks Library')
# MAGIC 
# MAGIC 2. On the New Library screen, do the following:
# MAGIC 
# MAGIC   - **Source**: Select Maven Coordinate.
# MAGIC   - **Coordinate**: Enter "azure-cosmosdb-spark", and then select **com.microsoft.azure:azure-cosmosdb-spark_2.3.0_2.11:1.2.7** (or later version).
# MAGIC   - Select **Create Library**.
# MAGIC   
# MAGIC   ![Databricks new Maven library](https://databricksdemostore.blob.core.windows.net/images/04-MDW/cosmos-db-create-library.png 'Databricks new Maven library')
# MAGIC 
# MAGIC 3. On the library page that is displayed, check the **Attach** checkbox next to the name of your cluster to run the library on that cluster.
# MAGIC 
# MAGIC   ![Databricks attach library](https://databricksdemostore.blob.core.windows.net/images/04-MDW/cosmos-db-attach-library.png 'Databricks attach library')
# MAGIC 
# MAGIC Once complete, return to this notebook to continue with the lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Access Key Vault secrets and configure the Cosmos DB connector
# MAGIC 
# MAGIC In a previous lesson, you created two secrets in Key Vault for your Azure Cosmos DB instance: **cosmos-uri** and **cosmos-key**. These two values will be used to configure the Cosmos DB connector. Let's start out by retrieving those values and storing them in new variables.

# COMMAND ----------

uri = dbutils.secrets.get(scope = "key-vault-secrets", key = "cosmos-uri")
key = dbutils.secrets.get(scope = "key-vault-secrets", key = "cosmos-key")

# COMMAND ----------

# MAGIC %md 
# MAGIC In order to query Cosmos DB, you need to first create a configuration object that contains the configuration information. 
# MAGIC 
# MAGIC If you are curious, read the [configuration reference](https://github.com/Azure/azure-cosmosdb-spark/wiki/Configuration-references) for details on all of the options. 
# MAGIC 
# MAGIC The core items you need to provide are:
# MAGIC 
# MAGIC **Endpoint**: Your Cosmos DB url (i.e. https://youraccount.documents.azure.com:443/)
# MAGIC 
# MAGIC **Masterkey**: The primary or secondary key string for you Cosmos DB account
# MAGIC 
# MAGIC **Database**: The name of the database
# MAGIC 
# MAGIC **Collection**: The name of the collection that you wish to query

# COMMAND ----------

readConfig = {
"Endpoint" : uri, # from Key Vault
"Masterkey" : key, # from Key Vault
"Database" : "demos",
"Collection" : "documents",
"preferredRegions" : "West US", # or whichver region you added Cosmos DB to
"SamplingRatio" : "1.0",
"schema_samplesize" : "1000",
"query_pagesize" : "2147483647",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load parquet data into DataFrames and normalize
# MAGIC 
# MAGIC We want to normalize New York and Boston data sets and join them into a new DataFrame. This way, we can write the joined data into our Cosmos DB collection so that data can be accessed by external services and applications.  
# MAGIC 
# MAGIC Start by creating a DataFrame of the data from New York and then Boston:
# MAGIC 
# MAGIC | City          | Table Name              | Path to DBFS file
# MAGIC | ------------- | ----------------------- | -----------------
# MAGIC | **New York**  | `CrimeDataNewYork`      | `dbfs:/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet`
# MAGIC | **Boston**    | `CrimeDataBoston`       | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Boston-2016.parquet`
# MAGIC 
# MAGIC Then, normalize the data structure of each table so all the columns (and their values) line up with each other.
# MAGIC 
# MAGIC In the case of New York and Boston, here are the unique characteristics of each data set:
# MAGIC 
# MAGIC | | Offense-Column        | Offense-Value          | Reported-Column  | Reported-Data Type |
# MAGIC |-|-----------------------|------------------------|-----------------------------------|
# MAGIC | New York | `offenseDescription`  | starts with "murder" or "homicide" | `reportDate`     | `timestamp`    |
# MAGIC | Boston | `OFFENSE_CODE_GROUP`  | "Homicide"             | `MONTH`          | `integer`      |

# COMMAND ----------

from pyspark.sql.functions import lower, upper, month, col

crimeDataNewYorkDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet")
crimeDataBostonDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-Boston-2016.parquet")

homicidesNewYorkDF = (crimeDataNewYorkDF 
  .select(month(col("reportDate")).alias("month"), col("offenseDescription").alias("offense")) 
  .filter(lower(col("offenseDescription")).contains("murder") | lower(col("offenseDescription")).contains("homicide"))
)

homicidesBostonDF = (crimeDataBostonDF 
  .select("month", col("OFFENSE_CODE_GROUP").alias("offense")) 
  .filter(lower(col("OFFENSE_CODE_GROUP")).contains("homicide"))
)

# Join both DataFrames into a new one
homicidesBostonAndNewYorkDF = homicidesNewYorkDF.union(homicidesBostonDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the new DataFrame to get a sense of the data we will be writing to Cosmos DB.

# COMMAND ----------

display(homicidesBostonAndNewYorkDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write data to Cosmos DB

# COMMAND ----------

# MAGIC %md
# MAGIC You write data back to Cosmos DB by creating a DataFrame that contains the desired changes and then use the Write API to save the changes back. 
# MAGIC 
# MAGIC As Spark DataFrames are immutable, if you want to insert new rows to a collection in Cosmos DB, you will need to create a new DataFrame. If you want to both modify and insert, the connector supports the upsert operation.

# COMMAND ----------

# MAGIC %md
# MAGIC The Write API requires similar configuration to the Read API we used previously. Take note of the Upsert parameter we use here, which enables Upsert actions on the Cosmos DB side. 

# COMMAND ----------

writeConfig = {
"Endpoint" : uri,
"Masterkey" : key,
"Database" : "demos",
"Collection" : "documents",
"Upsert" : "true"
}

# COMMAND ----------

# MAGIC %md
# MAGIC By default, the Connector will not allow us to write to a collection that has documents already. In order to perform an upsert, we must specify that mode is set to `overwrite`.

# COMMAND ----------

homicidesBostonAndNewYorkDF.write.format("com.microsoft.azure.cosmosdb.spark").mode("overwrite").options(**writeConfig).save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge

# COMMAND ----------

# MAGIC %md
# MAGIC Read the data back out of Cosmos DB, using the Read Connector you configured earlier.

# COMMAND ----------

# TODO

# Add your code here, saving to a DataFrame named 'documents'

# COMMAND ----------

# MAGIC %md
# MAGIC Display the contents of the `documents` DataFrame to ensure the data was successfully retrieved.

# COMMAND ----------

display(documents)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference
# MAGIC 
# MAGIC If you wish to apply what you have learned so far to a new set of challenges, complete the optional exercise: [Exploratory Data Analysis]($./Optional/Exploratory-Data-Analysis)