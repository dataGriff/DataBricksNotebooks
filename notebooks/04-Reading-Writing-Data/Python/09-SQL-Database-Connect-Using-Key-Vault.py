# Databricks notebook source
# MAGIC %md 
# MAGIC # SQL Database - Connect using Key Vault
# MAGIC 
# MAGIC In the previous lession ([Key Vault-backed Secret Scopes]($./08-Key-Vault-backed-secret-scopes)), you created a Key Vault-backed secret scope for Azure Databricks, and securely stored your Azure SQL username and password within.
# MAGIC 
# MAGIC In this lesson, you will use the Azure SQL Database secrets that are securely stored within the Key Vault-backed secret scope to connect to your Azure SQL Database instance, using the JDBC drivers that come with Databricks Runtime version 3.4 and above. Next, you will use the DataFrame API to execute SQL queries and control the parallelism of reads through the JDBC interface.
# MAGIC 
# MAGIC You will be using a new data set, as the customer has decided to make their e-commerce SQL data available for querying and processing within Azure Databricks. You will prove that you can successfully read data from and write data to the database using the JDBC driver.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Access Key Vault secrets and configure JDBC connection
# MAGIC 
# MAGIC In the previous lesson, you created two secrets in Key Vault for your Azure SQL Database instance: **sql-username** and **sql-password**. You should have also made note of the Azure SQL Server host name and Database name. If you do not have this information, take a moment to retrieve those details from the Azure portal.

# COMMAND ----------

# MAGIC %scala
# MAGIC val jdbcUsername = dbutils.secrets.get(scope = "key-vault-secrets", key = "sql-username")
# MAGIC val jdbcPassword = dbutils.secrets.get(scope = "key-vault-secrets", key = "sql-password")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Notice that the values of `jdbcUsername` and `jdbcPassword` when printed out are `[REDACTED]`. This is to prevent your secrets from being exposed.
# MAGIC 
# MAGIC The next step is to ensure the JDBC driver is available.

# COMMAND ----------

# MAGIC %scala
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

# MAGIC %md
# MAGIC Now create a connection to the Azure SQL Database using the username and password from Key Vault, as well as the SQL Server host name and database name.
# MAGIC 
# MAGIC > The **host name** value will be in the following format: `<YOUR_SERVER_NAME>.databrickssqlserver.database.windows.net`.

# COMMAND ----------

# Create input widgets to store the host name and database values. This will allow us to access those same values from cells that use different languages.
# Execute this cell to display the widgets on top of the page, then fill the information before continuing to the next cell.
dbutils.widgets.text("hostName", "", "SQL Server Host Name")
dbutils.widgets.text("database", "", "Database Name")

# COMMAND ----------

# MAGIC %scala
# MAGIC val jdbcHostname = dbutils.widgets.get("hostName")
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcDatabase = dbutils.widgets.get("database")
# MAGIC 
# MAGIC // Create the JDBC URL without passing in the user and password parameters.
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC 
# MAGIC // Create a Properties() object to hold the parameters.
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC 
# MAGIC connectionProperties.put("user", s"${jdbcUsername}")
# MAGIC connectionProperties.put("password", s"${jdbcPassword}")

# COMMAND ----------

# MAGIC %md
# MAGIC Make sure you can connect to the Azure SQL Database, using the JDBC driver.

# COMMAND ----------

# MAGIC %scala
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC connectionProperties.setProperty("Driver", driverClass)

# COMMAND ----------

# MAGIC %md
# MAGIC If there are no errors, then the connection was successful.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read data from JDBC
# MAGIC 
# MAGIC To start, we'll use a single JDBC connection to pull an Azure SQL Database table into the Spark environment. Your queries will be executed through the DataFrame API, providing a consistent experience whether you are querying flat files, Databricks tables, Azure Cosmos DB, SQL Server, or any other of the number of data sources you can access from Databricks.

# COMMAND ----------

# MAGIC %scala
# MAGIC val product_table = spark.read.jdbc(jdbcUrl, "SalesLT.Product", connectionProperties)

# COMMAND ----------

# MAGIC %md
# MAGIC Spark automatically reads the schema from the database table and maps its types back to Spark SQL types. You can see the schema using the `printSchema` command:

# COMMAND ----------

# MAGIC %scala
# MAGIC product_table.printSchema

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have our DataFrame, we can run queries against the JDBC table. Below, we are calculating the average list price by product category.

# COMMAND ----------

# MAGIC %scala
# MAGIC display(product_table.select("ProductCategoryID", "ListPrice")
# MAGIC         .groupBy("ProductCategoryID")
# MAGIC         .avg("ListPrice"))

# COMMAND ----------

# MAGIC %md
# MAGIC It is possible to save the DataFrame out to a temporary view. This opens opportunities, such as querying using the SQL syntax, as you will see a few cells below.

# COMMAND ----------

# MAGIC %scala
# MAGIC product_table.createOrReplaceTempView("Product")
# MAGIC display(spark.sql("SELECT * FROM Product"))

# COMMAND ----------

# MAGIC %md
# MAGIC Use the `%sql` magic to change the language of a cell to SQL. In the cell below, execute a SQL query that performs the same average `ListPrice` and product category grouping that was done using the DataFrame earlier.

# COMMAND ----------

# MAGIC %sql
# MAGIC select ProductCategoryID, AVG(ListPrice) from Product
# MAGIC Group By ProductCategoryID

# COMMAND ----------

# MAGIC %md
# MAGIC You can also use the JDBC driver using Python. The cell below establishes a JDBC connection to the Azure SQL Database just as was done earlier using Scala.
# MAGIC 
# MAGIC Notice that you must define new parameters for the host name, database, username, and password. This is because those variables were defined using a different language earlier, which are inaccessible by a different language engine.

# COMMAND ----------

jdbcHostname = dbutils.widgets.get("hostName")
jdbcDatabase = dbutils.widgets.get("database")
jdbcPort = 1433

# Retrieve the database username and password from Key Vault
jdbcUsername = dbutils.secrets.get(scope = "key-vault-secrets", key = "sql-username")
jdbcPassword = dbutils.secrets.get(scope = "key-vault-secrets", key = "sql-password")

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# MAGIC %md
# MAGIC Now that the JDBC connection has been created in Python, let's query the JDBC connections across multiple workers. You do this by setting the `numPartitions` property. This determines how many connections used to push data through the JDBC API.

# COMMAND ----------

df = spark.read.jdbc(url=jdbcUrl, table='SalesLT.SalesOrderDetail', properties=connectionProperties, column='SalesOrderDetailID', lowerBound=1, upperBound=100000, numPartitions=100)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge
# MAGIC 
# MAGIC Write new records to the `SalesLT.ProductModel` Azure SQL Database table.
# MAGIC 
# MAGIC The first step is to return a count of records in the `SalesLT.ProductModel` table. We've provided the code to do this as a pushdown query (where you push down an entire query to the database and return just the result) below.

# COMMAND ----------

productmodel_count_query = "(select COUNT(*) count from SalesLT.ProductModel) count"
productmodel_count = spark.read.jdbc(url=jdbcUrl, table=productmodel_count_query, properties=connectionProperties)
display(productmodel_count)

# COMMAND ----------

# MAGIC %md
# MAGIC The second step is to define a schema that matches the data types in SQL. Since the primary key is an automatically generated identity column, you must exclude it from your schema since you will not be setting the value.
# MAGIC 
# MAGIC **Note:** You can get the schema with data types by retrieving the first row into a new DataFrame and printing the schema:
# MAGIC 
# MAGIC ```python
# MAGIC productmodel_df = spark.read.jdbc(url=jdbcUrl, table="SalesLT.ProductModel", properties=connectionProperties).limit(1)
# MAGIC productmodel_df.printSchema
# MAGIC ```

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
  StructField("Name", StringType(), True),
  StructField("CatalogDescription", StringType(), True),
  StructField("rowguid", StringType(), True),
  StructField("ModifiedDate", TimestampType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC Now, write new rows to the table, applying the schema from the `productModel_df` DataFrame.

# COMMAND ----------

# Import libraries to generate UUIDs (GUIDs), get the current date/time, set the save mode, and to use the Row object
import uuid
import datetime
from pyspark.sql import Row
newRows = [
  Row("Biking Shorts","They make you ride faster and look awesome doing it!", str(uuid.uuid4()), datetime.datetime.now()),
  Row("Biking Shirt","Glide through the atmosphere like Phoenix rising from the ashes", str(uuid.uuid4()), datetime.datetime.now())
]
parallelizeRows = spark.sparkContext.parallelize(newRows)
new_df = spark.createDataFrame(parallelizeRows, schema)

# TODO

# Add code here to write to the SalesLT.ProductModel table

# COMMAND ----------

# MAGIC %md
# MAGIC Do a final count on the `SalesLT.ProductModel` table to make sure the two new rows were inserted. You can also use the Query Editor in the Azure SQL Database portal interface to view your new rows there.

# COMMAND ----------

# TODO

# Get a count of rows once more. The value should be 130

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Cosmos DB - Connect using Key Vault]($./10-Cosmos-DB-Connect-Using-Key-Vault)