// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img src="https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
// MAGIC 
// MAGIC # Connecting to Azure Blob Storage
// MAGIC 
// MAGIC Apache Spark&trade; and Databricks&reg; allow you to connect to virtually any data store including Azure Blob Storage.
// MAGIC ## In this lesson you:
// MAGIC * Mount and access data in Azure Blob Storage
// MAGIC * Define options when reading from Azure Blob Storage
// MAGIC 
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers
// MAGIC * Additional Audiences: Data Scientists and Data Pipeline Engineers
// MAGIC 
// MAGIC ## Prerequisites
// MAGIC * Web browser: Chrome
// MAGIC * A cluster configured with **8 cores** and **DBR 6.2**

// COMMAND ----------

// MAGIC %md
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Setup
// MAGIC 
// MAGIC For each lesson to execute correctly, please make sure to run the **`Classroom-Setup`** cell at the<br/>
// MAGIC start of each lesson (see the next cell) and the **`Classroom-Cleanup`** cell at the end of each lesson.

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/8qe9xs3k7u?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/8qe9xs3k7u?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Spark as a Connector
// MAGIC 
// MAGIC Spark quickly rose to popularity as a replacement for the [Apache Hadoop&trade;](http://hadoop.apache.org/) MapReduce paradigm in a large part because it easily connected to a number of different data sources.  Most important among these data sources was the Hadoop Distributed File System (HDFS).  Now, Spark engineers connect to a wide variety of data sources including:  
// MAGIC <br>
// MAGIC * Traditional databases like Postgres, SQL Server, and MySQL
// MAGIC * Message brokers like <a href="https://kafka.apache.org/" target="_blank">Apache Kafka</a> and <a href="https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-about">Azure Event Hubs</a>
// MAGIC * Distributed databases like Cassandra and Redshift
// MAGIC * Data warehouses like Hive and Cosmos DB
// MAGIC * File types like CSV, Parquet, and Avro
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/open-source-ecosystem_2.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### DBFS Mounts and Azure Blob Storage
// MAGIC 
// MAGIC Azure Blob Storage is the backbone of Databricks workflows.  Azure Blob Storage offers data storage that easily scales to the demands of most data applications and, by colocating data with Spark clusters, Databricks quickly reads from and writes to Azure Blob Storage in a distributed manner.
// MAGIC 
// MAGIC The Databricks File System (DBFS), is a layer over Azure Blob Storage that allows you to mount Blob containers, making them available to other users in your workspace and persisting the data after a cluster is shut down.
// MAGIC 
// MAGIC In our road map for ETL, this is the <b>Extract and Validate </b> step:
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/ETL-Process-1.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

// COMMAND ----------

// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/sls8z8pw8n?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/sls8z8pw8n?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC Define your Azure Blob credentials.  You need the following elements:<br><br>
// MAGIC 
// MAGIC * Storage account name
// MAGIC * Container name
// MAGIC * Mount point (how the mount will appear in DBFS)
// MAGIC * Shared Access Signature (SAS) key
// MAGIC 
// MAGIC Below these elements are defined, including a read-only SAS key.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> For more information on SAS keys, <a href="https://docs.microsoft.com/en-us/azure/storage/common/storage-dotnet-shared-access-signature-part-1" target="_blank"> see the Azure documentation.</a><br>
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> SAS keys are normally provided as a SAS URI. Of this URI, focus on everything from the `?` on, including the `?`. The following cell provides an example of this.

// COMMAND ----------

val storageAccount = "dbtraineastus2"
val container = "training"
val sasKey = "?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:32:30Z&st=2018-04-18T22:32:30Z&spr=https&sig=BB%2FQzc0XHAH%2FarDQhKcpu49feb7llv3ZjnfViuI9IWo%3D"

// COMMAND ----------

// MAGIC %md
// MAGIC In addition to the sourcing information above, we need to define a target location.
// MAGIC 
// MAGIC So that no two students produce the exact same mount, we are going to be a little more creative with this one.

// COMMAND ----------

val mountPoint = s"/mnt/etlp1a-$username-ss"

// COMMAND ----------

// MAGIC %md
// MAGIC In case you mounted this bucket earlier, you might need to unmount it.

// COMMAND ----------

try {
  dbutils.fs.unmount(s"$mountPoint") // Use this to unmount as needed
} catch {
  case ioe: java.rmi.RemoteException => println(s"$mountPoint already unmounted")
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Define two strings populated with the storage account and container information.  This will be passed to the `mount` function.

// COMMAND ----------

val sourceString = s"wasbs://$container@$storageAccount.blob.core.windows.net/"
val confKey = s"fs.azure.sas.$container.$storageAccount.blob.core.windows.net"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC Now, mount the container <a href="https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html#mount-azure-blob-storage-containers-with-dbfs" target="_blank"> using the template provided in the docs.</a>
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The code below includes error handling logic to handle the case where the mount is already mounted.

// COMMAND ----------

try {
  dbutils.fs.mount(
    source = sourceString,
    mountPoint = mountPoint,
    extraConfigs = Map(confKey -> sasKey)
  )
}
catch {
  case e: Exception =>
    println(s"*** ERROR: Unable to mount $mountPoint. Run previous cells to unmount first")
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Next, explore the mount using `%fs ls` and the name of the mount.

// COMMAND ----------

// MAGIC %fs ls /mnt/training

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC In practice, always secure your credentials.  Do this by either maintaining a single notebook with restricted permissions that holds SAS keys, or delete the cells or notebooks that expose the keys. **After a cell used to mount a container is run, access this mount in any notebook, any cluster, and share the mount between colleagues.**
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See <a href="https://docs.azuredatabricks.net/user-guide/secrets/index.html" target="_blank">secret management to securely store and reference your credentials in notebooks and jobs.</a> 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Adding Options
// MAGIC 
// MAGIC When you import that data into a cluster, you can add options based on the specific characteristics of the data.

// COMMAND ----------

// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/6pckay2lii?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/6pckay2lii?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC Display the first few lines of `Chicago-Crimes-2018.csv` using `%fs head`.

// COMMAND ----------

// MAGIC %fs head /mnt/training/Chicago-Crimes-2018.csv

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC **`option`** is a method of **`DataFrameReader`**. 
// MAGIC 
// MAGIC Options are key/value pairs and must be specified before calling **`.csv()`**.
// MAGIC 
// MAGIC This is a tab-delimited file, as seen in the previous cell. Specify the **`"delimiter"`** option in the import statement.  
// MAGIC 
// MAGIC :NOTE: Find a [full list of parameters here.](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader@csv%28paths:String*%29:org.apache.spark.sql.DataFrame)

// COMMAND ----------

display(spark.read
  .option("delimiter", "\t")
  .csv("/mnt/training/Chicago-Crimes-2018.csv")
)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Spark doesn't read the header by default, as demonstrated by the column names of `_c0`, `_c1`, etc. Notice the column names are present in the first row of the DataFrame. 
// MAGIC 
// MAGIC Fix this by setting the `"header"` option to `true`.

// COMMAND ----------

display(spark.read
  .option("delimiter", "\t")
  .option("header", true)
  .csv("/mnt/training/Chicago-Crimes-2018.csv")
)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Spark didn't infer the schema or read the timestamp format.  Since this file uses an atypical timestamp, Spark inferred the timestamp as a string. Change that by adding the option `"timestampFormat"` and pass it the format used in this file.  
// MAGIC 
// MAGIC Set `"inferSchema"` to `true`, which triggers Spark to make an extra pass over the data to infer the schema.

// COMMAND ----------

val crimeDF = spark.read
  .option("delimiter", "\t")
  .option("header", true)
  .option("timestampFormat", "mm/dd/yyyy hh:mm:ss a")
  .option("inferSchema", true)
  .csv("/mnt/training/Chicago-Crimes-2018.csv")

display(crimeDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## The Design Pattern
// MAGIC 
// MAGIC Other connections work in much the same way, whether your data sits in Cassandra, Cosmos DB, Redshift, or another common data store.  The general pattern is always:  
// MAGIC <br>
// MAGIC 1. Define the connection point
// MAGIC 2. Define connection parameters such as access credentials
// MAGIC 3. Add necessary options
// MAGIC 
// MAGIC After adhering to this, read data using `spark.read.options(<option key>, <option value>).<connection_type>(<endpoint>)`.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 1: Read Wikipedia Data
// MAGIC 
// MAGIC Read Wikipedia data from Azure Blob Storage, accounting for its delimiter and header.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Get a Sense for the Data
// MAGIC 
// MAGIC Take a look at the head of the data, located at `/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv`.

// COMMAND ----------

// ANSWER
println(dbutils.fs.head("/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv", 100)) 
// this evaluates to the thing as %fs head /mnt/training/wikipedia/pageviews/pageviews_by_second.tsv

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2: Import the Raw Data
// MAGIC 
// MAGIC Import the data **without any options** and save it to `wikiDF`. Display the result.

// COMMAND ----------

// ANSWER
val path = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

val wikiDF = spark.read.csv(path)
display(wikiDF)

// COMMAND ----------

// TEST - Run this cell to test your solution

dbTest("ET1-S-03-01-01", 7200001, wikiDF.count)
dbTest("ET1-S-03-01-02", "_c0", wikiDF.columns(0))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 3: Import the Data with Options
// MAGIC 
// MAGIC Import the data with options and save it to `wikiWithOptionsDF`.  Display the result.  Your import statement should account for:<br><br>  
// MAGIC 
// MAGIC  - The header
// MAGIC  - The delimiter

// COMMAND ----------

// ANSWER
val path = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

val wikiWithOptionsDF = spark.read
  .option("header", true)
  .option("delimiter", "\t")
  .csv(path)

display(wikiWithOptionsDF)

// COMMAND ----------

// TEST - Run this cell to test your solution
val cols = wikiWithOptionsDF.columns
 
dbTest("ET1-S-03-02-01", 7200000, wikiWithOptionsDF.count)
 
dbTest("ET1-S-03-02-02", true, cols.contains("timestamp"))
dbTest("ET1-S-03-02-03", true, cols.contains("site"))
dbTest("ET1-S-03-02-04", true, cols.contains("requests"))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Review
// MAGIC 
// MAGIC **Question:** What accounts for Spark's quick rise in popularity as an ETL tool?  
// MAGIC **Answer:** Spark easily accesses data virtually anywhere it lives, and the scalable framework lowers the difficulties in building connectors to access data.  Spark offers a unified API for connecting to data making reads from a CSV file, JSON data, or a database, to provide a few examples, nearly identical.  This allows developers to focus on writing their code rather than writing connectors.
// MAGIC 
// MAGIC **Question:** What is DBFS and why is it important?  
// MAGIC **Answer:** The Databricks File System (DBFS) allows access to scalable, fast, and distributed storage backed by S3 or the Azure Blob Store.
// MAGIC 
// MAGIC **Question:** How do you connect your Spark cluster to the Azure Blob?  
// MAGIC **Answer:** By mounting it. Mounts require Azure credentials such as SAS keys and give access to a virtually infinite store for your data. One other option is to define your keys in a single notebook that only you have permission to access. Click the arrow next to a notebook in the Workspace tab to define access permissions.
// MAGIC 
// MAGIC **Question:** How do you specify parameters when reading data?  
// MAGIC **Answer:** Using `.option()` during your read allows you to pass key/value pairs specifying aspects of your read.  For instance, options for reading CSV data include `header`, `delimiter`, and `inferSchema`.
// MAGIC 
// MAGIC **Question:** What is the general design pattern for connecting to your data?  
// MAGIC **Answer:** The general design pattern is as follows:
// MAGIC 0. Define the connection point
// MAGIC 0. Define connection parameters such as access credentials
// MAGIC 0. Add necessary options such as for headers or parallelization

// COMMAND ----------

// MAGIC %md
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Cleanup<br>
// MAGIC 
// MAGIC Run the **`Classroom-Cleanup`** cell below to remove any artifacts created by this lesson.

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Cleanup"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Next Steps
// MAGIC 
// MAGIC Start the next lesson, [Connecting to JDBC]($./ETL1 04 - Connecting to JDBC ).

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Topics & Resources
// MAGIC 
// MAGIC **Q:** Where can I find more information on DBFS?  
// MAGIC **A:** <a href="https://docs.azuredatabricks.net/user-guide/dbfs-databricks-file-system.html#dbfs" target="_blank">Take a look at the Databricks documentation for more details

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>