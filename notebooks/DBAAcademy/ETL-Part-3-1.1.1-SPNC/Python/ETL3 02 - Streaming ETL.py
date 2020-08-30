# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img src="https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
# MAGIC 
# MAGIC # Streaming ETL
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; makes it easy to build scalable and fault-tolerant streaming ETL applications.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Define logic to read from a stream of data
# MAGIC * Perform a basic ETL job on a streaming data source
# MAGIC * Join a stream to historical data
# MAGIC * Write to an always up-to-date Databricks Delta table
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Additional Audiences: Data Scientists and Data Pipeline Engineers
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: Chrome
# MAGIC * A cluster configured with **8 cores** and **DBR 6.2**
# MAGIC * Course: ETL Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>
# MAGIC * Course: ETL Part 2 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Deprecation Warning
# MAGIC 
# MAGIC This lesson as been replaced by the Structured Streaming course.
# MAGIC 
# MAGIC If you have not completed an Instructor-Led or Self-Paced course on Structured Stream, we encourage you to do so now at <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>.
# MAGIC 
# MAGIC In addition to the Structured Streaming API and concepts, our prerequisites course also introduce a number of utility methods for controlling streams which are employed in this lesson:
# MAGIC * **`untilStreamIsReady(name)`**
# MAGIC * **`stopAllStreams()`**
# MAGIC 
# MAGIC Both of these methods are defined above in the call to **`Classroom-Setup`** and can be found in the notebook **`./Includes/Common-Notebooks/Utility-Methods`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Setup
# MAGIC 
# MAGIC For each lesson to execute correctly, please make sure to run the **`Classroom-Setup`** cell at the<br/>
# MAGIC start of each lesson (see the next cell) and the **`Classroom-Cleanup`** cell at the end of each lesson.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/y36vjvhx5e?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/y36vjvhx5e?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### ETL on Streaming Data
# MAGIC 
# MAGIC Spark Streaming enables scalable and fault-tolerant ETL operations that continuously clean and aggregate data before pushing it to data stores.  Streaming applications can also incorporate machine learning and other Spark features to trigger actions in real time, such as flagging potentially fraudulent user activity.  This lesson is meant as an introduction to streaming applications as they pertain to production ETL jobs.  
# MAGIC 
# MAGIC Streaming poses a number of specific obstacles. These obstacles include:<br><br>
# MAGIC 
# MAGIC * *End-to-end reliability and correctness:* Applications must be resilient to failures of any element of the pipeline caused by network issues, traffic spikes, and/or hardware malfunctions
# MAGIC * *Handle complex transformations:* applications receive many data formats that often involve complex business logic
# MAGIC * *Late and out-of-order data:* network issues can result in data that arrives late and out of its intended order
# MAGIC * *Integrate with other systems:* Applications must integrate with the rest of a data infrastructure
# MAGIC 
# MAGIC Streaming data sources in Spark offer the same DataFrames API for interacting with your data.  The crucial difference is that in structured streaming, the DataFrame is unbounded.  In other words, data arrives in an input stream and new records are appended to the input DataFrame.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/structured-streamining-model.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Connecting to the Stream
# MAGIC 
# MAGIC As data technology matures, the industry has been converging on a set of technologies.  Apache Kafka and the Azure managed alternative Event Hubs has become the ingestion engine at the heart of many pipelines.  
# MAGIC 
# MAGIC This technology brokers messages between producers, such as an IoT device writing data, and consumers, such as a Spark cluster reading data to perform real time analytics. There can be a many-to-many relationship between producers and consumers and the broker itself is scalable and fault tolerant.
# MAGIC 
# MAGIC Connect to a Kafka topic that is streaming Wikipedia event data.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/>  There are a number of ways to stream data.  One other common design pattern is to stream from an Azure Blob Container where any new files that appear will be read by the stream.  In this example, we'll be streaming directly from Kafka.

# COMMAND ----------

# MAGIC %md
# MAGIC Start by defining the schema of the data in the stream.

# COMMAND ----------

from pyspark.sql.types import BooleanType, IntegerType, StructType, StringType, StructField, TimestampType

schema = (StructType()
  .add("timestamp", TimestampType())
  .add("url", StringType())
  .add("userURL", StringType())
  .add("pageURL", StringType())
  .add("isNewPage", BooleanType())
  .add("geocoding", StructType()
    .add("countryCode2", StringType())
    .add("city", StringType())
    .add("latitude", StringType())
    .add("country", StringType())
    .add("longitude", StringType())
    .add("stateProvince", StringType())
    .add("countryCode3", StringType())
    .add("user", StringType())
    .add("namespace", StringType()))
)

# COMMAND ----------

# MAGIC %md
# MAGIC To read from a stream, use `spark.readStream()`, which returns a `DataStreamReader` class.  Then, configure the stream by adding the following options:<br><br>
# MAGIC 
# MAGIC * The server endpoint: `server1.databricks.training:9092`
# MAGIC * The topic to subscribe to: `en`
# MAGIC * A location to log checkpoint metadata (more on this below)
# MAGIC * The format: `kafka` 
# MAGIC 
# MAGIC Finally, use the load method, which loads the data stream from the Kafka source and returns it as an unbounded `DataFrame`.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

kafkaDF = (spark
  .readStream
  .option("kafka.bootstrap.servers", "server1.databricks.training:9092")
  .option("subscribe", "en")
  .format("kafka")
  .load()
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Kafka transmits information using a key, value, and metadata such as topic and partition.  The information we're interested in is the `value` column.  Since this is a binary value, we must first cast it to a `StringType`.  We must also provide it with a schema.  Finally, we can expand the full structure of the JSON.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Wait until the stream finishes initializing.  
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Often streams are started with the `.start()` method.  In this example, `display()`, provided in Databricks environments, is running that command for you.

# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType

kafkaCleanDF = (kafkaDF
  .select(from_json(col("value").cast(StringType()), schema).alias("message"))
  .select("message.*")
)

# COMMAND ----------

myStreamName = "lesson02_ps"
display(kafkaCleanDF, streamName = myStreamName)

# COMMAND ----------

# Wait until the stream is ready...
untilStreamIsReady(myStreamName)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we have a live stream of Wikipedia data.  Now stop the stream (and any other active streams) to reduce resource consumption.

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform the Stream
# MAGIC 
# MAGIC We can now start to apply transformation logic to our data in real time.  Parse out only the non-null country data.

# COMMAND ----------

goeocodingDF = (kafkaCleanDF
  .filter(col("geocoding.country").isNotNull())
  .select("timestamp", "pageURL", "geocoding.countryCode2", "geocoding.city")
)

display(goeocodingDF, streamName = myStreamName)

# COMMAND ----------

# Wait until the stream is ready...
untilStreamIsReady(myStreamName)

# COMMAND ----------

# MAGIC %md
# MAGIC Stop the stream

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine with Historical Data
# MAGIC 
# MAGIC Joins between historical data and streams operate in much the same way that other joins work.  Join the stream of Wikipedia data on country metadata

# COMMAND ----------

# MAGIC %md
# MAGIC Import a lookup table of country data.

# COMMAND ----------

countries = spark.read.parquet("/mnt/training/countries/ISOCountryCodes/ISOCountryLookup.parquet")

display(countries)

# COMMAND ----------

# MAGIC %md
# MAGIC Join the two `DataFrame`s on their two-letter country code.

# COMMAND ----------

joinedDF = goeocodingDF.join(countries, goeocodingDF.countryCode2 == countries.alpha2Code)

display(joinedDF, streamName = myStreamName)

# COMMAND ----------

# Wait until the stream is ready...
untilStreamIsReady(myStreamName)

# COMMAND ----------

# MAGIC %md
# MAGIC Stop the stream

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Batch and Streaming Writes
# MAGIC 
# MAGIC The logic that applies to batch processing can be ported over to streaming often with only minor modifications.  One best practice is to run batch operations as streaming jobs using a single trigger.  By using a checkpoint location, the metadata on which data has already been processed will be maintained so the cluster can be shut down without a loss of information.  This works best on streaming from a directory, where new files that appear are added to the stream.
# MAGIC 
# MAGIC Writes can be done against always up-to-date parquet files or a Databricks Delta table, which offers ACID compliant transactions on top of parquet.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> For more information on Databricks Delta, see the <a href="https://academy.databricks.com/collections/frontpage/products/using-databricks-delta-1-user-1-year" target="_blank">Databricks Delta course from Databricks Academy</a><br>
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Read more about triggers in the <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers" target="_blank">Structured Streaming Programming Guide</a>

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm that Delta is enabled on your cluster.

# COMMAND ----------

spark.sql("set spark.databricks.delta.preview.enabled=true")

# COMMAND ----------

# MAGIC %md
# MAGIC Write to a Databricks Delta table and partition by country.

# COMMAND ----------

basePath = "{}/etl3p".format(getUserhome())
checkpointPath = "{}/joined.checkpoint".format(basePath)
joinedPath = "{}/joined".format(basePath)

(joinedDF
  .writeStream                                   # Write the stream
  .format("delta")                               # Use the delta format
  .partitionBy("countryCode2")                   # Specify a feature to partition on
  .option("checkpointLocation", checkpointPath)  # Specify where to log metadata
  .option("path", joinedPath)                    # Specify the output path
  .outputMode("append")                          # Append new records to the output path
  .queryName(myStreamName)                       # The name of the stream
  .start()                                       # Start the operation
)

# COMMAND ----------

# Wait until the stream is ready...
untilStreamIsReady(myStreamName)

# COMMAND ----------

# MAGIC %md
# MAGIC Check to see that the data is there.

# COMMAND ----------

from pyspark.sql.functions import count

countsDF = (spark.read.format("delta").load(joinedPath)
  .select(count("*").alias("count"))
)

display(countsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Now stop all streams

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Failure Recovery
# MAGIC 
# MAGIC To recover from cluster failure, Spark uses checkpointing for maintaining state.  In our initial command, we accomplished this using `.option("checkpointLocation", checkpointPath)`.  The checkpoint directory is per query and while that query is active, Spark continuously writes metadata of the processed data to this checkpoint directory.  Even if the entire cluster fails, the query can be restarted on a new cluster using the same directory and Spark can use this to start a new query where the failed one left off.  
# MAGIC 
# MAGIC **This is how Spark ensures end-to-end, exactly-once guarantees and how Spark maintains metadata on what data it has already seen between streaming jobs.**

# COMMAND ----------

display(dbutils.fs.ls(checkpointPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Streaming ETL
# MAGIC 
# MAGIC This exercise entails reading from a Kafka stream of product orders and writing the results to a Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create a Streaming DataFrame
# MAGIC 
# MAGIC Create the streaming DataFrame `productsDF` using the following specifics:<br><br>
# MAGIC 
# MAGIC * Server endpoint: `server1.databricks.training:9092`
# MAGIC * Topic name: `product-orders`
# MAGIC * Format: `kafka`

# COMMAND ----------

# TODO
productsDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.sql.types import *

dbTest("ET3-P-02-01-01", True, productsDF.schema['key'].dataType in [BinaryType(), StringType()])
dbTest("ET3-P-02-01-02", True, productsDF.schema['value'].dataType in [BinaryType(), StringType()])
dbTest("ET3-P-02-01-03", True, productsDF.schema['topic'].dataType == StringType())
dbTest("ET3-P-02-01-04", True, productsDF.schema['partition'].dataType == IntegerType())
dbTest("ET3-P-02-01-05", True, productsDF.schema['offset'].dataType == LongType())
dbTest("ET3-P-02-01-06", True, productsDF.schema['timestamp'].dataType in [TimestampType(), StringType()])
dbTest("ET3-P-02-01-07", True, productsDF.schema['timestampType'].dataType == IntegerType())

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Defining a Schema
# MAGIC 
# MAGIC Define a schema that consists of the following values.  Save it as `productSchema`<br>
# MAGIC 
# MAGIC | Field            | Type             |
# MAGIC |:-----------------|:-----------------|
# MAGIC | `orderID`        | `IntegerType()`  |
# MAGIC | `productID`      | `IntegerType()`  |
# MAGIC | `orderTimestamp` | `TimestampType()`|
# MAGIC | `orderQty`       | `IntegerType()`  |

# COMMAND ----------

# TODO
productSchema = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.
from pyspark.sql.types import IntegerType, StringType, TimestampType

dbTest("ET3-P-02-02-01", True, productSchema['orderID'].dataType == IntegerType())
dbTest("ET3-P-02-02-02", True, productSchema['productID'].dataType == IntegerType())
dbTest("ET3-P-02-02-03", True, productSchema['orderTimestamp'].dataType in [StringType(), TimestampType()])
dbTest("ET3-P-02-02-04", True, productSchema['orderQty'].dataType == IntegerType())

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Parsing the Kafka Data
# MAGIC 
# MAGIC Parse the `value` column of the Kafka data.  Remember to use the `from_json()` function and apply your schema.  Save the results to `ordersDF`

# COMMAND ----------

# TODO
ordersDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.
from pyspark.sql.types import IntegerType, TimestampType

_df = ordersDF.select(col("order.orderID").alias("orderID"),
                       col("order.productID").alias("productID"),
                       col("order.orderTimestamp").alias("orderTimestamp"),
                       col("order.orderQty").alias("orderQty"))

expected = {"orderID": IntegerType(), "orderQty": IntegerType(), "orderTimestamp": TimestampType(), "productID": IntegerType()}
sch = {c.name: c.dataType for c in _df.schema}

dbTest("ET3-P-02-03-01", True, sch == expected)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Write to a Delta Table
# MAGIC 
# MAGIC Write the results to an always up-to-date Databricks Delta table to the provided path.  The table should have the following columns:<br><br>
# MAGIC 
# MAGIC 1. `orderID`
# MAGIC 1. `productID`
# MAGIC 1. `orderTimestamp`
# MAGIC 1. `orderQty`

# COMMAND ----------

# TODO

ordersPath = "{}/orders".format(basePath)
checkpointPath = "{}/orders.checkpoint".format(basePath)

FILL_IN

# COMMAND ----------

# Wait until the stream is ready...
untilStreamIsReady(myStreamName)

# COMMAND ----------

# TEST - Run this cell to test your solution.
from pyspark.sql.types import *

ordersPath = "{}/orders".format(basePath)
_df = spark.read.format("delta").load(ordersPath)

expected = {"orderID": IntegerType(), "orderQty": IntegerType(), "orderTimestamp": TimestampType(), "productID": IntegerType()}
sch = {c.name: c.dataType for c in _df.schema}

dbTest("ET3-P-02-04-01", True, sch == expected)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Now Stop the Stream and Delete Files
# MAGIC 
# MAGIC Stop the stream.

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC Recursively delete the files you created (this is a permanent operation).

# COMMAND ----------

dbutils.fs.rm(basePath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What is the best practice for different versions of ETL jobs?  
# MAGIC **Answer:** Generally speaking, an ETL solution should have three versions:
# MAGIC 0. *Batch* or the ability to run a periodic workload
# MAGIC 0. *Streaming* or the ability to process incoming data in real time
# MAGIC 0. *Incremental* or the ability to process a specific set of data, especially in the case of job failure.<br>
# MAGIC In practice, batch and streaming jobs are oftentimes combined where a batch job is a streaming workload using a single trigger.
# MAGIC 
# MAGIC **Question:** What are commonly approached as data streams?  
# MAGIC **Answer:** Apache Kafka and the Azure managed alternative Event Hubs are common data streams.  Additionally, it's common to monitor a directory for incoming files.  When a new file appears, it is brought into the stream for processing.
# MAGIC 
# MAGIC **Question:** How does Spark ensure exactly-once data delivery and maintain metadata on a stream?  
# MAGIC **Answer:** Checkpoints give Spark this fault tolerance through the ability to maintain state off of the cluster.
# MAGIC 
# MAGIC **Question:** How does the Spark approach to streaming integrate with other Spark features?  
# MAGIC **Answer:** Spark Streaming uses the same DataFrame API, allowing easy integration with other Spark functionality.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Cleanup<br>
# MAGIC 
# MAGIC Run the **`Classroom-Cleanup`** cell below to remove any artifacts created by this lesson.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Cleanup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Runnable Notebooks]($./ETL3 03 - Runnable Notebooks ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find out more information on streaming ETL jobs?  
# MAGIC **A:** Check out the Databricks blog post <a href="https://databricks.com/blog/2017/01/19/real-time-streaming-etl-structured-streaming-apache-spark-2-1.html" target="_blank">Real-time Streaming ETL with Structured Streaming in Apache Spark 2.1</a>
# MAGIC 
# MAGIC **Q:** Where can I get more information on integrating Streaming and Kafka?  
# MAGIC **A:** Check out the <a href="https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html" target="_blank">Structured Streaming + Kafka Integration Guide</a>
# MAGIC 
# MAGIC **Q:** Where can I see a case study on an IoT pipeline using Spark Streaming?  
# MAGIC **A:** Check out the Databricks blog post <a href="https://databricks.com/blog/2017/04/26/processing-data-in-apache-kafka-with-structured-streaming-in-apache-spark-2-2.html" target="_blank">Processing Data in Apache Kafka with Structured Streaming in Apache Spark 2.2</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>