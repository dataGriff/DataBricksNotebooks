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
# MAGIC # Structured Streaming with Kafka 
# MAGIC 
# MAGIC We have another server that reads Wikipedia edits in real time, with a multitude of different languages. 
# MAGIC 
# MAGIC **What you will learn:**
# MAGIC * About Kafka
# MAGIC * How to establish a connection with Kafka
# MAGIC * More examples 
# MAGIC * More visualizations
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Additional Audiences: Data Scientists and Software Engineers
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: **Chrome**
# MAGIC * A cluster configured with **8 cores** and **DBR 6.2**
# MAGIC * External Services
# MAGIC   - Familiarity with Kafka is helpful, but not required
# MAGIC * Suggested Courses from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>:
# MAGIC   - ETL Part 1
# MAGIC   - Spark-SQL

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
# MAGIC src="//fast.wistia.net/embed/iframe/p5v3fw7auc?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/p5v3fw7auc?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <img style="float:right" src="https://files.training.databricks.com/images/eLearning/Structured-Streaming/kafka.png"/>
# MAGIC 
# MAGIC <div>
# MAGIC   <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Kafka Ecosystem</h2>
# MAGIC   <p>Kafka is software designed upon the <b>publish/subscribe</b> messaging pattern.
# MAGIC      Publish/subscribe messaging is where a sender (publisher) sends a message that is not specifically directed to a receiver (subscriber). 
# MAGIC      The publisher classifies the message somehow and the receiver subscribes to receive certain categories of messages.
# MAGIC      There are other usage patterns for Kafka, but this is the pattern we focus on in this course.
# MAGIC   </p>
# MAGIC   <p>Publisher/subscriber systems typically have a central point where messages are published, called a <b>broker</b>. 
# MAGIC      The broker receives messages from publishers, assigns offsets to them and commits messages to storage.
# MAGIC   </p>
# MAGIC 
# MAGIC   <p>The Kafka version of a unit of data an array of bytes called a <b>message</b>.</p>
# MAGIC 
# MAGIC   <p>A message can also contain a bit of information related to partitioning called a <b>key</b>.</p>
# MAGIC 
# MAGIC   <p>In Kafka, messages are categorized into <b>topics</b>.</p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Kafka Server</h2>
# MAGIC 
# MAGIC 
# MAGIC The Kafka server is fed by a separate TCP server that reads the Wikipedia edits, in real time, from the various language-specific IRC channels to which Wikimedia posts them. 
# MAGIC 
# MAGIC That server parses the IRC data, converts the results to JSON, and sends the JSON to
# MAGIC a Kafka server, with the edits segregated by language. The various languages are <b>topics</b>.
# MAGIC 
# MAGIC For example, the Kafka topic "en" corresponds to edits for en.wikipedia.org.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Required Options
# MAGIC 
# MAGIC When consuming from a Kafka source, you **must** specify at least two options:
# MAGIC 
# MAGIC <p>1. The Kafka bootstrap servers, for example:</p>
# MAGIC <p>`dsr.option("kafka.bootstrap.servers", "server1.databricks.training:9092")`</p>
# MAGIC <p>2. Some indication of the topics you want to consume.</p>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC #### Specifying a Topic
# MAGIC 
# MAGIC There are three, mutually-exclusive, ways to specify the topics for consumption:
# MAGIC 
# MAGIC | Option        | Value                                          | Description                            | Example |
# MAGIC | ------------- | ---------------------------------------------- | -------------------------------------- | ------- |
# MAGIC | **subscribe** | A comma-separated list of topics               | A list of topics to which to subscribe | `dsr.option("subscribe", "topic1")` <br/> `dsr.option("subscribe", "topic1,topic2,topic3")` |
# MAGIC | **assign**    | A JSON string indicating topics and partitions | Specific topic-partitions to consume.  | `dsr.dsr.option("assign", "{'topic1': [1,3], 'topic2': [2,5]}")`
# MAGIC | **subscribePattern**   | A (Java) regular expression           | A pattern to match desired topics      | `dsr.option("subscribePattern", "e[ns]")` <br/> `dsr.option("subscribePattern", "topic[123]")`|
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> In the example to follow, we're using the "subscribe" option to select the topics we're interested in consuming. 
# MAGIC We've selected only the "en" topic, corresponding to edits for the English Wikipedia. 
# MAGIC If we wanted to consume multiple topics (multiple Wikipedia languages, in our case), we could just specify them as a comma-separate list:
# MAGIC 
# MAGIC ```dsr.option("subscribe", "en,es,it,fr,de,eo")```
# MAGIC 
# MAGIC There are other, optional, arguments you can give the Kafka source. 
# MAGIC 
# MAGIC For more information, see the <a href="https://people.apache.org//~pwendell/spark-nightly/spark-branch-2.1-docs/latest/structured-streaming-kafka-integration.html#" target="_blank">Structured Streaming and Kafka Integration Guide</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Kafka Schema</h2>
# MAGIC 
# MAGIC Reading from Kafka returns a `DataFrame` with the following fields:
# MAGIC 
# MAGIC | Field             | Type   | Description |
# MAGIC |------------------ | ------ |------------ |
# MAGIC | **key**           | binary | The key of the record (not needed) |
# MAGIC | **value**         | binary | Our JSON payload. We'll need to cast it to STRING |
# MAGIC | **topic**         | string | The topic this record is received from (not needed) |
# MAGIC | **partition**     | int    | The Kafka topic partition from which this record is received (not needed). This server only has one partition. |
# MAGIC | **offset**        | long   | The position of this record in the corresponding Kafka topic partition (not needed) |
# MAGIC | **timestamp**     | long   | The timestamp of this record  |
# MAGIC | **timestampType** | int    | The timestamp type of a record (not needed) |
# MAGIC 
# MAGIC In the example below, the only column we want to keep is `value`.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The default of `spark.sql.shuffle.partitions` is 200.
# MAGIC This setting is used in operations like `groupBy`.
# MAGIC In this case, we should be setting this value to match the current number of cores.

# COMMAND ----------

from pyspark.sql.functions import col
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

kafkaServer = "server1.databricks.training:9092"   # US (Oregon)
# kafkaServer = "server2.databricks.training:9092" # Singapore

editsDF = (spark.readStream                        # Get the DataStreamReader
  .format("kafka")                                 # Specify the source format as "kafka"
  .option("kafka.bootstrap.servers", kafkaServer)  # Configure the Kafka server name and port
  .option("subscribe", "en")                       # Subscribe to the "en" Kafka topic
  .option("startingOffsets", "earliest")           # Rewind stream to beginning when we restart notebook
  .option("maxOffsetsPerTrigger", 1000)            # Throttle Kafka's processing of the streams
  .load()                                          # Load the DataFrame
  .select(col("value").cast("STRING"))             # Cast the "value" column to STRING
)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's display some data.

# COMMAND ----------

myStreamName = "lesson04a_ps"
display(editsDF,  streamName = myStreamName)

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until stream is done initializing...

# COMMAND ----------

untilStreamIsReady(myStreamName)

# COMMAND ----------

# MAGIC %md
# MAGIC Make sure to stop the stream before continuing.

# COMMAND ----------

stopAllStreams()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Use Kafka to display the raw data</h2>
# MAGIC 
# MAGIC The Kafka server acts as a sort of "firehose" (or asynchronous buffer) and displays raw data.
# MAGIC 
# MAGIC Since raw data coming in from a stream is transient, we'd like to save it to a more permanent data structure.
# MAGIC 
# MAGIC The first step is to define the schema for the JSON payload.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Only those fields of future interest are commented below.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark.sql.functions import from_json, unix_timestamp

schema = StructType([
  StructField("channel", StringType(), True),
  StructField("comment", StringType(), True),
  StructField("delta", IntegerType(), True),
  StructField("flag", StringType(), True),
  StructField("geocoding", StructType([                 # (OBJECT): Added by the server, field contains IP address geocoding information for anonymous edit.
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("countryCode2", StringType(), True),
    StructField("countryCode3", StringType(), True),
    StructField("stateProvince", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
  ]), True),
  StructField("isAnonymous", BooleanType(), True),      # (BOOLEAN): Whether or not the change was made by an anonymous user
  StructField("isNewPage", BooleanType(), True),
  StructField("isRobot", BooleanType(), True),
  StructField("isUnpatrolled", BooleanType(), True),
  StructField("namespace", StringType(), True),         # (STRING): Page's namespace. See https://en.wikipedia.org/wiki/Wikipedia:Namespace 
  StructField("page", StringType(), True),              # (STRING): Printable name of the page that was edited
  StructField("pageURL", StringType(), True),           # (STRING): URL of the page that was edited
  StructField("timestamp", StringType(), True),         # (STRING): Time the edit occurred, in ISO-8601 format
  StructField("url", StringType(), True),
  StructField("user", StringType(), True),              # (STRING): User who made the edit or the IP address associated with the anonymous editor
  StructField("userURL", StringType(), True),
  StructField("wikipediaURL", StringType(), True),
  StructField("wikipedia", StringType(), True),         # (STRING): Short name of the Wikipedia that was edited (e.g., "en" for the English)
])

# COMMAND ----------

# MAGIC %md
# MAGIC Next we can use the function `from_json` to parse out the full message with the schema specified above.

# COMMAND ----------

from pyspark.sql.functions import col, from_json

jsonEdits = editsDF.select(
  from_json("value", schema).alias("json"))  # Parse the column "value" and name it "json"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC When parsing a value from JSON, we end up with a single column containing a complex object.
# MAGIC 
# MAGIC We can clearly see this by simply printing the schema.

# COMMAND ----------

jsonEdits.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The fields of a complex object can be referenced with a "dot" notation as in:
# MAGIC 
# MAGIC `col("json.geocoding.countryCode3")` 
# MAGIC  
# MAGIC 
# MAGIC A large number of these fields/columns can become unwieldy.
# MAGIC 
# MAGIC For that reason, it is common to extract the sub-fields and represent them as first-level columns as seen below:

# COMMAND ----------

from pyspark.sql.functions import isnull, unix_timestamp

anonDF = (jsonEdits
  .select(col("json.wikipedia").alias("wikipedia"),      # Promoting from sub-field to column
          col("json.isAnonymous").alias("isAnonymous"),  #     "       "      "      "    "
          col("json.namespace").alias("namespace"),      #     "       "      "      "    "
          col("json.page").alias("page"),                #     "       "      "      "    "
          col("json.pageURL").alias("pageURL"),          #     "       "      "      "    "
          col("json.geocoding").alias("geocoding"),      #     "       "      "      "    "
          col("json.user").alias("user"),                #     "       "      "      "    "
          col("json.timestamp").cast("timestamp"))       # Promoting and converting to a timestamp
  .filter(col("namespace") == "article")                 # Limit result to just articles
  .filter(~isnull(col("geocoding.countryCode3")))        # We only want results that are geocoded
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Mapping Anonymous Editors' Locations</h2>
# MAGIC 
# MAGIC When you run the query, the default is a [live] html table.
# MAGIC 
# MAGIC The geocoded information allows us to associate an anonymous edit with a country.
# MAGIC 
# MAGIC We can then use that geocoded information to plot edits on a [live] world map.
# MAGIC 
# MAGIC In order to create a slick world map visualization of the data, you'll need to click on the item below.
# MAGIC 
# MAGIC Under <b>Plot Options</b>, use the following:
# MAGIC * <b>Keys:</b> `countryCode3`
# MAGIC * <b>Values:</b> `count`
# MAGIC 
# MAGIC In <b>Display type</b>, use <b>World map</b> and click <b>Apply</b>.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/Structured-Streaming/plot-options-map-04.png"/>
# MAGIC 
# MAGIC By invoking a `display` action on a DataFrame created from a `readStream` transformation, we can generate a LIVE visualization!
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Keep an eye on the plot for a minute or two and watch the colors change.

# COMMAND ----------

mappedDF = (anonDF
  .groupBy("geocoding.countryCode3") # Aggregate by country (code)
  .count()                           # Produce a count of each aggregate
)
display(mappedDF, streamName = myStreamName)

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until stream is done initializing...

# COMMAND ----------

untilStreamIsReady(myStreamName)

# COMMAND ----------

# MAGIC %md
# MAGIC Stop the streams.

# COMMAND ----------

stopAllStreams()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Review Questions</h2>
# MAGIC 
# MAGIC **Q:** What `format` should you use with Kafka?<br>
# MAGIC **A:** `format("kafka")`
# MAGIC 
# MAGIC **Q:** How do you specify a Kafka server?<br>
# MAGIC **A:** `.option("kafka.bootstrap.servers"", "server1.databricks.training:9092")`
# MAGIC 
# MAGIC **Q:** What verb should you use in conjunction with `readStream` and Kafka to start the streaming job?<br>
# MAGIC **A:** `load()`, but with no parameters since we are pulling from a Kafka server.
# MAGIC 
# MAGIC **Q:** What fields are returned in a Kafka DataFrame?<br>
# MAGIC **A:** Reading from Kafka returns a DataFrame with the following fields:
# MAGIC key, value, topic, partition, offset, timestamp, timestampType 

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Cleanup<br>
# MAGIC 
# MAGIC Run the **`Classroom-Cleanup`** cell below to remove any artifacts created by this lesson.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Cleanup"

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Next Steps</h2>
# MAGIC 
# MAGIC Start the next lab, [Using Kafka Lab]($./Labs/SS 04a - Using Kafka Lab).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Additional Topics &amp; Resources</h2>
# MAGIC 
# MAGIC * <a href="http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-stream#" target="_blank">Create a Kafka Source Stream</a>
# MAGIC * <a href="https://kafka.apache.org/documentation/" target="_blank">Official Kafka Documentation</a>
# MAGIC * <a href="https://www.confluent.io/blog/okay-store-data-apache-kafka/" target="_blank">Use Kafka to store data</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>