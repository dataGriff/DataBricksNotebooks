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
# MAGIC # Structured Streaming
# MAGIC 
# MAGIC <p>
# MAGIC Structured Streaming is an efficient way to ingest large quantities of data from a variety of sources.<br> 
# MAGIC This course is intended to teach you how how to use Structured Streaming to ingest data from files and<br>
# MAGIC publisher-subscribe systems. Starting with the fundamentals of streaming systems, we introduce concepts<br> 
# MAGIC such as reading streaming data, writing out streaming data to directories, displaying streaming data and<br>
# MAGIC Triggers. We discuss the problems associated with trying to aggregate streaming data and then teach<br>
# MAGIC how to solve this problem using structures called windows and expiring old data using watermarking.<br>
# MAGIC Finally, we examine how to connect Structured Streaming with popular publish-subscribe systems to <br>
# MAGIC stream data from Wikipedia.
# MAGIC </p>
# MAGIC 
# MAGIC ## Upon completion of this course, students should be able to
# MAGIC 
# MAGIC * Read, write and display streaming data.	
# MAGIC * Apply time windows and watermarking to aggregate streaming data.
# MAGIC * Use a publish-subscribe system to stream wikipedia data in order to visualize meaningful analytics	
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: **Chrome**
# MAGIC * A cluster configured with **8 cores** and **DBR 6.2**
# MAGIC * Suggested Courses from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>:
# MAGIC   - ETL Part 1
# MAGIC   - Spark-SQL

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Before You Start</h2>
# MAGIC 
# MAGIC Before starting this course, you will need to create a cluster and attach it to this notebook.
# MAGIC 
# MAGIC Please configure your cluster to use Databricks Runtime version **6.2** which includes:
# MAGIC - Python Version 3.x
# MAGIC - Scala Version 2.11
# MAGIC - Apache Spark 2.4.4
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Do not use an ML or GPU accelerated runtimes
# MAGIC 
# MAGIC Step-by-step instructions for creating a cluster are included here:
# MAGIC - <a href="https://www.databricks.training/step-by-step/creating-clusters-on-azure" target="_blank">Azure Databricks</a>
# MAGIC - <a href="https://www.databricks.training/step-by-step/creating-clusters-on-aws" target="_blank">Databricks on AWS</a>
# MAGIC - <a href="https://www.databricks.training/step-by-step/creating-clusters-on-ce" target="_blank">Databricks Community Edition (CE)</a>
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This courseware has been tested against the specific DBR listed above. Using an untested DBR may yield unexpected results and/or various errors. If the required DBR has been deprecated, please <a href="https://academy.databricks.com/" target="_blank">download an updated version of this course</a>.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Setup
# MAGIC In general, all courses are designed to run on one of the following Databricks platforms:
# MAGIC * Databricks Community Edition (CE)
# MAGIC * Databricks (an AWS hosted service)
# MAGIC * Azure-Databricks (an Azure-hosted service)
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Some features are not available on the Community Edition, which limits the ability of some courses to be executed in that environment. Please see the course's prerequisites for specific information on this topic.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Additionally, private installations of Databricks (e.g., accounts provided by your employer) may have other limitations imposed, such as aggressive permissions and or language restrictions such as prohibiting the use of Scala which will further inhibit some courses from being executed in those environments.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** All courses provided by Databricks Academy rely on custom variables, functions, and settings to provide you with the best experience possible.
# MAGIC 
# MAGIC For each lesson to execute correctly, please make sure to run the **`Classroom-Setup`** cell at the start of each lesson (see the next cell) and the **`Classroom-Cleanup`** cell at the end of each lesson.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/mkslslo1zl?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/mkslslo1zl?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Problem</h2>
# MAGIC 
# MAGIC We have a stream of data coming in from a TCP-IP socket, Kafka, Kinesis or other sources...
# MAGIC 
# MAGIC The data is coming in faster than it can be consumed
# MAGIC 
# MAGIC How do we solve this problem?
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/drinking-from-the-fire-hose.png"  style="height: 300px;"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Micro-Batch Model</h2>
# MAGIC 
# MAGIC Many APIs solve this problem by employing a Micro-Batch model.
# MAGIC 
# MAGIC In this model, we take our firehose of data and collect data for a set interval of time (the **Trigger Interval**).
# MAGIC 
# MAGIC In our example, the **Trigger Interval** is two seconds.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/streaming-timeline.png" style="height: 150px;"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Processing the Micro-Batch</h2>
# MAGIC 
# MAGIC For each interval, our job is to process the data from the previous [two-second] interval.
# MAGIC 
# MAGIC As we are processing data, the next batch of data is being collected for us.
# MAGIC 
# MAGIC In our example, we are processing two seconds worth of data in about one second.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/streaming-timeline-1-sec.png" style="height: 150px;">

# COMMAND ----------

# MAGIC %md
# MAGIC ### What happens if we don't process the data fast enough when reading from a TCP/IP Stream?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <html>
# MAGIC   <head>
# MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
# MAGIC     <link rel="stylesheet" type="text/css" href="https://files.training.databricks.com/static/assets/spark-ilt/labs.css">
# MAGIC   </head>
# MAGIC   <body>
# MAGIC     <div id="the-button"><button style="width:15em" onclick="block('the-answer', 'the-button')">Continue Reading</button></div>
# MAGIC 
# MAGIC     <div id="the-answer" style="display:none">
# MAGIC       <p>In the case of a TCP/IP stream, we will most likely drop packets.</p>
# MAGIC       <p>In other words, we would be losing data.</p>
# MAGIC       <p>If this is an IoT device measuring the outside temperature every 15 seconds, this might be OK.</p>
# MAGIC       <p>If this is a critical shift in stock prices, you could be out thousands of dollars.</p>
# MAGIC     </div>
# MAGIC   </body>
# MAGIC </html>

# COMMAND ----------

# MAGIC %md
# MAGIC ### What happens if we don't process the data fast enough when reading from a pubsub system like Kafka?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <html>
# MAGIC   <head>
# MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
# MAGIC     <link rel="stylesheet" type="text/css" href="https://files.training.databricks.com/static/assets/spark-ilt/labs.css">
# MAGIC   </head>
# MAGIC   <body>
# MAGIC     <div id="the-button"><button style="width:15em" onclick="block('the-answer', 'the-button')">Continue Reading</button></div>
# MAGIC 
# MAGIC     <div id="the-answer" style="display:none">
# MAGIC       <p>In the case of a pubsub system, it simply means we fall further behind.</p>
# MAGIC       <p>Eventually, the pubsub system would reach resource limits inducing other problems.</p>
# MAGIC       <p>However, we can always re-launch the cluster with enough cores to catch up and stay current.</p>
# MAGIC     </div>
# MAGIC   </body>
# MAGIC </html>

# COMMAND ----------

# MAGIC %md
# MAGIC Our goal is simply to process the data for the previous interval before data from the next interval arrives.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> From Micro-Batch to Table</h2>
# MAGIC 
# MAGIC In Apache Spark, we treat such a stream of **micro-batches** as continuous updates to a table.
# MAGIC 
# MAGIC The developer then defines a query on this **input table**, as if it were a static table.
# MAGIC 
# MAGIC The computation on the input table is then pushed to a **results table**.
# MAGIC 
# MAGIC And finally, the results table is written to an output **sink**. 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/Delta/stream2rows.png" style="height: 300px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC In general, Spark Structured Streams consist of two parts:
# MAGIC * The **Input source** such as 
# MAGIC   * Kafka
# MAGIC   * Azure Event Hub
# MAGIC   * Files on a distributed system
# MAGIC   * TCP-IP sockets
# MAGIC * And the **Sinks** such as
# MAGIC   * Kafka
# MAGIC   * Azure Event Hub
# MAGIC   * Various file formats
# MAGIC   * The system console
# MAGIC   * Apache Spark tables (memory sinks)
# MAGIC   * The completely custom `foreach()` iterator

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Triggers
# MAGIC Developers define **triggers** to control how frequently the **input table** is updated. 
# MAGIC 
# MAGIC Each time a trigger fires, Spark checks for new data (new rows for the input table), and updates the result.
# MAGIC 
# MAGIC From the docs for `DataStreamWriter.trigger(Trigger)`:
# MAGIC > The default value is ProcessingTime(0) and it will run the query as fast as possible.
# MAGIC 
# MAGIC And the process repeats in perpetuity.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Summary</h2>
# MAGIC 
# MAGIC Use cases for streaming include bank card transactions, log files, Internet of Things (IoT) device data, video game play events and countless others.
# MAGIC 
# MAGIC Some key properties of streaming data include:
# MAGIC * Data coming from a stream is typically not ordered in any way
# MAGIC * The data is streamed into a **data lake**
# MAGIC * The data is coming in faster than it can be consumed
# MAGIC * Streams are often chained together to form a data pipeline
# MAGIC * Streams don't have to run 24/7:
# MAGIC   * Consider the new log files that are processed once an hour
# MAGIC   * Or the financial statement that is processed once a month

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Review Questions</h2>
# MAGIC 
# MAGIC **Question:** What is Structured Streaming?<br>
# MAGIC **Answer:** A stream is a sequence of data that is made available over time.<br>
# MAGIC Structured Streaming where we treat a <b>stream</b> of data as a table to which data is continously appended.<br>
# MAGIC The developer then defines a query on this input table, as if it were a static table, to compute a final result table that will be written to an output <b>sink</b>. 
# MAGIC .
# MAGIC 
# MAGIC **Question:** What purpose do triggers serve?<br>
# MAGIC **Answer:** Developers define triggers to control how frequently the input table is updated.
# MAGIC 
# MAGIC **Question:** How does micro batch work?<br>
# MAGIC **Answer:** We take our firehose of data and collect data for a set interval of time (the Trigger Interval).<br>
# MAGIC For each interval, our job is to process the data from the previous time interval.<br>
# MAGIC As we are processing data, the next batch of data is being collected for us.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Cleanup<br>
# MAGIC 
# MAGIC During the course of this lesson, files, tables, and other artifacts may have been created.
# MAGIC 
# MAGIC These resources create clutter, consume resources (generally in the form of storage), and may potentially incur some [minor] long-term expense.
# MAGIC 
# MAGIC You can remove these artifacts by running the **`Classroom-Cleanup`** cell below.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Next Steps</h2>
# MAGIC 
# MAGIC Start the next lesson, [Streaming Concepts]($./SS 02 - Streaming Concepts).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>