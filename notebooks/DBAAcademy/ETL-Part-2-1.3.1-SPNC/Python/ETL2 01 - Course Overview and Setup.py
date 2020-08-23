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
# MAGIC # Course Overview and Setup
# MAGIC ## ETL Part 2: Transformations and Loads
# MAGIC 
# MAGIC In this course, Data Engineers apply data transformation and extraction best practices such as user-defined functions, efficient table joins, and parallel database writes.  
# MAGIC By the end of this course, you will transform complex data with custom functions, load it into a target database, and navigate Databricks and Spark documents to source solutions.
# MAGIC 
# MAGIC ## Lessons
# MAGIC 1. Course Overview and Setup
# MAGIC 1. Common Transformations
# MAGIC 1. User Defined Functions
# MAGIC 1. Advanced UDFs
# MAGIC 1. Joins and Lookup Tables
# MAGIC 1. Database Writes
# MAGIC 1. Table Management
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Additional Audiences: Data Scientists and Data Pipeline Engineers
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: Chrome
# MAGIC * A cluster configured with **8 cores** and **DBR 6.2**
# MAGIC * Course: ETL Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>

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

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/xksj642zx1?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/xksj642zx1?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Raw, Query, and Summary Tables
# MAGIC 
# MAGIC A number of different terms describe the movement of data through an ETL pipeline. For course purposes, data begins in the pipeline with **raw tables.** This refers to data that arrives in the pipeline, conforms to a schema, and does not include any sort of parsing or aggregation.
# MAGIC 
# MAGIC Raw tables are then parsed into query-ready tables, known as **query tables.**  Query tables might populate a relational data model for ad hoc (OLAP) or online (OLTP) queries, but generally do not include any sort of aggregation such as daily averages.  Put another way, query tables are cleaned and filtered data.
# MAGIC 
# MAGIC Finally, **summary tables** are business level aggregates often used for reporting and dashboarding. This includes aggregations such as daily active website visitors.
# MAGIC 
# MAGIC It is a good idea to preserve raw tables because it lets you adapt to future needs not considered in the original data model or correct parsing errors. This pipeline ensures data engineers always have access to the data they need, while reducing the barriers to common data access patterns. Data becomes more valuable and easier to access in each subsequent stage of the pipeline.  
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-2/gold-silver-bronze.png" style="height: 400px; margin: 20px"/></div>

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

# MAGIC %run "./Includes/Classroom-Cleanup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the lesson [Common Transformations]($./ETL2 02 - Common Transformations).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>