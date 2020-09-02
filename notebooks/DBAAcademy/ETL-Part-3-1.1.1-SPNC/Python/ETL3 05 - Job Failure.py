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
# MAGIC # Job Failure
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; allow for the creation of robust job failure strategies
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Use antijoins to ensure that duplicate records are not loaded into your target database
# MAGIC * Design a job failure monitoring strategy using job ID's
# MAGIC * Evaluate job failure recovery strategies for idempotence 
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
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Setup
# MAGIC 
# MAGIC For each lesson to execute correctly, please make sure to run the **`Classroom-Setup`** cell at the<br/>
# MAGIC start of each lesson (see the next cell) and the **`Classroom-Cleanup`** cell at the end of each lesson.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/6env3ecrhv?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/6env3ecrhv?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Idempotent Failure Recovery
# MAGIC 
# MAGIC Jobs can fail for any number of reasons.  The majority of job failures are caused by input/output (I/O) problems but other issues include schema evolution, data corruption, and hardware failures.  Recovery from job failure should be guided by the principle of *idempotence, or the property of operations whereby the operation can be applied multiple times without changing the results beyond the first application.*
# MAGIC 
# MAGIC More technically, the definition of idempotence is as follows where a function `f` applied to `x` is equal to that function applied to `x` two or more times:
# MAGIC 
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;`f(x) = f(f(x)) = f(f(f(x))) = ...`
# MAGIC 
# MAGIC In ETL job recovery, we need to be able to run a job multiple times and get our data into our target database without duplicates.  This can be accomplished in a few ways:<br><br>
# MAGIC 
# MAGIC * A **left antijoin** of new data on data already in a database will give you only the data that was not inserted
# MAGIC * Overwriting all data is a resource-intensive way to ensure that all data was written
# MAGIC * The transactionality of databases enable all-or-nothing database writes where failure of any part of the job will not result in any committed data
# MAGIC * Leveraging primary keys in a database will only write data where the primary key is not already present or upsert the data

# COMMAND ----------

# MAGIC %md
# MAGIC ### One Idempotent Strategy: Left Antijoin
# MAGIC 
# MAGIC In traditional ETL, a job recovery strategy where only partial data was written to database would look something as follow:
# MAGIC 
# MAGIC ```
# MAGIC begin transaction;
# MAGIC   delete from production_table where batch_period = failed_batch_period;
# MAGIC   insert into production_table select * from staging_table;
# MAGIC   drop table staging_table;  
# MAGIC end transaction;
# MAGIC ```
# MAGIC 
# MAGIC This won't work in a Spark environment because data structures are immutable.  One alternative strategy among the several listed in the cell above relies on a left antijoin, which returns all data in the left table that doesn't exist in the right table.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the follow cell to create a mock production and staging table. Create a staging table from parquet that contains log records and then create a production table that only has 20 percent of the records from staging.

# COMMAND ----------

from pyspark.sql.functions import col 

staging_table = (spark.read.parquet("/mnt/training/EDGAR-Log-20170329/enhanced/EDGAR-Log-20170329-sample.parquet/")
  .dropDuplicates(['ip', 'date', 'time']))

production_table = staging_table.sample(.2, seed=123)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to see that the `poduction_table` only has 20% of the data from `staging_table`

# COMMAND ----------

production_table.count() / staging_table.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Join the two tables using a left antijoin.

# COMMAND ----------

failedDF = staging_table.join(production_table, on=["ip", "date", "time"], how="left_anti")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Union `production_table` with the results from the left antijoin.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Append operations are generally not idempotent as they can result in duplicate records.  Streaming operations that maintain state and append to an always up-to-date parquet or Databricks Delta table are idempotent.

# COMMAND ----------

fullDF = production_table.union(failedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC The two tables are now equal.

# COMMAND ----------

staging_table.count() == fullDF.count()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Monitoring Jobs for Failure
# MAGIC 
# MAGIC Monitoring for job failure often entails a server or cluster that tracks job performance.  One common monitoring table to build using this server or cluster is as follows:<br><br>
# MAGIC 
# MAGIC 1. `batchID`: A unique ID for each batch
# MAGIC 2. `runID`: The ID that matches the API call to execute a job
# MAGIC 3. `time`: Time of the query
# MAGIC 4. `status`: Status of the job
# MAGIC 
# MAGIC On job failure, jobs can be retried multiple times before fully failing.  Inital tries can utilize spot instances to reduce costs and fall back to on-demand resources as needed.
# MAGIC 
# MAGIC <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Cluster logs can be preserved off of the cluster itself for downstream analysis in case of failure.  <a href="https://docs.azuredatabricks.net/user-guide/clusters/log-delivery.html#id1" target="_blank">See the Databricks documentation for details.</a>  
# MAGIC <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> One best practice involves counting your data at the beginning of an ETL job and at the end to ensure that the expected data was written to the target database.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What is idempotence?  
# MAGIC **Answer:** For ETL jobs, idempotence is the ability to run the same job multiple times without getting duplicate data.  This is the primary axiom for ensuring that ETL workloads do not have any unexpected behavior.
# MAGIC 
# MAGIC **Question:** How can I accomplish idempotence in Spark jobs?  
# MAGIC **Answer:** There are a number of strategies for accomplishing this.  Doing an antijoin of your full data on already loaded data is one method.  This can be in the form of an incremental update script that would run on the case of job failure.  By counting the records at the beginning and end of a job, you can detect whether any unexpected behavior would demand the use of this incremental script.
# MAGIC 
# MAGIC **Question:** How can I detect job failure?  
# MAGIC **Answer:** This depends largely on the pipeline you're creating.  One common best practice is to have a monitoring job that periodically checks jobs for failure.  This can be tied to email or other alerting mechanisms.

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
# MAGIC Start the next lesson, [ETL Optimizations]($./ETL3 06 - ETL Optimizations ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** How can I enable email notifications on jobs in Databricks?  
# MAGIC **A:** Check out the <a href="https://docs.azuredatabricks.net/user-guide/jobs.html#id1" target="_blank">Databricks documentation for step-by-step instructions.</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>