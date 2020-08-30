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
# MAGIC # Table Management
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; allow you to access and optimize data in managed and unmanaged tables.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Write to managed and unmanaged tables
# MAGIC * Explore the effect of dropping tables on the metadata and underlying data
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
# MAGIC src="//fast.wistia.net/embed/iframe/gyw20pwx6j?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/gyw20pwx6j?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Optimization of Data Storage with Managed and Unmanaged Tables
# MAGIC 
# MAGIC A **managed table** is a table that manages both the data itself as well as the metadata.  In this case, a `DROP TABLE` command removes both the metadata for the table as well as the data itself.  
# MAGIC 
# MAGIC **Unmanaged tables** manage the metadata from a table such as the schema and data location, but the data itself sits in a different location, often backed by a blob store like the Azure Blob or S3. Dropping an unmanaged table drops only the metadata associated with the table while the data itself remains in place.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-2/managed-and-unmanaged-tables.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing to a Managed Table
# MAGIC 
# MAGIC Managed tables allow access to data using the Spark SQL API.

# COMMAND ----------

# MAGIC %md
# MAGIC Create a DataFrame.

# COMMAND ----------

df = spark.range(1, 100)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Register the table.

# COMMAND ----------

df.write.mode("OVERWRITE").saveAsTable("myTableManaged")

# COMMAND ----------

# MAGIC %md
# MAGIC Use `DESCRIBE EXTENDED` to describe the contents of the table.  Scroll down to see the table `Type`.
# MAGIC 
# MAGIC Notice the location is also `dbfs:/user/hive/warehouse/< your database >/mytablemanaged`.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED myTableManaged

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing to an Unmanaged Table
# MAGIC 
# MAGIC Write to an unmanaged table by adding an `.option()` that includes a path.

# COMMAND ----------

unmanagedPath = f"{workingDir}/myTableUnmanaged"

df.write.mode("OVERWRITE").option('path', unmanagedPath).saveAsTable("myTableUnmanaged")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Now examine the table type and location of the data.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> An external table is the same as an unmanaged table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED myTableUnmanaged

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dropping Tables
# MAGIC 
# MAGIC Take a look at how dropping tables operates differently in the two cases below.

# COMMAND ----------

# MAGIC %md
# MAGIC Look at the files backing up the managed table.

# COMMAND ----------

hivePath = f"dbfs:/user/hive/warehouse/{databaseName}.db/mytablemanaged"

display(dbutils.fs.ls(hivePath))

# COMMAND ----------

# MAGIC %md
# MAGIC Drop the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE myTableManaged

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Next look at the underlying data.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This command will throw an error.

# COMMAND ----------

try:
  display(dbutils.fs.ls(hivePath))
  
except Exception as e:
  print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC The data was deleted so spark will not find the underlying data. Perform the same operation with the unmanaged table.

# COMMAND ----------

display(dbutils.fs.ls(unmanagedPath))

# COMMAND ----------

# MAGIC %md
# MAGIC Drop the unmanaged table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE myTableUnmanaged

# COMMAND ----------

# MAGIC %md
# MAGIC See if the data is still there.

# COMMAND ----------

display(dbutils.fs.ls(unmanagedPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What happens to the original data when I delete a managed table?  What about an unmanaged table?  
# MAGIC **Answer:** Deleting a managed table deletes both the metadata and the data itself. Deleting an unmanaged table does not delete the original data.
# MAGIC 
# MAGIC **Question:** What is a metastore?  
# MAGIC **Answer:** A metastore is a repository of metadata such as the location of where data is and the schema information. A metastore does not include the data itself.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Cleanup<br>
# MAGIC 
# MAGIC Run the **`Classroom-Cleanup`** cell below to remove any artifacts created by this lesson.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Cleanup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> All done!</h2>
# MAGIC 
# MAGIC Thank you for your participation!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find out more about connecting to my own metastore?  
# MAGIC **A:** Take a look at the <a href="https://docs.databricks.com/user-guide/advanced/external-hive-metastore.html" target="_blank">Databricks documentation for more details</a>
# MAGIC 
# MAGIC **Q:** Where can I find out more about Spark Tables?  
# MAGIC **A:** Take a look at the <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Databricks documentation for more details</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>