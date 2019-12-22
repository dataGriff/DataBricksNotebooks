# Databricks notebook source
# MAGIC %md
# MAGIC # Table Management
# MAGIC 
# MAGIC Apache Spark&trade; and Azure Databricks&reg; allow you to access and optimize data in managed and unmanaged tables.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Optimization of Data Storage with Managed and Unmanaged Tables
# MAGIC 
# MAGIC A **managed table** is a table that manages both the data itself as well as the metadata.  In this case, a `DROP TABLE` command removes both the metadata for the table as well as the data itself.  
# MAGIC 
# MAGIC **Unmanaged tables** manage the metadata from a table such as the schema and data location, but the data itself sits in a different location, often backed by a blob store like the Azure Blob Storage. Dropping an unmanaged table drops only the metadata associated with the table while the data itself remains in place.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-2/managed-and-unmanaged-tables.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing to a Managed Table
# MAGIC 
# MAGIC Managed tables allow access to data using the Spark SQL API.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to mount the data.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

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
# MAGIC Notice the location is also `dbfs:/user/hive/warehouse/mytable`.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED myTableManaged

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing to an Unmanaged Table
# MAGIC 
# MAGIC Write to an unmanaged table by adding an `.option()` that includes a path.

# COMMAND ----------

df.write.mode("OVERWRITE").option('path', '/tmp/myTableUnmanaged').saveAsTable("myTableUnmanaged")

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

# MAGIC %fs ls dbfs:/user/hive/warehouse/mytablemanaged

# COMMAND ----------

# MAGIC %md
# MAGIC Drop the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE myTableManaged

# COMMAND ----------

# MAGIC %md
# MAGIC Next look at the underlying data.

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/mytablemanaged

# COMMAND ----------

# MAGIC %md
# MAGIC The data was deleted so spark will not find the underlying data. Perform the same operation with the unmanaged table.

# COMMAND ----------

# MAGIC %fs ls /tmp/myTableUnmanaged

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

# MAGIC %fs ls /tmp/myTableUnmanaged

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
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find out more about connnecting to my own metastore?  
# MAGIC **A:** Take a look at the <a href="https://docs.azuredatabricks.net/user-guide/advanced/external-hive-metastore.html" target="_blank">Databricks documentation for more details</a>
# MAGIC 
# MAGIC **Extra Practice:** Apply what you learned in this module by completing the optional [Custom Transformations, Aggregating, and Loading]($./Optional/Custom-Transformations) exercise.