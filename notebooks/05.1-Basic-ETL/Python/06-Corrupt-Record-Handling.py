# Databricks notebook source
# MAGIC %md
# MAGIC # Corrupt Record Handling
# MAGIC 
# MAGIC Apache Spark&trade; and Azure Databricks&reg; provide ways to handle corrupt records.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Working with Corrupt Data
# MAGIC 
# MAGIC ETL pipelines need robust solutions to handle corrupt data. This is because data corruption scales as the size of data and complexity of the data application grow. Corrupt data includes:  
# MAGIC <br>
# MAGIC * Missing information
# MAGIC * Incomplete information
# MAGIC * Schema mismatch
# MAGIC * Differing formats or data types
# MAGIC * User errors when writing data producers
# MAGIC 
# MAGIC Since ETL pipelines are built to be automated, production-oriented solutions must ensure pipelines behave as expected. This means that **data engineers must both expect and systematically handle corrupt records.**
# MAGIC 
# MAGIC In the roadmap for ETL, this is the **Handle Corrupt Records** step:
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/ETL-Process-3.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to mount the data:

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Run the following cell, which contains a corrupt record, `{"a": 1, "b, "c":10}`:
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This is not the preferred way to make a DataFrame.  This code allows us to mimic a corrupt record you might see in production.

# COMMAND ----------

data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')

corruptDF = (spark.read
  .option("mode", "PERMISSIVE")
  .option("columnNameOfCorruptRecord", "_corrupt_record")
  .json(sc.parallelize(data))
)
display(corruptDF)  

# COMMAND ----------

# MAGIC %md
# MAGIC In the previous results, Spark parsed the corrupt record into its own column and processed the other records as expected. This is the default behavior for corrupt records, so you didn't technically need to use the two options `mode` and `columnNameOfCorruptRecord`.
# MAGIC 
# MAGIC There are three different options for handling corrupt records [set through the `ParseMode` option](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/ParseMode.scala#L34):
# MAGIC 
# MAGIC | `ParseMode` | Behavior |
# MAGIC |-------------|----------|
# MAGIC | `PERMISSIVE` | Includes corrupt records in a "_corrupt_record" column (by default) |
# MAGIC | `DROPMALFORMED` | Ignores all corrupted records |
# MAGIC | `FAILFAST` | Throws an exception when it meets corrupted records |
# MAGIC 
# MAGIC The following cell acts on the same data but drops corrupt records:

# COMMAND ----------

data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')

corruptDF = (spark.read
  .option("mode", "DROPMALFORMED")
  .json(sc.parallelize(data))
)
display(corruptDF)

# COMMAND ----------

# MAGIC %md
# MAGIC The following cell throws an error once a corrupt record is found, rather than ignoring or saving the corrupt records:

# COMMAND ----------

data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')

corruptDF = (spark.read
  .option("mode", "FAILFAST")
  .json(sc.parallelize(data))
)
display(corruptDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recommended Pattern: `badRecordsPath`
# MAGIC 
# MAGIC Databricks Runtime has [a built-in feature](https://docs.azuredatabricks.net/spark/latest/spark-sql/handling-bad-records.html#handling-bad-records-and-files) that saves corrupt records to a given end point. To use this, set the `badRecordsPath`.
# MAGIC 
# MAGIC This is a preferred design pattern since it persists the corrupt records for later analysis even after the cluster shuts down.

# COMMAND ----------

data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')

corruptDF = (spark.read
  .option("badRecordsPath", "/tmp/badRecordsPath")
  .json(sc.parallelize(data))
)
display(corruptDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC See the results in `/tmp/badRecordsPath`.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Recall that `/tmp` is a directory backed by S3 or the Azure Blob available to all clusters.

# COMMAND ----------

display(spark.read.json("/tmp/badRecordsPath/*/*/*"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Working with Corrupt Records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Diagnose the Problem
# MAGIC 
# MAGIC Import the data used in the last lesson, which is located at `/mnt/training/UbiqLog4UCI/14_F/log*`.  Import the corrupt records in a new column `SMSCorrupt`.  <br>
# MAGIC 
# MAGIC Save only the columns `SMS` and `SMSCorrupt` to the new DataFrame `SMSCorruptDF`.

# COMMAND ----------

# TODO
SMSCorruptDF = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
cols = set(SMSCorruptDF.columns)
SMSCount = SMSCorruptDF.cache().count()

dbTest("ET1-P-06-01-01", {'SMS', 'SMSCorrupt'}, cols)
dbTest("ET1-P-06-01-02", 8, SMSCount)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Examine the corrupt records to determine what the problem is with the bad records.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Take a look at the name in metadata.

# COMMAND ----------

# MAGIC %md
# MAGIC The entry `{"name": "mr Khojasteh"flash""}` should have single quotes around `flash` since the double quotes are interpreted as the end of the value.  It should read `{"name": "mr Khojasteh'flash'"}` instead.
# MAGIC 
# MAGIC The optimal solution is to fix the initial producer of the data to correct the problem at its source.  In the meantime, you could write ad hoc logic to turn this into a readable field.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Use `badRecordsPath`
# MAGIC 
# MAGIC Use the `badRecordsPath` option to save corrupt records to the directory `/tmp/corruptSMS`.

# COMMAND ----------

# TODO

# COMMAND ----------

# TEST - Run this cell to test your solution
SMSCorruptDF2.count()
corruptCount = spark.read.json("/tmp/corruptSMS/*/*/*").count()

dbTest("ET1-P-06-02-01", True, corruptCount >= 8)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** By default, how are corrupt records dealt with using `spark.read.json()`?  
# MAGIC **Answer:** They appear in a collumn called `_corrupt_record`.
# MAGIC 
# MAGIC **Question:** How can a query persist corrupt records in separate destination?  
# MAGIC **Answer:** The Databricks feature `badRecordsPath` allows a query to save corrupt records to a given end point for the pipeline engineer to investigate corruption issues.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Loading Data and Productionalizing]($./07-Loading-Data-and-Productionalizing).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I get more information on dealing with corrupt records?  
# MAGIC **A:** Check out the Spark Summit talk on <a href="https://databricks.com/session/exceptions-are-the-norm-dealing-with-bad-actors-in-etl" target="_blank">Exceptions are the Norm: Dealing with Bad Actors in ETL</a>