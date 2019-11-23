# Databricks notebook source
# MAGIC %md
# MAGIC # Challenge Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom".

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge Exercise 3
# MAGIC 
# MAGIC Starting with the `/mnt/training/ssn/names-1880-2016.parquet/` file, find the most popular first name for girls in 1885, 1915, 1945, 1975, and 2005.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1
# MAGIC Create a temporary view called `HistoricNames` where:
# MAGIC 0. You may need to create temporary DataFrames to generate the DataFrame listing the names.
# MAGIC 0. The result has three columns:
# MAGIC   * `firstName`
# MAGIC   * `year`
# MAGIC   * `total`
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** This is an example of a nested `SELECT` if you were to use SQL syntax. Using DataFrames, you will need to craft two queries and perform a `join`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC 
# MAGIC Load `/mnt/training/ssn/names-1880-2016.parquet` into a DataFrame called `ssaDF` and display the schema.

# COMMAND ----------

# TODO

ssaDF = # FILL_IN

ssaDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2
# MAGIC 
# MAGIC Craft a DataFrame query that solves the problem described above.
# MAGIC 
# MAGIC Display the output of the DataFrame.

# COMMAND ----------

# TODO

from pyspark.sql.functions import col, max
maxNamesDF = # FILL_IN
outerQueryDF = # FILL_IN
joinedQueryDF = # FILL_IN

joinedQueryDF.show()

# COMMAND ----------

# TEST - Run this cell to test your solution.

resultsDF = joinedQueryDF.select("firstName", "year", "total")
results = [ (r[0]+" "+str(r[1])+": "+str(r[2])) for r in resultsDF.collect()]

dbTest("DF-L3-Opt-historicNames-0", u'Mary 1885: 9128', results[0])
dbTest("DF-L3-Opt-historicNames-1", u'Mary 1915: 58187', results[1])
dbTest("DF-L3-Opt-historicNames-2", u'Mary 1945: 59288', results[2])
dbTest("DF-L3-Opt-historicNames-3", u'Jennifer 1975: 58186', results[3])
dbTest("DF-L3-Opt-historicNames-4", u'Emily 2005: 23934', results[4])