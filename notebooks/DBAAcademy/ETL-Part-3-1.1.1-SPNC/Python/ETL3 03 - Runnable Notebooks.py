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
# MAGIC # Runnable Notebooks
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; notebooks can be run, opening the door for automated workflows.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Parameterize notebooks using widgets
# MAGIC * Execute single and multiple notebooks with dependencies
# MAGIC * Pass variables into notebooks using widgets
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
# MAGIC src="//fast.wistia.net/embed/iframe/6a1rc4aaif?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/6a1rc4aaif?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A Step Toward Full Automation
# MAGIC 
# MAGIC Notebooks are one--and not the only--way of interacting with the Spark and Databricks environment.  Notebooks can be executed independently and as recurring jobs.  They can also be exported and versioned using git.  Python files and Scala/Java jars can be executed against a Databricks cluster as well, allowing full integration with a developer's normal workflow.  Since notebooks can be executed like code files and compiled binaries, they offer a way of building production pipelines.
# MAGIC 
# MAGIC Functional programming design principles aid in thinking about pipelines.  In functional programming, your code always has known inputs and outputs without any side effects.  In the case of automating notebooks, coding notebooks in this way helps reduce any unintended side effects where each stage in a pipeline can operate independently from the rest.
# MAGIC 
# MAGIC More complex workflows using notebooks require managing dependencies between tasks and passing parameters into notebooks.  Dependency management can done by chaining notebooks together, for instance to run reporting logic after having completed a database write. Sometimes, when these pipelines become especially complex, chaining notebooks together isn't sufficient. In those cases, scheduling with Apache Airflow has become the preferred solution. Notebook widgets can be used to pass parameters to a notebook when the parameters are determined at runtime.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/notebook-workflows.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Widgets
# MAGIC 
# MAGIC Widgets allow for the customization of notebooks without editing the code itself.  They also allow for passing parameters into notebooks.  There are 4 types of widgets:
# MAGIC 
# MAGIC | Type          | Description                                                                                        |
# MAGIC |:--------------|:---------------------------------------------------------------------------------------------------|
# MAGIC | `text`        | Input a value in a text box.                                                                       |
# MAGIC | `dropdown`    | Select a value from a list of provided values.                                                     |
# MAGIC | `combobox`    | Combination of text and dropdown. Select a value from a provided list or input one in the text box.|
# MAGIC | `multiselect` | Select one or more values from a list of provided values.                                          |
# MAGIC 
# MAGIC Widgets are Databricks utility functions that can be accessed using the `dbutils.widgets` package and take a name, default value, and values (if not a `text` widget).
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Check out <a href="https://docs.azuredatabricks.net/user-guide/notebooks/widgets.html#id1" target="_blank">the Databricks documentation on widgets for additional information </a>

# COMMAND ----------

dbutils.widgets.dropdown("MyWidget", "1", [str(x) for x in range(1, 5)])

# COMMAND ----------

# MAGIC %md
# MAGIC Notice the widget created at the top of the screen.  Choose a number from the dropdown menu.  Now, bring that value into your code using the `get` method.

# COMMAND ----------

dbutils.widgets.get("MyWidget")

# COMMAND ----------

# MAGIC %md
# MAGIC Clear the widgets using either `remove()` or `removeAll()`

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC While great for adding parameters to notebooks and dashboards, widgets also allow us to pass parameters into notebooks when we run them like a Python or JAR file.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running Notebooks
# MAGIC 
# MAGIC There are two options for running notebooks.  The first is using `dbutils.notebook.run("<path>", "<timeout>")`.  This will run the notebook.  [Take a look at this notebook first to see what it accomplishes.]($./Runnable/Runnable-1 )
# MAGIC 
# MAGIC Now run the notebook with the following command.

# COMMAND ----------

return_value = dbutils.notebook.run("./Runnable/Runnable-1", 30)

print("Notebook successfully ran with return value: {}".format(return_value))

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how the `Runnable-1` notebook ends with the command `dbutils.notebook.exit("returnValue")`.  This is a `string` that's passed back into the running notebook's environment.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell and note how the variable doesn't exist.

# COMMAND ----------

# TODO

print(my_variable) # This will fail

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC This variable is not passed into our current environment.  The difference between `dbutils.notebook.run()` and `%run` is that the parent notebook will inherit variables from the ran notebook with `%run`.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This is how the classroom setup script at the beginning of each lesson works.  Among other things, it defines the variable `userhome` for you so that you have a unique write destination from colleagues on your Databricks workspace

# COMMAND ----------

# MAGIC %run ./Runnable/Runnable-1

# COMMAND ----------

# MAGIC %md
# MAGIC Now this variable is available for use in this notebook

# COMMAND ----------

print(my_variable)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameter Passing and Debugging
# MAGIC 
# MAGIC Notebook widgets allow us to pass parameters into notebooks.  This can be done in the form of a dictionary that maps the widget name to a value as a `string`.
# MAGIC 
# MAGIC [Take a look at the second notebook to see what it accomplishes.]($./Runnable/Runnable-2 )

# COMMAND ----------

# MAGIC %md
# MAGIC Pass your parameters into `dbutils.notebook.run` and save the resulting return value

# COMMAND ----------

basePath =  "{}/etl3p/".format(getUserhome())
dest_path = "{}/academy/raw_logs.parquet".format(basePath)

result = dbutils.notebook.run("./Runnable/Runnable-2", 60, {"date": "11-27-2013", "dest_path": dest_path})

# COMMAND ----------

# MAGIC %md
# MAGIC Click on `Notebook job #XXX` above to view the output of the notebook.  **This is helpful for debugging any problems.**

# COMMAND ----------

# MAGIC %md
# MAGIC Parse the JSON string results

# COMMAND ----------

import json
print(json.loads(result))

# COMMAND ----------

# MAGIC %md
# MAGIC Now look at what this accomplished: cell phone logs were parsed corresponding to the date of the parameter passed into the notebook.  The results were saved to the given destination path.

# COMMAND ----------

display(spark.read.parquet(dest_path))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Dependency Management, Timeouts, Retries, and Passing Data
# MAGIC 
# MAGIC Running notebooks can allow for more advanced workflows in the following ways:<br><br>
# MAGIC 
# MAGIC * Managing **dependencies** can be ensured by running a notebook that triggers other notebooks in the desired order
# MAGIC * Setting **timeouts** ensures that jobs have a set limit on when they must either complete or fail
# MAGIC * **Retry logic** ensures that fleeting failures do not prevent the proper execution of a notebook
# MAGIC * **Data can passed** between notebooks by saving the data to a blob store or table and passing the path as an exit parameter
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Check out <a href="https://docs.azuredatabricks.net/user-guide/notebooks/notebook-workflows.html" target="_blank">the Databricks documentation on Notebook Workflows for additional information </a>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Building a Generalized Notebook
# MAGIC 
# MAGIC Build a notebook that allows for customization using input parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Filter an Hour of Log Data
# MAGIC 
# MAGIC [Fill out the `Runnable-3` notebook]($./Runnable/Runnable-3 ) that takes accomplishes the following (it's helpful to open this in another tab):<br><br>
# MAGIC 
# MAGIC 1. Takes two parameters: `hour` and `output_path`
# MAGIC 1. Reads the following log file: `/mnt/training/EDGAR-Log-20170329/enhanced/EDGAR-Log-20170329-sample.parquet`
# MAGIC 1. Filters the data for the hour provided
# MAGIC 1. Writes the result to the `output_path`
# MAGIC 1. Exits with the `output_path` as the exit parameter

# COMMAND ----------

path = "{}/hour_03.parquet".format(basePath)

dbutils.notebook.run("./Runnable/Runnable-3", 60, {"hour": "03", "output_path": path})

# COMMAND ----------

# TEST - Run this cell to test your solution
import random

r = str(random.randint(0, 10**10))
_path = "{}/hour_08_{}.parquet".format(basePath, r)

_returnValue = dbutils.notebook.run("./Runnable/Runnable-3", 60, {"hour": "08", "output_path": _path})
_df = spark.read.parquet(_returnValue)

dbTest("ET3-P-03-01-01", True, _path == _returnValue)
dbTest("ET3-P-03-01-02", 54206, _df.count())

dbutils.fs.rm(_path, True)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** How can I start to transition from notebooks to production environments?  
# MAGIC **Answer:** Runnable notebooks are the first step towards transitioning into production environments since they allow us to generalize and parameterize our code.  There are other options, including running Python files and Scala/Java jars against a cluster as well.
# MAGIC 
# MAGIC **Question:** How does passing parameters into and out of notebooks work?  
# MAGIC **Answer:** Widgets allow for the customization of notebooks and passing parameters into them.  This takes place in the form of a dictionary (Python) or map (Scala) of key/value pairs that match the names of widgets.  Only strings can be passed out of a notebook as an exit parameter.
# MAGIC 
# MAGIC **Question:** Since I can only pass strings of a limited length out of a notebook, how can I pass data out of a notebook?  
# MAGIC **Answer:** The preferred way is to save your data to a blob store or Spark table.  On the notebook's exit, pass the location of that data as a string.  It can then be easily imported and manipulated in another notebook.

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
# MAGIC Start the next lesson, [Scheduling Jobs Programatically]($./ETL3 04 - Scheduling Jobs Programatically ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** How can I integrate Databricks with more complex workflow schedulers like Apache Airflow?  
# MAGIC **A:** Check out the Databricks blog <a href="https://databricks.com/blog/2017/07/19/integrating-apache-airflow-with-databricks.html" target="_blank">Integrating Apache Airflow with Databricks</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>