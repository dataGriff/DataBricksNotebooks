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
# MAGIC # Scheduling Jobs Programatically
# MAGIC 
# MAGIC Apache Spark&trade; and Databricks&reg; can be automated through the Jobs UI, REST API, or the command line
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Submit jobs using the Jobs UI and REST API
# MAGIC * Monitor jobs using the REST API
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
# MAGIC src="//fast.wistia.net/embed/iframe/1ie2iv3vou?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/1ie2iv3vou?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Automating ETL Workloads
# MAGIC 
# MAGIC Since recurring production jobs are the goal of ETL workloads, Spark needs a way to integrate with other automation and scheduling tools.  We also need to be able to run Python files and Scala/Java jars.
# MAGIC 
# MAGIC Recall from <a href="https://academy.databricks.com/collections/frontpage/products/etl-part-1-data-extraction" target="_blank">ETL Part 1 course from Databricks Academy</a> how we can schedule jobs using the Databricks user interface.  In this lesson, we'll explore more robust solutions to schedule jobs.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/jobs.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC There are a number of different automation and scheduling tools including the following:<br><br>
# MAGIC 
# MAGIC * Command line tools integrated with the UNIX scheduler Cron
# MAGIC * The workflow scheduler Apache Airflow
# MAGIC * Microsoft's Scheduler or Data Factory
# MAGIC 
# MAGIC The gateway into job scheduling is programmatic access to Databricks, which can be achieved either through the REST API or the Databricks Command Line Interface (CLI).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access Tokens
# MAGIC 
# MAGIC Access tokens provide programmatic access to the Databricks CLI and REST API.  This lesson uses the REST API but could also be completed <a href="https://docs.azuredatabricks.net/user-guide/dev-tools/databricks-cli.html" target="_blank">using the command line alternative.</a>
# MAGIC 
# MAGIC To get started, first generate an access token.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC In order to generate a token:<br><br>
# MAGIC 
# MAGIC 1. Click on the person icon in the upper-right corner of the screen.
# MAGIC 2. Click **User Settings**
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/token-1.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 3. Click on **Access Tokens**
# MAGIC 4. Click on **Generate New Token**
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/token-2-azure.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC 5. Name your token
# MAGIC 6. Designate a lifespan (a shorter lifespan is generally better to minimize risk exposure)
# MAGIC 7. Click **Generate**
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/token-3.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 8. Copy your token.  You'll only be able to see it once.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/token-4.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Be sure to keep this key secure.  This grants the holder full programmatic access to Databricks, including both resources and data that's available to your Databricks environment.

# COMMAND ----------

# MAGIC %md
# MAGIC Paste your token into the following cell along with the domain of your Databricks deployment (you can see this in the notebook's URL).  The deployment should look something like `https://westus2.azuredatabricks.net`

# COMMAND ----------

# TODO
import base64

token = b"FILL IN"
domain = "https://<REGION>.azuredatabricks.net" + "/api/2.0/"

header = {"Authorization": b"Basic " + base64.standard_b64encode(b"token:" + token)}

# COMMAND ----------

# MAGIC %md
# MAGIC Test that the connection works by listing all files in the root directory of DBFS.

# COMMAND ----------

try:
  import json
  import requests

  endPoint = domain+"dbfs/list?path=/"
  r = requests.get(endPoint, headers=header)

  [i.get("path") for i in json.loads(r.text).get("files")]  

except Exception as e:
  print(e)
  print("\n** Double check your previous settings **\n")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The REST API can be used at the command line using a command like `curl -s -H "Authorization: Bearer token" https://domain.cloud.databricks.com/api/2.0/dbfs/list\?path\=/`
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The CLI can be used with the command `databricks fs ls dbfs:/` once it has been installed and configured with your access token

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scheduling with the REST API and CLI
# MAGIC 
# MAGIC Jobs can either be scheduled for running on a consistent basis or they can be run every time the API call is made.  Since there are many parameters in scheduling jobs, it's often best to schedule a job through the user interface, parse the configuration settings, and then run later jobs using the API.
# MAGIC 
# MAGIC Run the following cell to get the sense of what a basic job accomplishes.

# COMMAND ----------

path = dbutils.notebook.run("./Runnable/Runnable-4", 120, {"username": getUsername(), "ranBy": "NOTEBOOK"})
display(spark.read.parquet(path))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC The notebook `Runnable-4` logs a timestamp and how the notebook is run.  This will log our jobs.
# MAGIC 
# MAGIC Schedule this job notebook as a job using parameters by first navigating to the jobs panel on the left-hand side of the screen and creating a new job.  Customize the job as follows:<br><br>
# MAGIC 
# MAGIC 1. Give the job a name
# MAGIC 2. Choose the notebook `Runnable-4` in the `Runnable` directory of this course
# MAGIC 3. Add parameters for `username`, which is your Databricks login email (this gives you a unique path to save your data), and set `ranBy` as `JOB`
# MAGIC 4. Choose a cluster of 2 workers and 1 driver (the default is too large for our needs).  **You can also choose to run a job against an already active cluster, reducing the time to spin up new resources.**
# MAGIC 5. Click **Run now** to execute the job.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/runnable-4-execution.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Set recurring jobs in the same way by adding a schedule
# MAGIC <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Set email alerts in case of job failure

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC When the job completes, paste the `Run ID` that appears under completed runs below.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> On the jobs page, you can access the logs to determine the cause of any failures.

# COMMAND ----------

try:
  runId = "FILL_IN"
  endPoint = domain + "jobs/runs/get?run_id={}".format(runId)

  json.loads(requests.get(endPoint, headers=header).text)
  
except Exception as e:
  print(e)
  print("\n** Double check your runId and domain **\n")

# COMMAND ----------

# MAGIC %md
# MAGIC Now take a look at the table to see the update

# COMMAND ----------

display(spark.read.parquet(path))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC With this design pattern, you can have full, programmatic access to Databricks.  <a href="https://docs.databricks.com/api/latest/examples.html#jobs-api-examples" target="_blank">See the documentation</a> for examples on submitting jobs from Python files and JARs and other API examples.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Always run production jobs on a new cluster to minimize the chance of unexpected behavior.  Autoscaling clusters allows for elastically allocating more resources to a job as needed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Create and Submit a Job using the REST API
# MAGIC 
# MAGIC Now that a job has been submitted through the UI, we can easily capture and re-run that job.  Re-run the job using the REST API and different parameters.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create the `POST` Request Payload
# MAGIC 
# MAGIC To create a new job, communicate the specifications about the job using a `POST` request.  First, define the following variables:<br><br>
# MAGIC 
# MAGIC * `name`: The name of your job
# MAGIC * `notebook_path`: The path to the notebook `Runnable-4`.  This will be the `noteboook_path` variable listed in the API call above.

# COMMAND ----------

# TODO
import json

name = "FILL_IN"
notebook_path = "FILL_IN"

data = {
  "name": name,
  "new_cluster": {
    "spark_version": "4.2.x-scala2.11",
    "node_type_id": "Standard_DS3_v2", # Change the instance type here
    "num_workers": 2,
    "spark_conf": {"spark.databricks.delta.preview.enabled": "true"}
  },
  "notebook_task": {
    "notebook_path": notebook_path,
    "base_parameters": {
      "username": username, "ranBy": "REST-API"
    }
  }
}

data_str = json.dumps(data)
print(data_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create the Job
# MAGIC 
# MAGIC Use the base `domain` defined above to create a URL for the REST endpoint `jobs/create`.  Then, submit a `POST` request using `data_str` as the payload.

# COMMAND ----------

# TODO
createEndPoint = FILL_IN
r = requests.post(createEndPoint, headers=header, data=data_str)

job_id = json.loads(r.text).get("job_id")
print(job_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Run the Job
# MAGIC 
# MAGIC Run the job using the `job_id` from above.  You'll need to submit the post request to the `RunEndPoint` URL of `jobs/run-now`

# COMMAND ----------

# TODO
RunEndPoint = FILL_IN

data2 = {"job_id": job_id}
data2_str = json.dumps(data2)

r = requests.post(RunEndPoint, headers=header, data=data2_str)

r.text

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Confirm that the Job Ran
# MAGIC 
# MAGIC Confirm that the job ran by checking the parquet file.  It can take a few minutes for the job to run and update this file.

# COMMAND ----------

display(spark.read.parquet(path))

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.sql.functions import col

APICounts = (spark.read.parquet(path)
  .filter(col("ranBy") == "REST-API")
  .count()
)

if APICounts > 0:
  print("Tests passed!")
else:
  print("Test failed, no records found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What ways can you schedule jobs on Databricks?  
# MAGIC **Answer:** Jobs can be scheduled using the UI, REST API, or Databricks CLI.
# MAGIC 
# MAGIC **Question:** How can you gain programmatic access to Databricks?  
# MAGIC **Answer:** Generating a token will give programmatic access to most Databricks services.

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
# MAGIC Start the next lesson, [Job Failure]($./ETL3 05 - Job Failure ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I get more information on the REST API?  
# MAGIC **A:** Check out the <a href="https://docs.azuredatabricks.net/api/index.html" target="_blank">Databricks documentation.</a>
# MAGIC 
# MAGIC **Q:** How can I set up the Databricks CLI?  
# MAGIC **A:** Check out the <a href="https://docs.azuredatabricks.net/user-guide/dev-tools/databricks-cli.html#set-up-the-cli" target="_blank">Databricks documentation for step-by-step instructions.</a>
# MAGIC 
# MAGIC **Q:** How can I do a `spark-submit` job using the API?  
# MAGIC **A:** Check out the <a href="https://docs.azuredatabricks.net/api/latest/examples.html#spark-submit-api-example" target="_blank">Databricks documentation for API examples.</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>