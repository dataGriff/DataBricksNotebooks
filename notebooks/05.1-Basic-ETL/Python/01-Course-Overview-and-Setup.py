# Databricks notebook source
# MAGIC %md
# MAGIC # Course Overview and Setup
# MAGIC ## ETL Part 1: Data Extraction
# MAGIC 
# MAGIC In this course data engineers access data where it lives and then apply data extraction best practices, including schemas, corrupt record handling, and parallelized code. By the end of this course, you will extract data from multiple sources, use schema inference and apply user-defined schemas, and navigate Databricks and Spark documents to source solutions.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### ETL with Azure Databricks and Spark
# MAGIC 
# MAGIC The **extract, transform, load (ETL)** process takes data from one or more sources, transforms it, normally by adding structure, and then loads it into a target database. 
# MAGIC 
# MAGIC A common ETL job takes log files from a web server, parses out pertinent fields so it can be readily queried, and then loads it into a database.
# MAGIC 
# MAGIC ETL may seem simple: applying structure to data so it’s in a desired form. However, the complexity of ETL is in the details. Data Engineers building ETL pipelines must understand and apply the following concepts:<br><br>
# MAGIC 
# MAGIC * Optimizing data formats and connections
# MAGIC * Determining the ideal schema
# MAGIC * Handling corrupt records
# MAGIC * Automating workloads
# MAGIC 
# MAGIC This course addresses these concepts.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/ETL-overview.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>
# MAGIC 
# MAGIC Stay tuned for upcoming courses which will cover:<br><br>
# MAGIC 
# MAGIC * Complex and performant data transformations
# MAGIC * Schema changes over time  
# MAGIC * Recovery from job failures
# MAGIC * Avoiding duplicate records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC Create a notebook and Spark cluster.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1
# MAGIC Databricks notebooks are backed by clusters, or networked computers, that process data. Create a Spark cluster (*if you already have a running cluster, skip to **Step 3** *):
# MAGIC 1. Select the **Clusters** icon in the sidebar.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-cluster-4.png" style="height: 200px; margin: 20px"/></div>
# MAGIC 2. Click the **Create Cluster** button.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-cluster-5.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC 3. Name your cluster. Use your name or initials to easily differentiate your cluster from your coworkers.
# MAGIC 4. Select the cluster type. We recommend the latest Databricks runtime (**3.3**, **3.4**, etc.) and Scala **2.11**.
# MAGIC 5. Specify your cluster configuration.
# MAGIC   * For clusters created on a **Community Edition** shard the default values are sufficient for the remaining fields.
# MAGIC   * For all other shards, please refer to your company's policy on private clusters.</br></br>
# MAGIC 6. Click the **Create Cluster** button.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-cluster-2.png" style="height: 300px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC 
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Check with your local system administrator to see if there is a recommended default cluster at your company to use for the rest of the class. This could save you  money!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2
# MAGIC 
# MAGIC Create a new notebook in your home folder:
# MAGIC 1. Select the **Home** icon in the sidebar.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/home.png" style="height: 200px; margin: 20px"/></div>
# MAGIC 2. Right-click your home folder.
# MAGIC 3. Select **Create**.
# MAGIC 4. Select **Notebook**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-notebook-1.png" style="height: 150px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC 5. Name your notebook `My Notebook`.<br/>
# MAGIC 6. Set the language to **Python**.<br/>
# MAGIC 7. Select the cluster to attach this Notebook.  
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If a cluster is not currently running, this option will not exist.
# MAGIC 8. Click **Create**.
# MAGIC <div>
# MAGIC   <div style="float:left"><img src="https://files.training.databricks.com/images/eLearning/create-notebook-2b.png" style="width:400px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC   <div style="float:left; margin-left: e3m; margin-right: 3em">or</div>
# MAGIC   <div style="float:left"><img src="https://files.training.databricks.com/images/eLearning/create-notebook-2.png" style="width:400px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC   <div style="clear:both"></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 3
# MAGIC 
# MAGIC Now  you have a notebook, use it to run code.
# MAGIC 1. In the first cell of your notebook, type `1 + 1`. 
# MAGIC 2. Run the cell: Click the **Run** icon and then select **Run Cell**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/run-notebook-1.png" style="width:600px; margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> **Ctrl-Enter** also runs a cell.

# COMMAND ----------

1 + 1

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Attach and Run
# MAGIC 
# MAGIC If your notebook was not previously attached to a cluster you might receive the following prompt: 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/run-notebook-2.png" style="margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC 
# MAGIC If you click **Attach and Run**, first make sure that you are attaching to the correct cluster.
# MAGIC 
# MAGIC If it is not the correct cluster, click **Cancel** instead see the next cell, **Attach & Detach**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Attach & Detach
# MAGIC 
# MAGIC If your notebook is detached you can attach it to another cluster:  
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/attach-to-cluster.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/>
# MAGIC 
# MAGIC If your notebook is attached to a cluster you can:
# MAGIC * Detach your notebook from the cluster.
# MAGIC * Restart the cluster.
# MAGIC * Attach to another cluster.
# MAGIC * Open the Spark UI.
# MAGIC * View the Driver's log files.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/detach-from-cluster.png" style="margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC * Click the down arrow on a folder and select the **Create Notebook** option to create notebooks.
# MAGIC * Click the down arrow on a folder and select the **Import** option to import notebooks.
# MAGIC * Select the **Attached/Detached** option directly below the notebook title to attach to a spark cluster 
# MAGIC * Create clusters using the Clusters button on the left sidebar.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC 
# MAGIC **Question:** How do you create a Notebook?  
# MAGIC **Answer:** Sign into Azure Databricks, select the **Home** icon from the sidebar, right-click your home-folder, select **Create**, and then **Notebook**. In the **Create Notebook** dialog, specify the name of your notebook and the default programming language.
# MAGIC 
# MAGIC **Question:** How do you create a cluster?  
# MAGIC **Answer:** Select the **Clusters** icon on the sidebar, click the **Create Cluster** button, specify the specific settings for your cluster and then click **Create Cluster**.
# MAGIC 
# MAGIC **Question:** How do you attach a notebook to a cluster?  
# MAGIC **Answer:** If you run a command while detached, you may be prompted to connect to a cluster. To connect to a specific cluster, open the cluster menu by clicking the **Attached/Detached** menu item and then selecting your desired cluster.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Next Steps
# MAGIC 
# MAGIC This course is available in Python and Scala.  Start the next lesson, **ETL Process Overview**.
# MAGIC 1. Click the **Home** icon in the left sidebar.
# MAGIC 2. Select your home folder.
# MAGIC 3. Select the folder **ETL-Part-1**
# MAGIC 4. Open the notebook **02-ETL-Process-Overview** in either the Python or Scala folder
# MAGIC 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/ETL-Part-1/Course-Import2-Azure.png" style="margin-bottom: 5px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; width: auto; height: auto; max-height: 383px"/>
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The Python and Scala content is identical except for the language used.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC **Q:** Are there additional docs I can reference to find my way around Azure Databricks?  
# MAGIC **A:** See <a href="https://docs.azuredatabricks.net/getting-started/index.html" target="_blank">Getting Started Guides</a>.
# MAGIC 
# MAGIC **Q:** Where can I learn more about the cluster configuration options?  
# MAGIC **A:** See <a href="https://docs.azuredatabricks.net/user-guide/clusters/index.html#id1" target="_blank">Spark Clusters on Azure Databricks</a>.
# MAGIC 
# MAGIC **Q:** Can I import formats other than .dbc files?  
# MAGIC **A:** Yes, see <a href="https://docs.azuredatabricks.net/user-guide/notebooks/notebook-manage.html#notebook-external-formats" target="_blank">Importing Notebooks</a>.