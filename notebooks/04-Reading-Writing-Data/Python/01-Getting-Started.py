# Databricks notebook source
# MAGIC %md
# MAGIC # Getting Started on Azure Databricks
# MAGIC 
# MAGIC Azure Databricks&reg; provides a notebook-oriented Apache Spark&trade; as-a-service workspace environment, making it easy to manage clusters and explore data interactively.
# MAGIC 
# MAGIC ### Use cases for Apache Spark 
# MAGIC * Read and process huge files and data sets
# MAGIC * Query, explore, and visualize data sets
# MAGIC * Join disparate data sets found in data lakes
# MAGIC * Train and evaluate machine learning models
# MAGIC * Process live streams of data
# MAGIC * Perform analysis on large graph data sets and social networks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC Create a notebook and Spark cluster.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** This step will require you navigate around Databricks while doing this lesson.  We recommend you <a href="" target="_blank">open a second browser window</a> when navigating around Databricks.  This way you can view these instructions in one window and navigate in another.
# MAGIC 
# MAGIC ### Step 1
# MAGIC Databricks notebooks are backed by clusters, or networked computers that work together to process your data. Create a Spark cluster (*if you already have a running cluster, skip to **Step 2** *):
# MAGIC 1. In your new window, click the **Clusters** button in the sidebar.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-cluster-4.png" style="height: 200px"/></div><br/>
# MAGIC 2. Click the **Create Cluster** button.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-cluster-5.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div><br/>
# MAGIC 3. Name your cluster. Use your name or initials to easily differentiate your cluster from your coworkers.
# MAGIC 4. Select the Databricks Runtime version. To complete all the lessons in this module, use the latest runtime (**5.1** or newer) and Scala **2.11**.
# MAGIC 5. Select **3** as the Python version.
# MAGIC 5. Specify your cluster configuration.
# MAGIC   * For clusters created on a **Community Edition** shard the default values are sufficient for the remaining fields.
# MAGIC   * For all other environments, please refer to your company's policy on creating and using clusters.</br></br>
# MAGIC 6. Right click on **Cluster** button on left side and open a new tab. Click the **Create Cluster** button.
# MAGIC <div><img src="https://databricksdemostore.blob.core.windows.net/images/04/01/create-cluster.png" style="height: 300px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Check with your local system administrator to see if there is a recommended default cluster at your company to use for the rest of the class. This could save you some money!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2
# MAGIC Create a new notebook in your home folder:
# MAGIC 1. Click the **Home** button in the sidebar.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/home.png" style="height: 200px"/></div><br/>
# MAGIC 2. Right-click on your home folder.
# MAGIC 3. Select **Create**.
# MAGIC 4. Select **Notebook**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/create-notebook-1.png" style="height: 150px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div><br/>
# MAGIC 5. Name your notebook `First Notebook`.<br/>
# MAGIC 6. Set the language to **Python**.<br/>
# MAGIC 7. Select the cluster to which to attach this Notebook.  
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If a cluster is not currently running, this option will not exist.
# MAGIC 8. Click **Create**.
# MAGIC <div>
# MAGIC   <div style="float:left"><img src="https://files.training.databricks.com/images/eLearning/create-notebook-2b.png" style="width:400px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC   <div style="float:left">&nbsp;&nbsp;&nbsp;or&nbsp;&nbsp;&nbsp;</div>
# MAGIC   <div style="float:left"><img src="https://files.training.databricks.com/images/eLearning/create-notebook-2.png" style="width:400px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC   <div style="clear:both"></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 3
# MAGIC 
# MAGIC Now that you have a notebook, we can use it to run some code.
# MAGIC 1. In the first cell of your notebook, type `1 + 1`. 
# MAGIC 2. Run the cell by clicking the run icon and selecting **Run Cell**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/run-notebook-1.png" style="width:600px; margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You can also run a cell by typing **Ctrl-Enter**.

# COMMAND ----------

1 + 1

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Attach and Run
# MAGIC 
# MAGIC If your notebook was not previously attached to a cluster you might receive the following prompt: 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/run-notebook-2.png" style="margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC 
# MAGIC If you click **Attach and Run**, first make sure that you are attaching to the correct cluster.
# MAGIC 
# MAGIC If it is not the correct cluster, click **Cancel** instead see the next cell, **Attach & Detach**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Attach & Detach
# MAGIC 
# MAGIC If your notebook is detached you can attach it to another cluster:  
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/attach-to-cluster.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <br/>
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
# MAGIC * Create notebooks by clicking the down arrow on a folder and selecting the **Create Notebook** option.
# MAGIC * Import notebooks by clicking the down arrow on a folder and selecting the **Import** option.
# MAGIC * Attach to a spark cluster by selecting the **Attached/Detached** option directly below the notebook title.
# MAGIC * Create clusters using the Clusters button on the left sidebar.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Questions
# MAGIC 
# MAGIC **Q:** How do you create a Notebook?  
# MAGIC **A:** Sign into Databricks, select the **Home** icon from the sidebar, right-click your home-folder, select **Create**, and then **Notebook**. In the **Create Notebook** dialog, specify the name of your notebook and the default programming language.
# MAGIC 
# MAGIC **Q:** How do you create a cluster?  
# MAGIC **A:** Select the **Clusters** icon on the sidebar, click the **Create Cluster** button, specify the specific settings for your cluster and then click **Create Cluster**.
# MAGIC 
# MAGIC **Q:** How do you attach a notebook to a cluster?  
# MAGIC **A:** If you run a command while detached, you may be prompted to connect to a cluster. To connect to a specific cluster, open the cluster menu by clicking the **Attached/Detached** menu item and then selecting your desired cluster.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, **Querying Files with DataFrames**.
# MAGIC 1. In the left sidebar, click **Home**.
# MAGIC 2. Select your home folder.
# MAGIC 3. Select the folder **DataFrames-(version #)**
# MAGIC 4. Open the notebook **02-Querying-Files** by single-clicking on it (you'll be working the rest of the course from within your Databricks account)
# MAGIC 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/main-menu-4.png" style="margin-bottom: 5px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; width: auto; height: auto; max-height: 383px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC **Q:** Are there additional docs I can reference to find my way around Databricks?  
# MAGIC **A:** See <a href="https://docs.azuredatabricks.net/getting-started/index.html" target="_blank">Getting Started with Databricks</a>.
# MAGIC 
# MAGIC **Q:** Where can I learn more about the cluster configuration options?  
# MAGIC **A:** See <a href="https://docs.azuredatabricks.net/user-guide/clusters/index.html#id1" target="_blank">Spark Clusters on Databricks</a>.
# MAGIC 
# MAGIC **Q:** Can I import formats other than .dbc files?  
# MAGIC **A:** Yes, see <a href="https://docs.azuredatabricks.net/user-guide/notebooks/notebook-manage.html#notebook-external-formats" target="_blank">Importing Notebooks</a>.
# MAGIC 
# MAGIC **Q:** Can I install the courseware notebooks into a non-Databricks distribution of Spark?  
# MAGIC **A:** No, the files that contain the courseware are in a Databricks specific format (DBC).