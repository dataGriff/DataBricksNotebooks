# Databricks notebook source
# MAGIC %md 
# MAGIC # Data ingestion via Azure Data Factory
# MAGIC 
# MAGIC In this notebook, you will create an Azure Data Factory (ADF) v2 pipeline to ingest data from a public dataset into your Azure Storage account.
# MAGIC 
# MAGIC In this lesson you will complete the following:
# MAGIC * Create a copy data pipeline.
# MAGIC * Monitor the pipeline run.
# MAGIC * Verify copied files exist in Blob storage.
# MAGIC * Examine the ingested data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a copy data pipeline
# MAGIC 
# MAGIC We will start the lab by using the Copy Data Wizard to create a new ADF pipeline using the Azure Data Factory UI. The wizard handles creating Linked Services, Data Sources, and the Copy Activity for you.
# MAGIC 
# MAGIC To learn more about the various components of ADF, you can visit the following resources:
# MAGIC 
# MAGIC * [Pipeline and actvities](https://docs.microsoft.com/en-us/azure/data-factory/concepts-pipelines-activities)
# MAGIC * [Dataset and linked services](https://docs.microsoft.com/en-us/azure/data-factory/concepts-datasets-linked-services)
# MAGIC * [Pipeline execution and triggers](https://docs.microsoft.com/en-us/azure/data-factory/concepts-pipeline-execution-triggers)
# MAGIC * [Integration runtime](https://docs.microsoft.com/en-us/azure/data-factory/concepts-integration-runtime)
# MAGIC 
# MAGIC In the [Azure portal](https://portal.azure.com/), navigate to the ADF instance you provisioned in the Getting Started notebook, and then launch the Azure Data Factory UI by select the **Author & Monitor** tile on the ADF Overview blade.
# MAGIC 
# MAGIC ![Author & Monitor](https://databricksdemostore.blob.core.windows.net/images/03/02/adf-author-and-deploy.png "Author & Monitor")
# MAGIC 
# MAGIC On the ADF UI landing page, select **Copy data**.
# MAGIC 
# MAGIC ![Copy data](https://databricksdemostore.blob.core.windows.net/images/03/02/adf-copy-data.png "Copy pipeline")
# MAGIC 
# MAGIC ### Step 1: Properties
# MAGIC 
# MAGIC On the Properties page of the Copy Data wizard, do the following:
# MAGIC 
# MAGIC 1. **Task name**: Enter a name, such as LabPipeline.
# MAGIC 2. **Task cadence or Task schedule**: Choose Run once now.
# MAGIC 3. Select **Next**.
# MAGIC 
# MAGIC ![Copy Data wizard properties](https://databricksdemostore.blob.core.windows.net/images/03/02/adf-copy-data-step-1.png "Copy Data wizard properties")
# MAGIC 
# MAGIC ### Step 2: Source
# MAGIC 
# MAGIC The source will be configured to point to a publicly accessible Azure Storage account, which contains the files you will be copying into your own storage account.
# MAGIC 
# MAGIC On the Source data store page, select **+ Create new connection**.
# MAGIC 
# MAGIC ![Create new connection](https://databricksdemostore.blob.core.windows.net/images/03/02/adf-create-new-connection.png "Create new connection")
# MAGIC 
# MAGIC On the New Linked Service blade:
# MAGIC 
# MAGIC 1. Enter "storage" into the search box.
# MAGIC 2. Select **Azure Blob Storage**.
# MAGIC 3. Select **Continue**.
# MAGIC 
# MAGIC ![Add Azure Blob Storage Linked Service](https://databricksdemostore.blob.core.windows.net/images/03/02/adf-linked-service-azure-blob-storage.png "Add Azure Blob Storage Linked Service")
# MAGIC 
# MAGIC Configure the New Linked Service (Azure Blob Storage) with the following values:
# MAGIC 
# MAGIC 1. **Name**: Enter PublicDataset
# MAGIC 2. **Authentication method**: Select Use SAS URI
# MAGIC 3. **SAS URI**: Paste the following URI into the field: <https://databricksdemostore.blob.core.windows.net/?sv=2017-11-09&ss=b&srt=sco&sp=rl&se=2099-12-31T17:59:59Z&st=2018-09-22T15:21:51Z&spr=https&sig=LqDcqVNGNEuWILjNJoThzaXktV2N%2BFS354s716RJo80%3D>
# MAGIC 
# MAGIC 4. Select **Test connection** and ensure a Connection successful message is displayed
# MAGIC 5. Select **Finish**
# MAGIC 
# MAGIC ![Configure Azure Blob Storage Linked Service for public dataset](https://databricksdemostore.blob.core.windows.net/images/03/02/adf-linked-service-public-dataset.png "Configure Azure Blob Storage Linked Service for public dataset")
# MAGIC 
# MAGIC Back on the Source data source page, ensure **PublicDataset** is selected, and then select **Next**.
# MAGIC 
# MAGIC ![Source data store](https://databricksdemostore.blob.core.windows.net/images/03/02/adf-source-data-store.png "Source data store")
# MAGIC 
# MAGIC On the Choose the input file or folder page:
# MAGIC 
# MAGIC 1. **File or folder**: Use the **Browse** button to select the folder **training/crime-data-2016/**. *Do not enter the path as text, you must use the browse button due to a bug in some versions of the interface*
# MAGIC 2. **Copy file recursively**: Check this box.
# MAGIC 3. **Binary Copy**: Check this box.
# MAGIC 4. Select **Next**.
# MAGIC 
# MAGIC ![Choose the input file or folder](https://databricksdemostore.blob.core.windows.net/images/03/02/adf-choose-input-file-or-folder.png "Choose the input file or folder")
# MAGIC 
# MAGIC ### Step 3: Destination
# MAGIC 
# MAGIC The destination data store will be configured to point to the Azure Storage account you created in the [Getting Started notebook]($./01-Getting-Started.dbc) within this lab.
# MAGIC 
# MAGIC On the Destination data store page, select **+ Create new connection**.
# MAGIC 
# MAGIC ![Create new connection](https://databricksdemostore.blob.core.windows.net/images/03/02/adf-create-new-connection.png "Create new connection")
# MAGIC 
# MAGIC As you did previously, enter "storage" into the search box on the New Linked Service blade, choose **Azure Blob Storage** for the Linked Service and select **Continue**.
# MAGIC 
# MAGIC Configure the New Linked Service (Azure Blob Storage) with the following values:
# MAGIC 
# MAGIC 1. **Name**: Enter DestinationContainer.
# MAGIC 2. **Authentication method**: Select Use account key.
# MAGIC 3. **Storage account name**: Select the name of the Storage account you created in the Getting Started notebook from the list.
# MAGIC 4. Select **Test connection** and ensure a Connection successful message is displayed.
# MAGIC 5. Select **Finish**.
# MAGIC 
# MAGIC ![Configure Azure Blob Storage Linked Service for destination container](https://databricksdemostore.blob.core.windows.net/images/03/02/adf-linked-service-destination-container.png "Configure Azure Blob Storage Linked Service for destination container")
# MAGIC 
# MAGIC Back on the Destination data store view, ensure **DestinationContainer** is selected, and then select **Next**.
# MAGIC 
# MAGIC On the Choose the output file or folder page:
# MAGIC 
# MAGIC 1. **Folder path**: Enter **dwtemp/03.02/**.
# MAGIC 2. **File name**: Leave empty.
# MAGIC 3. **Copy behavior**: Select **Preserve hierarchy**.
# MAGIC 4. Select **Next**.
# MAGIC 
# MAGIC ![Choose the output file or folder](https://databricksdemostore.blob.core.windows.net/images/03/02/adf-choose-output-file-or-folder.png "Choose the output file or folder")
# MAGIC 
# MAGIC ### Step 4: Settings
# MAGIC 
# MAGIC On the Settings page, accept the default values, and select **Next**.
# MAGIC 
# MAGIC ### Step 5: Summary
# MAGIC 
# MAGIC On the Summary page, you can review the copy pipeline settings, and then select **Next** to deploy the pipeline.
# MAGIC 
# MAGIC ### Step 6: Deployment
# MAGIC 
# MAGIC The Deployment page displays the status of the pipeline deployment. This will create the Datasets and Pipeline, and then run the pipeline. Select **Monitor** to view the pipeline progress.
# MAGIC 
# MAGIC   ![Pipeline Deployment](https://databricksdemostore.blob.core.windows.net/images/03/02/adf-deployment.png "Pipeline Deployment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor the pipeline run
# MAGIC 
# MAGIC Selecting Monitor above will take you to the Pipeline Runs screen in the ADF UI, where you can monitor the status of your pipeline runs. Using the monitor dialog, you can track the completion status of pipeline runs, and access other details about the run.
# MAGIC 
# MAGIC   ![Monitor pipeline runs](https://databricksdemostore.blob.core.windows.net/images/03/02/adf-monitor-pipeline-runs.png "Monitor pipeline runs")
# MAGIC   
# MAGIC > NOTE: You may need to select Refresh if you don't see the pipeline listed, or to update the status displayed.
# MAGIC 
# MAGIC You can select the View Activity Runs icon under Actions if you want to see the progress if the individual activities that make up the pipeline. On the Activity Runs dialog, you can select the various icons under Actions to display the inputs, outputs, and run details.
# MAGIC 
# MAGIC   ![Monitor activity runs](https://databricksdemostore.blob.core.windows.net/images/03/02/adf-monitor-activity-runs.png "Monitor activity runs")
# MAGIC 
# MAGIC When the copy activity is completed, its status will change to Succeeded (requires refreshing the activities list).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify files in blob storage
# MAGIC 
# MAGIC Now, navigate to your storage account in the Azure portal, and locate the `dwtemp` container, and the `03.02` folder within it. Observe the files copied via ADF.
# MAGIC 
# MAGIC   ![Copied files in Blob storage](https://databricksdemostore.blob.core.windows.net/images/03/02/blob-storage-files.png "Blob storage files")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Examine the ingested data
# MAGIC 
# MAGIC Quickly examine a few of the crime datasets ingested by the above operation, and observe some of the differences in the datasets from each city.
# MAGIC 
# MAGIC First, run the cell below to create a direct connection to your Blob Storage account, replacing the values of `storageAccountName` and `storageAccountKey` with the appropriate values from your storage account. You retrieved these values in the [Getting Started notebook]($./01-Getting-Started).
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/>Remember to attach your notebook to a cluster before running any cells in your notebook. In the notebook's toolbar, select the drop down arrow next to Detached, then select your cluster under Attach to.
# MAGIC 
# MAGIC ![Attached to cluster](https://databricksdemostore.blob.core.windows.net/images/03/03/databricks-cluster-attach.png)

# COMMAND ----------

containerName = "dwtemp"
storageAccountName = "[your-storage-account-name]"
storageAccountKey = "[your-storage-account-key]"

spark.conf.set(
  "fs.azure.account.key.%(storageAccountName)s.blob.core.windows.net" % locals(),
  storageAccountKey)

connectionString = "wasbs://%(containerName)s@%(storageAccountName)s.blob.core.windows.net/03.02" % locals()

# COMMAND ----------

# MAGIC %md
# MAGIC Start by reviewing the `crime-data-2016` parquet files copied from the public storage account.
# MAGIC 
# MAGIC In the below cell, replace `[your-storage-account-name]` with the name of your storage account and then run the cell. This will list the parquet files, containing crime data for Boston, New York, and other cities.

# COMMAND ----------

# MAGIC %fs ls wasbs://dwtemp@[your-storage-account-name].blob.core.windows.net/03.02

# COMMAND ----------

# MAGIC %md
# MAGIC The next step is to examine the data for a few cities closer by creating a DataFrame for each file.
# MAGIC 
# MAGIC Start by creating a DataFrame for the New York and Boston data.

# COMMAND ----------

crimeDataNewYorkDf = spark.read.parquet("%(connectionString)s/Crime-Data-New-York-2016.parquet" % locals())

# COMMAND ----------

crimeDataBostonDf = spark.read.load("%(connectionString)s/Crime-Data-Boston-2016.parquet" % locals())

# COMMAND ----------

# MAGIC %md
# MAGIC With the two DataFrames created, it is now possible to review the first couple records of each file.

# COMMAND ----------

display(crimeDataNewYorkDf)

# COMMAND ----------

display(crimeDataBostonDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Same Type of Data, Different Structure
# MAGIC 
# MAGIC Notice in the examples above:
# MAGIC * The `crimeDataNewYorkDF` and `crimeDataBostonDF` DataFrames use different names for the columns.
# MAGIC * The data itself is formatted differently and different names are used for similar concepts.
# MAGIC 
# MAGIC This is common when pulling data from disparate data sources.
# MAGIC 
# MAGIC In the next lesson, we will use an ADF Databricks Notebooks activity to perform data cleanup and extract homicide statistics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Data Transformation]($./03-Data-Transformation)