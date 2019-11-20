# Databricks notebook source
# MAGIC %md 
# MAGIC # Getting Started
# MAGIC 
# MAGIC In this lab you will learn how Azure Data Factory (ADF) v2 can be used to orchestrate the ingestion and transformation of data with Azure Databricks. You will use a Copy activity to copy a public dataset into a location accessible to your Azure Databricks cluster. Then, a Databricks notebook activity will be employed to batch process the files within that dataset, identify issues with the data, clean and format the data, and load it into Databricks global tables to support downstream analytics.
# MAGIC 
# MAGIC This notebook provides the steps required to provision the necessary pre-requisite resources in your Azure subscription to complete this lab.
# MAGIC 
# MAGIC ## Pre-Requisites
# MAGIC 
# MAGIC Prior to beginning this lab, there are a few resources you need to provision within the Azure subscription you are using for this lab.
# MAGIC 
# MAGIC 1. Gneral purpose Azure Storage account
# MAGIC 2. Azure Data Factory v2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Azure Storage 
# MAGIC 
# MAGIC For this lab, Azure Storage blobs will be used as the intermediary for the exchange of data between ADF and Azure Databricks. To facilitate this, you will need to:
# MAGIC 
# MAGIC 1. Create a general purpose Azure Storage account v1
# MAGIC 2. Acquire the Account Name and Account Key for that Storage Account 
# MAGIC 3. Create a container named `dwtemp` that will be used to store data used during the exchange.
# MAGIC 
# MAGIC #### Create Azure Storage account
# MAGIC 
# MAGIC 1. In the [Azure portal](https://portal.azure.com), select **+ Create a resource**, enter "storage account" into the Search the Marketplace box, select **Storage account - blob, file, table, queue** from the results, and select **Create**.
# MAGIC 
# MAGIC    ![In the Azure portal, +Create a resource is highlighted in the navigation pane, "storage account" is entered into the Search the Marketplace box, and Storage account - blob, file, table, queue is highlighted in the results.](https://databricksdemostore.blob.core.windows.net/images/02-SQL-DW/create-resource-storage-account.png 'Create Storage account')
# MAGIC 
# MAGIC 2. In the Create storage account blade, enter the following:
# MAGIC 
# MAGIC    - **Subscription**: Select the subscription you are using for this module.
# MAGIC    - **Resource group**: Choose your module resource group.
# MAGIC    - **Storage account name**: Enter a unique name (make sure you see a green checkbox).
# MAGIC    - **Location**: Select the location you are using for resources in this module.
# MAGIC    - **Performance**: Select Standard.
# MAGIC    - **Account kind**: Select Storage (general purpose v1).
# MAGIC    - **Replication**: Select Locally-redundant storage (LRS).
# MAGIC 
# MAGIC    ![The Create storage account blade is displayed, with the previously mentioned settings entered into the appropriate fields.](https://databricksdemostore.blob.core.windows.net/images/02-SQL-DW/storage-account-create-new.png 'Create storage account')
# MAGIC 
# MAGIC 3. Select **Next: Advanced >**.
# MAGIC 
# MAGIC 4. In the Advanced tab, select the following:
# MAGIC 
# MAGIC    - **Secure transfer required**: Select Disabled
# MAGIC    - **Virtual network**: Select None
# MAGIC 
# MAGIC    ![The Create storage account blade is displayed with options under the Advanced tab.](https://databricksdemostore.blob.core.windows.net/images/02-SQL-DW/storage-account-create-new-advanced.png 'Create storage account - Advanced')
# MAGIC 
# MAGIC 5. Select **Review + create**.
# MAGIC 
# MAGIC 6. In the Review tab, select **Create**.
# MAGIC 
# MAGIC #### Acquire account name and key
# MAGIC 
# MAGIC Once provisioned, navigate to your storage account, select **Access keys** from the left-hand menu, and copy the **Storage account name** and **key1** Key value into a text editor, such as Notepad, for later use.
# MAGIC 
# MAGIC    ![Copy the storage account name and key1 from the Keys blade.](https://databricksdemostore.blob.core.windows.net/images/02-SQL-DW/storage-account-keys.png 'Storage account keys')
# MAGIC    
# MAGIC #### Create the dwtemp container
# MAGIC 
# MAGIC Select **Blobs** from the left-hand menu, then select **+ container** to create a new container. Enter `dwtemp` for the container name, leave the public access level selected as Private, then select **OK**.
# MAGIC 
# MAGIC    ![Create new container.](https://databricksdemostore.blob.core.windows.net/images/02-SQL-DW/storage-account-create-container.png 'New container')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Azure Data Factory (ADF) v2
# MAGIC [ADF v2](https://docs.microsoft.com/en-us/azure/data-factory/) will be used to orchestrate the ingestion and transformation of data in this lab. For this, you will need to:
# MAGIC 
# MAGIC 1. Create an ADF v2 instance in your Azure account by opening a new browser window or tab and navigating to the [Azure Portal](https://portal.azure.com) (https://portal.azure.com).
# MAGIC 
# MAGIC 2. Once there, select **+ Create a resource**, type "data factory" into the search bar, select **Data Factory** in the results, and then select **Create**.
# MAGIC 
# MAGIC   ![Select create a resource, type in Data Factory, then select Data Factory from the results list](https://databricksdemostore.blob.core.windows.net/images/03/01/create-resource-adf.png "Create Data Factory")
# MAGIC 
# MAGIC 3.  Set the following configuration on the Data Factory creation form:
# MAGIC 
# MAGIC     - **Name**: Enter a globally unique name, as indicated by a green checkmark.
# MAGIC     - **Subscription**: Select the subscription you are using for this workshop.
# MAGIC     - **Resource Group**: Choose Use existing, and select the resource group for this workshop.
# MAGIC     - **Version**: V2
# MAGIC     - **Location**: Select a region.
# MAGIC     
# MAGIC     ![Complete the Azure Data Factory creation form with the options as outlined above.](https://databricksdemostore.blob.core.windows.net/images/03/01/adf-create-new.png "Create New Data Factory")
# MAGIC 
# MAGIC 4.  Select **Create** to provision ADF v2.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC 
# MAGIC Start the next lesson, [Data Ingestion]($./02-Data-Ingestion)