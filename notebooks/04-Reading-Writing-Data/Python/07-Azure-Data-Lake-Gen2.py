# Databricks notebook source
# MAGIC %md
# MAGIC # Querying Azure Data Lake Storage Gen2
# MAGIC 
# MAGIC Azure Data Lake Storage Gen2 (ADLS Gen2) is a set of capabilities dedicated to big data analytics, built on Azure Blob storage. Data Lake Storage Gen2 is the result of converging the capabilities of Microsoft's two existing storage services, Azure Blob storage and Azure Data Lake Storage Gen1. Features from Azure Data Lake Storage Gen1, such as file system semantics, directory, and file level security and scale are combined with the low-cost, tiered storage, and high availability/disaster recovery capabilities from Azure Blob storage.
# MAGIC 
# MAGIC > **Note**: Azure Data Lake Storage Gen2 is in Public Preview.
# MAGIC 
# MAGIC ADLS Gen2 makes Azure Storage the foundation for building enterprise data lakes on Azure. Designed from the start to service multiple petabytes of information while sustaining hundreds of gigabits of throughput, ADLS Gen2 allows you to easily manage massive amounts of data.
# MAGIC 
# MAGIC This lesson illustrates how to provision an ADLS Gen2 instance and use the [Azure Blob File Stystem (ABFS) driver](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-abfs-driver) built into the Databricks Runtime to query data stored in ADLS Gen2.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * An Azure Databricks cluster running Databricks Runtime 5.1 or above. Azure Databricks integration with Azure Data Lake Storage Gen2 is **fully supported in Databricks Runtime 5.1**. You can use Azure Data Lake Storage Gen2 with Databricks Runtime 4.2 to 5.0, however support is limited and we strongly recommend that you upgrade your clusters to 5.1.
# MAGIC * **IMPORTANT**: To complete the OAuth 2.0 access components of this module you must:
# MAGIC   * Have a cluster running Databricks Runtime 5.1 and above.
# MAGIC   * Have permissions within your Azure subscription to create an App Registration and service principal within Azure Active Directory.
# MAGIC * Lesson: <a href="$./06-Data-Lakes">Querying Data Lakes with DataFrames</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Azure Data Lake Storage Gen2 (ADLS Gen2)
# MAGIC 
# MAGIC To get started, you first need to provision an Azure Data Lake Storage Gen2 account in Azure.
# MAGIC 
# MAGIC 1. In the [Azure portal](https://portal.azure.com), select **+ Create a resource**, enter "storage account" into the Search the Marketplace box, select **Storage account - blob, file, table, queue** from the results, and then select **Create**.
# MAGIC 
# MAGIC    ![In the Azure portal, +Create a resource is highlighted in the navigation pane, "storage account" is entered into the Search the Marketplace box, and Storage account - blob, file, table, queue is highlighted in the results.](https://databricksdemostore.blob.core.windows.net/images/04/07/create-resource-adls-gen2.png 'Create Azure Data Lake Storage Gen2')
# MAGIC 
# MAGIC 2. On the Create storage account blade's Basics tab, enter the following:
# MAGIC 
# MAGIC    - **Subscription**: Select the subscription you are using for this module.
# MAGIC    - **Resource group**: Choose your module resource group.
# MAGIC    - **Storage account name**: Enter a globally unique name (indicated by a green check mark).
# MAGIC    - **Location**: Select the location you are using for resources in this module.
# MAGIC    - **Performance**: Select Standard.
# MAGIC    - **Account kind**: Select StorageV2 (general purpose v2).
# MAGIC    - **Replication**: Choose Locally-redundant storage (LRS).
# MAGIC    - **Access tier (default)**: Select Hot.
# MAGIC 
# MAGIC    ![The Create storage account blade's Basics tab is displayed, with the previously mentioned settings entered into the appropriate fields.](https://databricksdemostore.blob.core.windows.net/images/04/07/create-storage-account-basics.png 'New Data Lake Storage Gen2')
# MAGIC 
# MAGIC 3. Select **Next : Advanced >** to move on to the Advanced tab.
# MAGIC 
# MAGIC 4. On the Advanced tab, set the Hierarchical namespace option to **Enabled** under Data Lake Storage Gen2 (Preview), and then select **Review + create**
# MAGIC 
# MAGIC   ![The Create storage account blade's Advanced tab is displayed, with Enabled selected and highlighted next to Hierarchical namespace under Data Lake Storage Gen2 (Preview).](https://databricksdemostore.blob.core.windows.net/images/04/07/create-storage-account-advanced.png 'Enable Hierarchical namespace')
# MAGIC 
# MAGIC 5. On the Review + create tab, ensure the **Validation passed** message is displayed, and select **Create** to provision the new ADLS Gen2 instance.
# MAGIC 
# MAGIC   ![The Create storage account blade's Review + create tab is displayed, with the validation passed message present.](https://databricksdemostore.blob.core.windows.net/images/04/07/create-storage-account-review.png 'Review and create storage account')
# MAGIC 
# MAGIC 6. Navigate to the newly provisioned ADLS Gen2 account in the Azure portal, then select **Access keys** under Settings on the left-hand menu and do the following:
# MAGIC 
# MAGIC   - Copy the **Storage account name** value and set the value of the `adlsGen2AccountName` variable in the cell below to the copied value.
# MAGIC   
# MAGIC   - Copy the key1 **Key** value and set the value fo the `adlsGen2Key` variable in the cell below to the copied value.
# MAGIC     
# MAGIC       ![The storage account Access keys blade is displayed, with the storage account name and key highlighted.](https://databricksdemostore.blob.core.windows.net/images/04/07/storage-account-access-keys.png 'Storage account access keys')
# MAGIC       
# MAGIC 7. Execute the following cell to set the account name and key variables.

# COMMAND ----------

adlsGen2AccountName = "<your-adls-gen2-account-name>"
adlsGen2Key = "<your-adls-gen2-access-key>"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect your Databricks Workspace to your ADLS Gen2 account
# MAGIC 
# MAGIC There are three supported methods for connecting Databricks to ADLS Gen2:
# MAGIC 
# MAGIC 1. Direct access with a Shared Key.
# MAGIC 2. Direct access with OAuth.
# MAGIC 3. Mounting using OAuth.
# MAGIC 
# MAGIC To begin, we will use the access key you copied above to set up direct access to your ADLS Gen2 account. Using that, we will copy files into your ADLS Gen2 account and quickly examine those files. After that, we will then demonstrate how to configure OAuth access.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Direct access with a Shared Key
# MAGIC 
# MAGIC Execute the cell below to add the required Spark configuration, containing the connection details for your ADLS Gen2 account.

# COMMAND ----------

spark.conf.set("fs.azure.account.key." + adlsGen2AccountName + ".dfs.core.windows.net", adlsGen2Key)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize a file system
# MAGIC 
# MAGIC A fundamental part of ADLS Gen2 is the addition of a [hierarchical namespace](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace) to Blob storage. The hierarchical namespace organizes objects/files into a hierarchy of directories for efficient data access. A common object store naming convention uses slashes in the name to mimic a hierarchical directory structure. This structure becomes real with ADLS Gen2. Operations such as renaming or deleting a directory become single atomic metadata operations on the directory rather than enumerating and processing all objects that share the name prefix of the directory.
# MAGIC 
# MAGIC > **Important**: When the hierarchical namespace is enabled, Azure Blob Storage APIs are not available, which means you cannot use the `wasb` or `wasbs` scheme to access the `blob.core.windows.net` endpoint.
# MAGIC 
# MAGIC Before you can access the hierarchical namespace in your ADLS Gen2 account, you must initialize a file system. To accomplish this, run the cell below, which will create a file system named `demo`. Note the use of the Azure Blob File System (ABFS) scheme in the second line (`abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net`).

# COMMAND ----------

fileSystemName = "demo"
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://" + fileSystemName + "@" + adlsGen2AccountName + ".dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy data into ADLS Gen2
# MAGIC 
# MAGIC With a file system created, we can directly access the ADLS Gen2 account from Databricks.
# MAGIC 
# MAGIC To demonstrate this, let's copy the Crime-data-2016 dataset into your ADLS Gen2 account. This will take a few minutes to complete.

# COMMAND ----------

dbutils.fs.cp("/mnt/training/crime-data-2016", "abfss://" + fileSystemName + "@" + adlsGen2AccountName + ".dfs.core.windows.net/training/crime-data-2016", True)

# COMMAND ----------

# MAGIC %md
# MAGIC When the copy is complete, execute the following cell, which uses the `dbutils` to list the files copied into the file system.

# COMMAND ----------

dbutils.fs.ls("abfss://" + fileSystemName + "@" + adlsGen2AccountName + ".dfs.core.windows.net/training/crime-data-2016")

# COMMAND ----------

# MAGIC %md
# MAGIC Next, let's read the data from the New York crime data file into a DataFrame using the direct access method.

# COMMAND ----------

crimeDataNewYorkDF = spark.read.parquet("abfss://" + fileSystemName + "@" + adlsGen2AccountName + ".dfs.core.windows.net/training/crime-data-2016/Crime-Data-New-York-2016.parquet")
display(crimeDataNewYorkDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create service principal for OAuth access
# MAGIC 
# MAGIC > **IMPORTANT**: You must have permissions within your Azure subscription to create an App registration and service principal within Azure Active Directory to complete this lesson.
# MAGIC 
# MAGIC Mounting an ADLS Gen2 filesystem using Databricks Runtime 5.1 or above requires that you use OAuth 2.0 for authentication, which in turn requires creating an identity in Azure Active Directory (Azure AD) known as a service principal.
# MAGIC 
# MAGIC 1. In the [Azure portal](https://portal.azure.com), select **Azure Active Directory** from the left-hand navigation menu, select **App registrations**, and then select **+ New application registration**.
# MAGIC 
# MAGIC    ![Register new app in Azure Active Directory](https://databricksdemostore.blob.core.windows.net/images/04/06/aad-app-registration.png 'Register new app in Azure Active Directory')
# MAGIC 
# MAGIC 2. On the Create blade, enter the following:
# MAGIC 
# MAGIC   * **Name**: Enter a unique name, such as databricks-demo (this name must be unique, as indicated by a green check mark).
# MAGIC   * **Application type**: Select Web app / API.
# MAGIC   * **Sign-on URL**: Enter https://databricks-demo.com.
# MAGIC 
# MAGIC    ![Create a new app registration](https://databricksdemostore.blob.core.windows.net/images/04/06/aad-app-create.png 'Create a new app registration')
# MAGIC 
# MAGIC 3. Select **Create**.
# MAGIC 
# MAGIC 4. To access your ADLS Gen2 account from Azure Databricks you will need to provide the credentials of your newly created service principal within Databricks. On the Registered app blade that appears, copy the **Application ID** and paste it into the cell below as the value for the `clientId` variable.
# MAGIC 
# MAGIC    ![Copy the Registered App Application ID](https://databricksdemostore.blob.core.windows.net/images/04/06/registered-app-id.png 'Copy the Registered App Application ID')
# MAGIC 
# MAGIC 5. Next, select **Settings** on the Registered app blade, and then select **Keys**.
# MAGIC 
# MAGIC    ![Open Keys blade for the Registered App](https://databricksdemostore.blob.core.windows.net/images/04/06/registered-app-settings-keys.png 'Open Keys blade for the Registered App')
# MAGIC 
# MAGIC 6. On the Keys blade, you will create a new password by doing the following under Passwords:
# MAGIC 
# MAGIC   * **Description**: Enter a description, such as ADLS Gen2 Auth.
# MAGIC   * **Expires**: Select a duration, such as In 1 year.
# MAGIC 
# MAGIC   ![Create new password](https://databricksdemostore.blob.core.windows.net/images/04/06/registered-app-create-key.png 'Create new password')
# MAGIC 
# MAGIC 7. Select **Save**, and then copy the key displayed under **Value**, and paste it into the cell below for the value of the `clientKey` variable. **Note**: This value will not be accessible once you navigate away from this screen, so make sure you copy it before leaving the Keys blade.
# MAGIC 
# MAGIC   ![Copy key value](https://databricksdemostore.blob.core.windows.net/images/04/06/registered-app-key-value.png 'Copy key value')
# MAGIC 
# MAGIC 8. Run the cell below to set the variables.

# COMMAND ----------

clientId = "<your-service-client-id>"
clientKey = "<your-service-credentials>"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Retrieve your Azure AD tenant ID
# MAGIC 
# MAGIC To perform authentication using the service principal account, Databricks uses OAUTH2. For this, you need to provide your Azure AD Tenant ID.
# MAGIC 
# MAGIC 1. To retrieve your tenant ID, select **Azure Active Directory** from the left-hand navigation menu in the Azure portal, then select **Properties**, and select the copy button next to **Directory ID** on the Directory Properties blade.
# MAGIC 
# MAGIC    ![Retrieve Tenant ID](https://databricksdemostore.blob.core.windows.net/images/04/06/aad-tenant-id.png 'Retrieve Tenant ID')
# MAGIC 
# MAGIC 2. Paste the copied value into the cell below for the value of the `tenantId` variable, and then run the cell.

# COMMAND ----------

tenantId = "<your-directory-id>"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assign permissions to the service principal in ADLS Gen2
# MAGIC 
# MAGIC Next, you need to assign the required permissions to the service principal in your ADLS Gen2 account.
# MAGIC 
# MAGIC 1. In the [Azure portal](https://portal.azure.com), navigate to the ADLS Gen2 account you created above, select **Access control (IAM)** from the left-hand menu, and then select **+ Add role assignment**.
# MAGIC 
# MAGIC    ![ADLS Gen2 Access Control blade](https://databricksdemostore.blob.core.windows.net/images/04/07/access-control.png 'ADLS Gen2 Access Control blade')
# MAGIC 
# MAGIC 2. On the Add role assignment blade, set the following:
# MAGIC 
# MAGIC   * **Permissions**: Check **Read**, **Write**, and **Execute**.
# MAGIC   * **Add to**: Choose This folder and all children.
# MAGIC   * **Add as**: Choose An access permission entry.
# MAGIC   
# MAGIC   ![ADLS Gen2 Add role assignment](https://databricksdemostore.blob.core.windows.net/images/04/07/add-role-assignment.png 'ADLS Gen2 Add role assignment')
# MAGIC 
# MAGIC 3. Select **Save**
# MAGIC 
# MAGIC 4. You will now see the service principal listed under **Role assignments** on the Access control (IAM) blade.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Direct access with OAuth
# MAGIC 
# MAGIC Using your newly created service principal, you are now ready to access your ADLS Gen2 account with OAuth. Both direct access and mounting will use the same configuration.

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": clientId,
           "fs.azure.account.oauth2.client.secret": clientKey,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + tenantId + "/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As with direct access with a Shared Key, direct access with OAuth uses the configuration to provide connection information to Databricks to create the connection and the `abfss` scheme. The cell below will list the files in the `crime-data-2016` folder in ADLS Gen2 using the provided OAuth credentials.

# COMMAND ----------

dbutils.fs.ls("abfss://" + fileSystemName + "@" + adlsGen2AccountName + ".dfs.core.windows.net/training/crime-data-2016")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount ADLS Gen2 with OAuth
# MAGIC 
# MAGIC To mount an ADLS Gen2 file system with DBFS, you will use the `dbutils.fs.mount()` method.
# MAGIC 
# MAGIC Run the cell below to mount ADLS Gen2 to DBFS, using the `configs` set above.

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://" + fileSystemName + "@" + adlsGen2AccountName + ".dfs.core.windows.net/",
  mount_point = "/mnt/adlsGen2",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Looking at the files in our Data Lake
# MAGIC 
# MAGIC Start by reviewing which files are in our Data Lake.
# MAGIC 
# MAGIC In `dbfs:/mnt/adlsGen2/training/crime-data-2016`, there are Parquet files containing 2016 crime data from several United States cities.
# MAGIC 
# MAGIC In the cell below we have data for Boston, Chicago, New Orleans, and more.

# COMMAND ----------

# MAGIC %fs ls /mnt/adlsGen2/training/crime-data-2016

# COMMAND ----------

# MAGIC %md
# MAGIC The next step in looking at the data is to create a DataFrame for each file.  
# MAGIC 
# MAGIC You previously created a DataFrame of the data from New York using the direct access with a Shared Key access method, so let create another Dataframe Boston using the mount you created above:
# MAGIC 
# MAGIC | City          | Table Name              | Path to DBFS file
# MAGIC | ------------- | ----------------------- | -----------------
# MAGIC | **Boston**    | `CrimeDataBoston`       | `dbfs:/mnt/adlsGen2/training/crime-data-2016/Crime-Data-Boston-2016.parquet`

# COMMAND ----------

crimeDataBostonDF = spark.read.parquet("/mnt/adlsGen2/training/crime-data-2016/Crime-Data-Boston-2016.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC With the two DataFrames created, it is now possible to review the first couple records of each file.
# MAGIC 
# MAGIC Notice in the example below:
# MAGIC * The `crimeDataNewYorkDF` and `crimeDataBostonDF` DataFrames use different names for the columns.
# MAGIC * The data itself is formatted differently and different names are used for similar concepts.
# MAGIC 
# MAGIC This is common in a Data Lake. Often files are added to a Data Lake by different groups at different times. The advantage of this strategy is that anyone can contribute information to the Data Lake and that Data Lakes scale to store arbitrarily large and diverse data. The tradeoff for this ease in storing data is that it doesn’t have the rigid structure of a traditional relational data model, so the person querying the Data Lake will need to normalize data before extracting useful insights.
# MAGIC 
# MAGIC The alternative to a Data Lake is a data warehouse.  In a data warehouse, a committee often regulates the schema and ensures data is normalized before being made available.  This makes querying much easier but also makes gathering the data much more expensive and time-consuming.  Many companies choose to start with a Data Lake to accumulate data.  Then, as the need arises, they normalize data and produce higher quality tables for querying.  This reduces the up front costs while still making data easier to query over time.  The normalized tables can later be loaded into a formal data warehouse through nightly batch jobs.  In this way, Apache Spark is used to manage and query both Data Lakes and data warehouses.

# COMMAND ----------

display(crimeDataNewYorkDF)

# COMMAND ----------

display(crimeDataBostonDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Same Type of Data, Different Structure
# MAGIC 
# MAGIC In this section, we examine crime data to determine how to extract homicide statistics.
# MAGIC 
# MAGIC Because the data sets are pooled together in a Data Lake, each city may use different field names and values to indicate homicides, dates, etc.
# MAGIC 
# MAGIC For example:
# MAGIC * Some cities use the value "HOMICIDE", "CRIMINAL HOMICIDE" or "MURDER".
# MAGIC * In the New York data, the column is named `offenseDescription` while in the Boston data, the column is named `OFFENSE_CODE_GROUP`.
# MAGIC * In the New York data, the date of the event is in the `reportDate`, while in the Boston data, there is a single column named `MONTH`.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To get started, create a temporary view containing only the homicide-related rows.
# MAGIC 
# MAGIC At the same time, normalize the data structure of each table so all the columns (and their values) line up with each other.
# MAGIC 
# MAGIC In the case of New York and Boston, here are the unique characteristics of each data set:
# MAGIC 
# MAGIC | | Offense-Column        | Offense-Value          | Reported-Column  | Reported-Data Type |
# MAGIC |-|-----------------------|------------------------|-----------------------------------|
# MAGIC | New York | `offenseDescription`  | starts with "murder" or "homicide" | `reportDate`     | `timestamp`    |
# MAGIC | Boston | `OFFENSE_CODE_GROUP`  | "Homicide"             | `MONTH`          | `integer`      |
# MAGIC 
# MAGIC For the upcoming aggregation, you need to alter the New York data set to include a `month` column which can be computed from the `reportDate` column using the `month()` function. Boston already has this column.
# MAGIC 
# MAGIC In this example, we use several functions in the `pyspark.sql.functions` library, and need to import:
# MAGIC 
# MAGIC * `month()` to extract the month from `reportDate` timestamp data type.
# MAGIC * `lower()` to convert text to lowercase.
# MAGIC * `contains(mySubstr)` to indicate a string contains substring `mySubstr`.
# MAGIC 
# MAGIC Also, note we use  `|`  to indicate a logical `or` of two conditions in the `filter` method.

# COMMAND ----------

from pyspark.sql.functions import lower, upper, month, col

homicidesNewYorkDF = (crimeDataNewYorkDF 
  .select(month(col("reportDate")).alias("month"), col("offenseDescription").alias("offense")) 
  .filter(lower(col("offenseDescription")).contains("murder") | lower(col("offenseDescription")).contains("homicide"))
)

display(homicidesNewYorkDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how the same kind of information is presented differently in the Boston data:
# MAGIC 
# MAGIC `offense` is called `OFFENSE_CODE_GROUP` and there is only one category `homicide`.

# COMMAND ----------

homicidesBostonDF = (crimeDataBostonDF 
  .select("month", col("OFFENSE_CODE_GROUP").alias("offense")) 
  .filter(lower(col("OFFENSE_CODE_GROUP")).contains("homicide"))
)

display(homicidesBostonDF)

# COMMAND ----------

# MAGIC %md
# MAGIC See below the structure of the two tables is now identical.

# COMMAND ----------

display(homicidesNewYorkDF.limit(5))

# COMMAND ----------

display(homicidesBostonDF.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyzing the Data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Now that you normalized the homicide data for each city, combine the two by taking their union.
# MAGIC 
# MAGIC When done, aggregate that data to compute the number of homicides per month.
# MAGIC 
# MAGIC Start by creating a new DataFrame called `homicidesBostonAndNewYorkDF` that consists of the `union` of `homicidesNewYorkDF` with `homicidesBostonDF`.

# COMMAND ----------

homicidesBostonAndNewYorkDF = homicidesNewYorkDF.union(homicidesBostonDF)

# COMMAND ----------

# MAGIC %md
# MAGIC See all the data in one table below:

# COMMAND ----------

display(homicidesBostonAndNewYorkDF.orderBy("month"))

# COMMAND ----------

# MAGIC %md
# MAGIC And finally, perform a simple aggregation to see the number of homicides per month:

# COMMAND ----------

display(homicidesBostonAndNewYorkDF.select("month").orderBy("month").groupBy("month").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC Merge the crime data for Chicago with the data for New York and Boston, and then update our final aggregation of counts-by-month.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1
# MAGIC 
# MAGIC Create the initial DataFrame of the Chicago data.
# MAGIC 0. The source file is `dbfs:/mnt/adlsGen2/training/crime-data-2016/Crime-Data-Chicago-2016.parquet`.
# MAGIC 0. Name the view `crimeDataChicagoDF`.
# MAGIC 0. View the data by invoking the `show()` method.

# COMMAND ----------

# TODO

crimeDataChicagoDF = # FILL_IN
display(crimeDataChicagoDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.

total = crimeDataChicagoDF.count()

dbTest("DF-L6-crimeDataChicago-count", 267872, total)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2
# MAGIC 
# MAGIC Create a new view that normalizes the data structure.
# MAGIC 0. Name the DataFrame `homicidesChicagoDF`.
# MAGIC 0. The DataFrame should have at least two columns: `month` and `offense`.
# MAGIC 0. Filter the data to include only homicides.
# MAGIC 0. View the data by invoking the `show()` method.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use the `month()` function to extract the month-of-the-year.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** To find out which values for each offense constitutes a homicide, produce a distinct list of values from the`crimeDataChicagoDF` DataFrame.

# COMMAND ----------

# TODO

FILL_IN

# COMMAND ----------

# TODO

homicidesChicagoDF = # FILL_IN

display(homicidesChicagoDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.
homicidesChicago = homicidesChicagoDF.select("month").groupBy("month").count().orderBy("month").collect()

dbTest("DF-L6-homicideChicago-len", 12, len(homicidesChicago))
dbTest("DF-L6-homicideChicago-0", 54, homicidesChicago[0][1])
dbTest("DF-L6-homicideChicago-6", 71, homicidesChicago[6][1])
dbTest("DF-L6-homicideChicago-11", 58, homicidesChicago[11][1])

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 3
# MAGIC 
# MAGIC Create a new DataFrame that merges all three data sets (New York, Boston, Chicago):
# MAGIC 0. Name the view `allHomicidesDF`.
# MAGIC 0. Use the `union()` method introduced earlier to merge all three tables.
# MAGIC   * `homicidesNewYorkDF`
# MAGIC   * `homicidesBostonDF`
# MAGIC   * `homicidesChicagoDF`
# MAGIC 0. View the data by invoking the `show()` method.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** To union three tables together, copy the previous example and apply a `union()` method again.

# COMMAND ----------

# TODO

allHomicidesDF = # FILL_IN
display(allHomicidesDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.

allHomicides = allHomicidesDF.count()
dbTest("DF-L6-allHomicides-count", 1203, allHomicides)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4
# MAGIC 
# MAGIC Create a new DataFrame that counts the number of homicides per month.
# MAGIC 0. Name the DataFrame `homicidesByMonthDF`.
# MAGIC 0. Rename the column `count(1)` to `homicides`.
# MAGIC 0. Group the data by `month`.
# MAGIC 0. Sort the data by `month`.
# MAGIC 0. Count the number of records for each aggregate.
# MAGIC 0. View the data by invoking the `show()` method.

# COMMAND ----------

# TODO
from pyspark.sql.functions import count
homicidesByMonthDF  = # FILL_IN
display(allHomicidesDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.
allHomicides = homicidesByMonthDF.collect()

dbTest("DF-L6-homicidesByMonth-len", 12, len(allHomicides))
dbTest("DF-L6-homicidesByMonth-0", 1, allHomicides[0][0])
dbTest("DF-L6-homicidesByMonth-11", 12, allHomicides[11][0])
dbTest("DF-L6-allHomicides-0", 83, allHomicides[0][1])
dbTest("DF-L6-allHomicides-1", 83, allHomicides[0][1])
dbTest("DF-L6-allHomicides-2", 68, allHomicides[1][1])
dbTest("DF-L6-allHomicides-3", 72, allHomicides[2][1])
dbTest("DF-L6-allHomicides-4", 76, allHomicides[3][1])
dbTest("DF-L6-allHomicides-5", 105, allHomicides[4][1])
dbTest("DF-L6-allHomicides-6", 120, allHomicides[5][1])
dbTest("DF-L6-allHomicides-7", 116, allHomicides[6][1])
dbTest("DF-L6-allHomicides-8", 144, allHomicides[7][1])
dbTest("DF-L6-allHomicides-9", 109, allHomicides[8][1])
dbTest("DF-L6-allHomicides-10", 109, allHomicides[9][1])
dbTest("DF-L6-allHomicides-11", 111, allHomicides[10][1])
dbTest("DF-L6-allHomicides-12", 90, allHomicides[11][1])

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unmount ADLS from DBFS

# COMMAND ----------

dbutils.fs.unmount("/mnt/adlsGen2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC * Spark DataFrames allow you to easily manipulate data in Azure Data Lake Storage Gen2.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Questions
# MAGIC **Q:** What is Azure Data Lake Storage Gen2?  
# MAGIC **A:** Azure Data Lake Storage Gen2 is a next-generation data lake solution for big data analytics. Azure Data Lake Storage Gen2 builds Azure Data Lake Storage Gen1 capabilities–such as file system semantics, file-level security, and scale–into Azure Blob Storage, with its low-cost tiered storage, high availability, and disaster recovery features.
# MAGIC 
# MAGIC **Q:** What is the primary access method for data in Azure Data Lake Storage Gen2 from Azure Databricks?  
# MAGIC **A:** The primary access methods for data in Azure Data Lake Storage Gen2 Preview is via the Hadoop FileSystem. Data Lake Storage Gen2 allows users of Azure Blob Storage access to a new driver, the Azure Blob File System driver or `ABFS`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC * Continue to the [Key Vault-backed secret scopes]($./08-Key-Vault-backed-secret-scopes) lesson