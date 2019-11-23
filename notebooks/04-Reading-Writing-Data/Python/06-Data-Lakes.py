# Databricks notebook source
# MAGIC %md
# MAGIC # Querying Data Lakes with DataFrames
# MAGIC 
# MAGIC Apache Spark&trade; and Azure Databricks&reg; make it easy to access and work with files stored in Data Lakes, such as Azure Data Lake Storage (ADLS).
# MAGIC 
# MAGIC Companies frequently store thousands of large data files gathered from various teams and departments, typically using a diverse variety of formats including CSV, JSON, and XML. Data scientists often wish to extract insights from this data.
# MAGIC 
# MAGIC The classic approach to querying this data is to load it into a central database called a **data warehouse**. Traditionally, data engineers must design the schema for the central database, extract the data from the various data sources, transform the data to fit the warehouse schema, and load it into the central database. A data scientist can then query the data warehouse directly or query smaller data sets created to optimize specific types of queries. The data warehouse approach works well, but requires a great deal of up front effort to design and populate schemas. It also limits historical data, which is constrained to only the data that fits the warehouse’s schema.
# MAGIC 
# MAGIC An alternative approach is a **Data Lake**, which:
# MAGIC 
# MAGIC * Is a storage repository that cheaply stores a vast amount of raw data in its native format.
# MAGIC * Consists of current and historical data dumps in various formats including XML, JSON, CSV, Parquet, etc.
# MAGIC * May contain operational relational databases with live transactional data.
# MAGIC 
# MAGIC Spark is ideal for querying Data Lakes. Spark DataFrames can be used to read directly from raw files contained in a Data Lake and then execute queries to join and aggregate the data.
# MAGIC 
# MAGIC This lesson illustrates how to perform exploratory data analysis (EDA) to gain insights from a Data Lake.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * **IMPORTANT**: You must have permissions within your Azure subscription to create an App Registration and service principal within Azure Active Directory to complete this lesson.
# MAGIC * Lesson: <a href="$./02-Querying-Files">Querying Files with SQL</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Azure Data Lake Storage Gen1 (ADLS)
# MAGIC 
# MAGIC 1. In the [Azure portal](https://portal.azure.com), select **+ Create a resource**, enter "data lake" into the Search the Marketplace box, select **Data Lake Storage Gen1** from the results, and then select **Create**.
# MAGIC 
# MAGIC    ![In the Azure portal, +Create a resource is highlighted in the navigation pane, "data lake" is entered into the Search the Marketplace box, and Data Lake Storage Gen1 is highlighted in the results.](https://databricksdemostore.blob.core.windows.net/images/04/06/adls-create-resource.png 'Create Azure Data Lake Storage Gen1')
# MAGIC 
# MAGIC 2. On the New Data Lake Storage Gen1 blade, enter the following:
# MAGIC 
# MAGIC    - **Name**: Enter a globally unique name (indicated by a green check mark).
# MAGIC    - **Subscription**: Select the subscription you are using for this module.
# MAGIC    - **Resource group**: Choose your module resource group.
# MAGIC    - **Location**: Select the closest location.
# MAGIC    - **Pricing package**: Choose Pay-as-You-Go.
# MAGIC    - **Encryption settings**: Leave set to the default value of Enabled.
# MAGIC 
# MAGIC    ![The New Data Lake Storage Gen1 blade is displayed, with the previously mentioned settings entered into the appropriate fields.](https://databricksdemostore.blob.core.windows.net/images/04/06/adls-create-new.png 'New Data Lake Storage Gen1')
# MAGIC 
# MAGIC 3. Select **Create** to provision the new ADLS instance.
# MAGIC 
# MAGIC 4. In the cell below, set the value of the `adlsAccountName` variable to the same name you used for the **Name** field when creating your ADLS instance above, and then run the cell.

# COMMAND ----------

adlsAccountName = "<your-data-lake-store-account-name>"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Azure Active Directory application and service principal
# MAGIC 
# MAGIC > **IMPORTANT**: You must have permissions within your Azure subscription to create an App registration and service principal within Azure Active Directory to complete this lesson.
# MAGIC 
# MAGIC ADLS uses Azure Active Directory for authentication. To provide access to your ADLS instance from Azure Databricks, you will use [service-to-service authentication](https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-service-to-service-authenticate-using-active-directory). For this, you need to create an identity in Azure Active Directory (Azure AD) known as a service principal.
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
# MAGIC 4. To access your ADLS instance from Azure Databricks you will need to provide the credentials of your newly created service principal within Databricks. On the Registered app blade that appears, copy the **Application ID** and paste it into the cell below as the value for the `clientId` variable.
# MAGIC 
# MAGIC    ![Copy the Registered App Application ID](https://databricksdemostore.blob.core.windows.net/images/04/06/registered-app-id.png 'Copy the Registered App Application ID')
# MAGIC 
# MAGIC 5. Next, select **Settings** on the Registered app blade, and then select **Keys**.
# MAGIC 
# MAGIC    ![Open Keys blade for the Registered App](https://databricksdemostore.blob.core.windows.net/images/04/06/registered-app-settings-keys.png 'Open Keys blade for the Registered App')
# MAGIC 
# MAGIC 6. On the Keys blade, you will create a new password by doing the following under Passwords:
# MAGIC 
# MAGIC   * **Description**: Enter a description, such as ADLS Auth.
# MAGIC   * **Expires**: Select a duration, such as In 1 year.
# MAGIC 
# MAGIC   ![Create new password](https://databricksdemostore.blob.core.windows.net/images/04/06/registered-app-create-key.png 'Create new password')
# MAGIC 
# MAGIC 7. Select **Save**, and then copy the key displayed under **Value**, and paste it into the cell below for the value of the `clientKey` variable. **Note**: This value will not be accessible once you navigate away from this screen, so make sure you copy it before leaving the Keys blade.
# MAGIC 
# MAGIC   ![Copy key value](https://databricksdemostore.blob.core.windows.net/images/04/06/registered-app-key-value.png 'Copy key value')
# MAGIC 
# MAGIC 8. Run the cell below.

# COMMAND ----------

clientId = "<your-service-client-id>"
clientKey = "<your-service-credentials>"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve your Azure AD tenant ID
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
# MAGIC ## Assign permissions to the service principal in ADLS
# MAGIC 
# MAGIC Next, you need to assign the required permissions to the service principal in ADLS.
# MAGIC 
# MAGIC 1. In the [Azure portal](https://portal.azure.com), navigate to the ADLS instance you created above, and on the Overview blade, select **Data explorer**.
# MAGIC 
# MAGIC    ![ADLS Overview blade](https://databricksdemostore.blob.core.windows.net/images/04/06/adls-overview.png 'ADLS Overview blade')
# MAGIC 
# MAGIC 2. In the Data Explorer blade, select **Access** on the toolbar.
# MAGIC 
# MAGIC    ![ADLS Data Explorer toolbar](https://databricksdemostore.blob.core.windows.net/images/04/06/adls-data-explorer-toolbar.png 'ADLS Data Explorer toolbar')
# MAGIC 
# MAGIC 3. On the Access blade, select **+ Add**.
# MAGIC 
# MAGIC    ![ADLS Data Explorer add access](https://databricksdemostore.blob.core.windows.net/images/04/06/adls-access.png 'ADLS Data Explorer add access')
# MAGIC 
# MAGIC 4. On the Assign permissions -> Select user or group blade, enter the name of your Registered app (e.g., databricks-demo) into the **Select** box, choose your app from the list, and select **Select**.
# MAGIC 
# MAGIC    ![ADLS assign permissions to user or group](https://databricksdemostore.blob.core.windows.net/images/04/06/adls-assign-permissions-select-user-or-group.png 'ADLS assign permissions to user or group')
# MAGIC 
# MAGIC 5. On the Assign permissions -> Select permissions blade, set the following:
# MAGIC 
# MAGIC   * **Permissions**: Check **Read**, **Write**, and **Execute**.
# MAGIC   * **Add to**: Choose This folder and all children.
# MAGIC   * **Add as**: Choose An access permission entry.
# MAGIC   
# MAGIC   ![ADLS assign permissions](https://databricksdemostore.blob.core.windows.net/images/04/06/adls-assign-permissions.png 'ADLS assign permissions')
# MAGIC 
# MAGIC 6. Select **Ok**
# MAGIC 
# MAGIC 7. You will now see the service principal listed under **Assigned permissions** on the Access blade.
# MAGIC 
# MAGIC   ![ADLS assigned permissions](https://databricksdemostore.blob.core.windows.net/images/04/06/adls-assigned-permissions.png 'ADLS assigned permissions')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount ADLS to DBFS
# MAGIC 
# MAGIC You are now ready to access your ADLS account from Azure Databricks. Run the cell below to set the required configuration and mount ADLS to DBFS.

# COMMAND ----------

configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": clientId,
           "dfs.adls.oauth2.credential": clientKey,
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/" + tenantId + "/oauth2/token"}

dbutils.fs.mount(
  source = "adl://" + adlsAccountName + ".azuredatalakestore.net/",
  mount_point = "/mnt/adls",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy data to ADLS
# MAGIC 
# MAGIC Run the following cell to copy the Crime-data-2016 dataset from the Training folder into your ADLS instance, in a folder named "training". This will take a few minutes to complete.

# COMMAND ----------

dbutils.fs.cp("/mnt/training/crime-data-2016", "mnt/adls/training/crime-data-2016", true)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Looking at the files in our Data Lake
# MAGIC 
# MAGIC Start by reviewing which files are in our Data Lake.
# MAGIC 
# MAGIC In `dbfs:/mnt/adls/training/crime-data-2016`, there are Parquet files containing 2016 crime data from several United States cities.
# MAGIC 
# MAGIC In the cell below we have data for Boston, Chicago, New Orleans, and more.

# COMMAND ----------

# MAGIC %fs ls /mnt/adls/training/crime-data-2016

# COMMAND ----------

# MAGIC %md
# MAGIC The next step in looking at the data is to create a DataFrame for each file.  
# MAGIC 
# MAGIC Start by creating a DataFrame of the data from New York and then Boston:
# MAGIC 
# MAGIC | City          | Table Name              | Path to DBFS file
# MAGIC | ------------- | ----------------------- | -----------------
# MAGIC | **New York**  | `CrimeDataNewYork`      | `dbfs:/mnt/adls/training/crime-data-2016/Crime-Data-New-York-2016.parquet`
# MAGIC | **Boston**    | `CrimeDataBoston`       | `dbfs:/mnt/adls/training/crime-data-2016/Crime-Data-Boston-2016.parquet`

# COMMAND ----------

crimeDataNewYorkDF = spark.read.parquet("/mnt/adls/training/crime-data-2016/Crime-Data-New-York-2016.parquet")

# COMMAND ----------

crimeDataBostonDF = spark.read.parquet("/mnt/adls/training/crime-data-2016/Crime-Data-Boston-2016.parquet")

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
# MAGIC 0. The source file is `dbfs:/mnt/adls/training/crime-data-2016/Crime-Data-Chicago-2016.parquet`.
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

dbutils.fs.unmount("/mnt/adls")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC * Spark DataFrames allow you to easily manipulate data in a Data Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Questions
# MAGIC **Q:** What is a Data Lake?  
# MAGIC **A:** A Data Lake is a collection of data files gathered from various sources.  Spark loads each file as a table and then executes queries by joining and aggregating these files.
# MAGIC 
# MAGIC **Q:** What are advantages of Data Lakes over classic Data Warehouses?  
# MAGIC **A:** Data Lakes allow for large amounts of data to be aggregated from many sources with minimal preparatory steps.  Data Lakes also allow for very large files.  Powerful query engines such as Spark can read the diverse collection of files and execute complex queries efficiently.
# MAGIC 
# MAGIC **Q:** What are some advantages of Data Warehouses?  
# MAGIC **A:** Data warehouses are neatly curated to ensure data from all sources fit a common schema.  This makes them easy to query.
# MAGIC 
# MAGIC **Q:** What's the best way to combine the advantages of Data Lakes and Data Warehouses?  
# MAGIC **A:** Start with a Data Lake.  As you query, you will discover cases where the data needs to be cleaned, combined, and made more accessible.  Create periodic Spark jobs to read these raw sources and write new "golden" DataFrames that are cleaned and more easily queried.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC * Continue to the [Azure Data Lake Gen2]($./07-Azure-Data-Lake-Gen2) lesson