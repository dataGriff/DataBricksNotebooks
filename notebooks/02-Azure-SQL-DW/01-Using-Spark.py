# Databricks notebook source
# MAGIC %md
# MAGIC # Using Spark in Azure Databricks
# MAGIC 
# MAGIC This notebook will give you a quick overview on how to harness the power of the Apache Spark engine within Azure Databricks by executing cells in an interactive web document, called a notebook.
# MAGIC 
# MAGIC Spark is used to do many things, including reading and writing huge files and data sets. It provides a query engine capable of processing data in very, very large data files.  Some of the largest Spark jobs in the world run on Petabytes of data.
# MAGIC 
# MAGIC Up to this point in the lab, you have worked with reading and writing flat files using Azure SQL Data Warehouse (SQL DW) and PolyBase. An alternative to working with these flat files is to use the Apache Spark engine. The reasons you would want to do this include having the ability to more rapidly work with the files in an interactive way, combine both batch and stream processing using the same engine and code base, and include machine learning and deep learning as part of your big data process. Use SQL Data Warehouse as a key component of a big data solution. Import big data into SQL Data Warehouse with simple PolyBase T-SQL queries, and then use the power of massively parallel processing (MPP) to run high-performance analytics. As you integrate and analyze, the data warehouse will become the single version of truth your business can count on for insights.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure your module.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Remember to attach your notebook to a cluster before running any cells in your notebook. In the notebook's toolbar, select the drop down arrow next to Detached, then select your cluster under Attach to.
# MAGIC 
# MAGIC ![Attached to cluster](https://databricksdemostore.blob.core.windows.net/images/03/03/databricks-cluster-attach.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 3
# MAGIC 
# MAGIC Now that you have opened this notebook, we can use it to run some code.
# MAGIC 1. In the cell below, we have a simple caluclation we would like to run: `1 + 1`.
# MAGIC 2. Run the cell by clicking the run icon and selecting **Run Cell**.
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/run-notebook-1.png" style="width:600px; margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You can also run a cell by typing **Ctrl-Enter**.

# COMMAND ----------

1 + 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data sources for this lab
# MAGIC 
# MAGIC For completion of this lab, it is mandatory to complete the environment setup found in the Microsoft learning module.
# MAGIC 
# MAGIC The PowerShell script executed during setup creates an Azure Storage account with two containers: `labdata` and `dwtemp`. The `dwtemp` container acts as a common Blob Storage container that both the Azure Databricks cluster and the SQL DW instance use to exchange data between the two systems.
# MAGIC 
# MAGIC The PowerShell script copied datasets into the `labdata` container that are required for this lab. Those files are contained within `/retaildata/rawdata/`.
# MAGIC 
# MAGIC Here is a description of the datasets we will use:
# MAGIC 
# MAGIC 1.  **ProductFile**: Contains products data e.g. ProductId, ProductName,
# MAGIC     Department, DepartmentId, Category, CategoryId and price for single
# MAGIC     product.
# MAGIC 
# MAGIC 2.  **UserFile**: Contains user data e.g. UserId, FirstName, LastName,
# MAGIC     Age, Gender, RegistrationDate.
# MAGIC 
# MAGIC 3.  **Weblog**: Contains weblog data which provides users activity log
# MAGIC     on retail site e.g. SessionId, UserId, CustomerAction, ProductId,
# MAGIC     TransactionTime etc. CustomerAction field determines if user has
# MAGIC     purchased, browsed or added product in cart.

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to mount an Azure Blob to DBFS
# MAGIC 
# MAGIC Azure Databricks has its own file system called Databricks File System, or DBFS. DBFS provides a common place to read and write files that are stored across one or more mount points. A mount point is how you attach file storage from one or more services, such as Azure Blob storage or Azure Data Lake Store. We will mount the Azure storage account as a new DBFS directory to make it easier to access the files contained within.
# MAGIC 
# MAGIC Once the blob is mounted as a DBFS directory, access it without exposing your Azure Blob Store keys.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Creating a Shared Access Signature (SAS) URL
# MAGIC Azure provides you with a secure way to create and share access keys for your Azure Blob Store without compromising your account keys.
# MAGIC 
# MAGIC More details are provided <a href="http://docs.microsoft.com/azure/storage/common/storage-dotnet-shared-access-signature-part-1" target="_blank"> in this document</a>.
# MAGIC 
# MAGIC This allows access to your Azure Blob Store data directly from Databricks distributed file system (DBFS).
# MAGIC 
# MAGIC As shown in the screen shot, in the Azure Portal, go to the storage account containing the blob to be mounted. Then:
# MAGIC 
# MAGIC 1. Select Shared access signature from the menu.
# MAGIC 2. Click the Generate SAS button.
# MAGIC 3. Copy the entire Blob service SAS URL to the clipboard.
# MAGIC 4. Use the URL in the mount operation, as shown below.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/DataFrames-MSFT/create-sas-keys.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; margin-top: 20px; padding: 10px"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Create the mount point with `dbutils.fs.mount(source = .., mountPoint = .., extraConfigs = ..)`.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> If the directory is already mounted, you receive the following error:
# MAGIC 
# MAGIC > Directory already mounted: /mnt/dw-training
# MAGIC 
# MAGIC In this case, use a different mount point such as `dw-training-2`, and ensure you update all three references below. Be sure to udate **SasURL**, and **StorageAccount**. The ContainerName value should be "labdata".

# COMMAND ----------

SasURL = "https://azuredatabricksstore03.blob.core.windows.net/?sv=2018-03-28&ss=bfqt&srt=sco&sp=rwdlacup&se=2020-01-06T06:44:19Z&st=2019-01-05T22:44:19Z&spr=https&sig=FCoo3O4PjjoeF34yn%2FIMBcKyte%2BMTaKFcrdZiP8cdLE%3D"
indQuestionMark = SasURL.index('?')
SasKey = SasURL[indQuestionMark:len(SasURL)]
StorageAccount = "azuredatabricksstore03"
ContainerName = "labdata"
MountPoint = "/mnt/dw-training"

dbutils.fs.mount(
  source = "wasbs://%s@%s.blob.core.windows.net/" % (ContainerName, StorageAccount),
  mount_point = MountPoint,
  extra_configs = {"fs.azure.sas.%s.%s.blob.core.windows.net" % (ContainerName, StorageAccount) : "%s" % SasKey}
)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the file contents of the **/retaildata/rawdata** subdirectory of the mount you just created:

# COMMAND ----------

# MAGIC %fs ls /mnt/dw-training/retaildata/rawdata

# COMMAND ----------

# MAGIC %md
# MAGIC ### Introducing DataFrames
# MAGIC 
# MAGIC This new mount point is now available to all Spark nodes of the Databricks cluster, allowing us to parallelize reads and writes as needed. We can now read the data into data structures called DataFrames.
# MAGIC 
# MAGIC Under the covers, DataFrames are derived from data structures known as Resilient Distributed Datasets (RDDs). RDDs and DataFrames are immutable distributed collections of data. Let's take a closer look at what some of these terms mean before we understand how they relate to DataFrames:
# MAGIC 
# MAGIC * **Resilient**: They are fault tolerant, so if part of your operation fails, Spark  quickly recovers the lost computation.
# MAGIC * **Distributed**: RDDs are distributed across networked machines known as a cluster.
# MAGIC * **DataFrame**: A data structure where data is organized into named columns, like a table in a relational database, but with richer optimizations under the hood. 
# MAGIC 
# MAGIC Without the named columns and declared types provided by a schema, Spark wouldn't know how to optimize the executation of any computation. Since DataFrames have a schema, they use the Catalyst Optimizer to determine the optimal way to execute your code.
# MAGIC 
# MAGIC DataFrames were invented because the business community uses tables in a relational database, Pandas or R DataFrames, or Excel worksheets. A Spark DataFrame is conceptually equivalent to these, with richer optimizations under the hood and the benefit of being distributed across a cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Interacting with DataFrames
# MAGIC 
# MAGIC Once created (instantiated), a DataFrame object has methods attached to it. Methods are operations one can perform on DataFrames such as filtering,
# MAGIC counting, aggregating and many others.
# MAGIC 
# MAGIC > <b>Example</b>: To create (instantiate) a DataFrame, use this syntax: `df = ...`
# MAGIC 
# MAGIC To display the contents of the DataFrame, apply a `show` operation (method) on it using the syntax `df.show()`. 
# MAGIC 
# MAGIC The `.` indicates you are *applying a method on the object*.
# MAGIC 
# MAGIC In working with DataFrames, it is common to chain operations together, such as: `df.select().filter().orderBy()`.  
# MAGIC 
# MAGIC By chaining operations together, you don't need to save intermediate DataFrames into local variables (thereby avoiding the creation of extra objects).
# MAGIC 
# MAGIC Also note that you do not have to worry about how to order operations because the optimizier determines the optimal order of execution of the operations for you.
# MAGIC 
# MAGIC `df.select(...).orderBy(...).filter(...)`
# MAGIC 
# MAGIC versus
# MAGIC 
# MAGIC `df.filter(...).select(...).orderBy(...)`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### DataFrames and SQL
# MAGIC 
# MAGIC DataFrame syntax is more flexible than SQL syntax. Here we illustrate general usage patterns of SQL and DataFrames.
# MAGIC 
# MAGIC Suppose we have a data set we loaded as a table called `myTable` and an equivalent DataFrame, called `df`.
# MAGIC We have three fields/columns called `col_1` (numeric type), `col_2` (string type) and `col_3` (timestamp type)
# MAGIC Here are basic SQL operations and their DataFrame equivalents. 
# MAGIC 
# MAGIC Notice that columns in DataFrames are referenced by `col("<columnName>")`.
# MAGIC 
# MAGIC | SQL                                         | DataFrame (Python)                    |
# MAGIC | ------------------------------------------- | ------------------------------------- | 
# MAGIC | `SELECT col_1 FROM myTable`                 | `df.select(col("col_1"))`             | 
# MAGIC | `DESCRIBE myTable`                          | `df.printSchema()`                    | 
# MAGIC | `SELECT * FROM myTable WHERE col_1 > 0`     | `df.filter(col("col_1") > 0)`         | 
# MAGIC | `..GROUP BY col_2`                          | `..groupBy(col("col_2"))`             | 
# MAGIC | `..ORDER BY col_2`                          | `..orderBy(col("col_2"))`             | 
# MAGIC | `..WHERE year(col_3) > 1990`                | `..filter(year(col("col_3")) > 1990)` | 
# MAGIC | `SELECT * FROM myTable LIMIT 10`            | `df.limit(10)`                        |
# MAGIC | `display(myTable)` (text format)            | `df.show()`                           | 
# MAGIC | `display(myTable)` (html format)            | `display(df)`                         |
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You can also run SQL queries with the special syntax `spark.sql("SELECT * FROM myTable")`
# MAGIC 
# MAGIC In this course you see many other usages of DataFrames. It is left up to you to figure out the SQL equivalents 
# MAGIC (left as exercises in some cases).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading data
# MAGIC 
# MAGIC Now that you understand what DataFrames are, let's create a new DataFrame named `weblogData` by reading the weblogs. These files are in text format, and the data within is delimited by pipe (|) characters. We also know that each file contains a head row. We can let the Spark engine know this by using the `option` properties of the `read` function, as seen below.

# COMMAND ----------

weblogData = (spark
              .read
              .option("header","true") # Allows us to extract the header
              .option("delimiter", '|') # Specifies the pipe character as the delimiter
              .csv("mnt/dw-training/retaildata/rawdata/weblognew/[3-5]/{*}/weblog.txt"))
weblogData.show()

# COMMAND ----------

# MAGIC %md
# MAGIC In the above cell, we are trying to read data from the mounted path:
# MAGIC 
# MAGIC `/retaildata/rawdata/weblognew/\[3-5\]/{\*}/weblog.txt`
# MAGIC 
# MAGIC We can use wild card characters in a path string to read multiple files.
# MAGIC 
# MAGIC **\[3-5\]** matches with all the sub directories inside the weblognew directory having names 3, 4 and 5.
# MAGIC 
# MAGIC **\*** matches with all the sub directories inside parent directory.
# MAGIC 
# MAGIC The above code returns a DataFrame and sets it to the variable `weblogData`.
# MAGIC 
# MAGIC **DataFrame.take(** *n* **)** returns the first n number of elements from a DataFrame.

# COMMAND ----------

display(weblogData.take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Now, let's create two new DataFrames; one for users and one for products.
# MAGIC 
# MAGIC These files are a bit different than the weblog files. To start, they are comma-delimited, and they also contain no header. As such, we set the delimiter to comma, and we set an alias for each column for clarity.

# COMMAND ----------

from pyspark.sql.functions import col
users = (spark
         .read
         .option("inferSchema", "true") # Automatically applies a schema by evaluating the data types
         .option("delimiter", ',')
         .csv("mnt/dw-training/retaildata/rawdata/UserFile/part{*}")
         .select(col("_c0").alias("UserId"),
                 col("_c3").alias("FirstName"),
                 col("_c5").alias("LastName"),
                 col("_c9").alias("Gender"),
                 col("_c15").alias("Age"),
                 col("_c18").alias("RegisteredDate")
                )
        )
products = (spark
            .read
            .option("inferSchema", "true")
            .option("delimiter", ',')
            .csv("mnt/dw-training/retaildata/rawdata/ProductFile/part{*}")
            .select(col("_c0").alias("ProductId"),
                    col("_c1").alias("ProductName"),
                    col("_c2").alias("BasePrice"),
                    col("_c3").alias("CategoryId"),
                    col("_c7").alias("Category"),
                    col("_c8").alias("Department")
                   )
           )

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the user data.

# COMMAND ----------

display(users)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, view the product data.

# COMMAND ----------

display(products)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform DataFrame operations
# MAGIC 
# MAGIC We will now explore various operations that can be performed on DataFrames, using the three we have created so far.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select
# MAGIC 
# MAGIC **select(\*cols)**
# MAGIC 
# MAGIC Projects a set of expressions and returns a new DataFrame.
# MAGIC 
# MAGIC **Parameters**:
# MAGIC 
# MAGIC cols -- list of column names (string) or expressions (Column). If
# MAGIC one of the column names is '\*', that column is expanded to include
# MAGIC all columns in the current DataFrame.
# MAGIC 
# MAGIC Try this out in the cell below:

# COMMAND ----------

products.select("ProductId","ProductName").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### OrderBy
# MAGIC 
# MAGIC **orderBy**(\*cols, \*\*kwargs)
# MAGIC 
# MAGIC Returns a new DataFrame sorted by the specified column(s).
# MAGIC 
# MAGIC **Parameters**:
# MAGIC 
# MAGIC cols -- list of Column or column names to sort by.
# MAGIC 
# MAGIC ascending -- boolean or list of boolean (default True). Sort
# MAGIC ascending vs. descending. Specify list for multiple sort orders. If
# MAGIC a list is specified, length of the list must equal length of the
# MAGIC cols.
# MAGIC 
# MAGIC Execute the following 3 cells to try a few of different methods:

# COMMAND ----------

products.orderBy("ProductId",ascending=False).show(5)

# COMMAND ----------

products.orderBy(["ProductName","BasePrice"],ascending=[0, 1]).show(5)

# COMMAND ----------

products.orderBy(products.ProductId.desc()).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Count
# MAGIC 
# MAGIC **count()**
# MAGIC 
# MAGIC Returns the number of rows in this DataFrame.
# MAGIC 
# MAGIC Execute the following cell to see the count of product records:

# COMMAND ----------

products.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Agg
# MAGIC 
# MAGIC **agg(\*exprs)**
# MAGIC 
# MAGIC Aggregate on the entire DataFrame without groups.
# MAGIC 
# MAGIC Execute the following two cells for a couple examples of the `agg` operation:

# COMMAND ----------

products.agg({"BasePrice" : "max"}).show(1)

# COMMAND ----------

from pyspark.sql import functions as F

products.agg(F.min(products.BasePrice)).show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Where
# MAGIC 
# MAGIC **where(**condition**)**
# MAGIC 
# MAGIC Filters rows using the given condition.
# MAGIC 
# MAGIC `where()` is an alias for `filter()`.
# MAGIC 
# MAGIC Execute the following cell:

# COMMAND ----------

products.where(products.BasePrice > 500).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### GroupBy
# MAGIC 
# MAGIC **groupBy**(\*cols)
# MAGIC 
# MAGIC Groups the DataFrame using the specified columns, so we can run
# MAGIC aggregation on them. See GroupedData for all the available aggregate
# MAGIC functions.
# MAGIC 
# MAGIC **Parameters**:
# MAGIC 
# MAGIC cols -- list of columns to group by. Each element should be a column
# MAGIC name (string) or an expression (Column).
# MAGIC 
# MAGIC Execute the following three cells for a few examples of the `groupBy` operation:

# COMMAND ----------

# Without column name

products.groupBy().min().show()

# COMMAND ----------

# With column name

products.groupBy("Department").count().show(5)

# COMMAND ----------

# Get minimum BasePrice in each department

products.groupBy(products.Department).min("BasePrice").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Joins
# MAGIC 
# MAGIC Before exploring joins, we need to create a weblog DataFrame containing a small sample of records.
# MAGIC 
# MAGIC Execute the following to retrieve 10 records from the weblog DataFrame where the customer action is Purchased:

# COMMAND ----------

weblogSampleData = weblogData.where(weblogData.Action == "Purchased").take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC The above code snippet returns a list of Row objects. We have to convert that list object into an RDD.
# MAGIC 
# MAGIC Execute the following cell to convert the list into an RDD:

# COMMAND ----------

weblogSampleRDD = sc.parallelize(weblogSampleData)

# COMMAND ----------

# MAGIC %md
# MAGIC Now execute below cell to convert the RDD into a DataFrame:

# COMMAND ----------

weblogSampleDF = weblogSampleRDD.toDF()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join
# MAGIC 
# MAGIC **join**(other, joinExprs=None, joinType=None)
# MAGIC 
# MAGIC Joins with another DataFrame, using the given join expression.
# MAGIC 
# MAGIC **Parameters**:
# MAGIC 
# MAGIC other -- Right side of the join
# MAGIC 
# MAGIC joinExprs -- a string for join column name, or a join expression
# MAGIC (Column). If joinExprs is a string indicating the name of the join
# MAGIC column, the column must exist on both sides, and this performs an
# MAGIC inner equi-join.
# MAGIC 
# MAGIC joinType -- str, default 'inner'. One of inner, outer, left\_outer,
# MAGIC right\_outer, semijoin.
# MAGIC 
# MAGIC Execute the following cell to join the sample weblog DataFrame with the products DataFrame based on the Product ID key to get the BasePrice for each product purchased in the weblog DataFrame:

# COMMAND ----------

weblogSampleDF.join(products,weblogSampleDF.ProductId == products.ProductId,'inner').select(weblogSampleDF.SessionId,weblogSampleDF.ProductId,weblogSampleDF.Quantity,products.BasePrice).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learn Spark SQL 
# MAGIC 
# MAGIC The **sql** function of **SparkSession** object enables applications to
# MAGIC run SQL queries programmatically and returns the result as a DataFrame
# MAGIC object. However, to run SQL queries we need to create a table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks databases and tables
# MAGIC 
# MAGIC An Azure Databricks database is a collection of tables. An Azure Databricks table is a collection of structured data. Tables are equivalent to Apache Spark DataFrames. This means that you can cache, filter, and perform any operations supported by DataFrames on tables. You can query tables with Spark APIs and Spark SQL.
# MAGIC 
# MAGIC There are two types of tables: global and local. A global table is available across all clusters. Azure Databricks registers global tables to the Hive metastore. A local table is not accessible from other clusters and is not registered in the Hive metastore. This is also known as a temporary table or a view.
# MAGIC 
# MAGIC A temporary view gives you a name to query from SQL, but unlike a table it exists only for the duration of your Spark Session. As a result, the temporary view will not carry over when you restart the cluster or switch to a new notebook. It also won't show up in the Data button on the menu on the left side of a Databricks notebook which provides easy access to databases and tables.
# MAGIC 
# MAGIC You create a temporary view with the `createOrReplaceTempView` operation on a DataFrame.
# MAGIC 
# MAGIC For example, to create a temporary view of the products DataFrame, you would execute the following:
# MAGIC 
# MAGIC `products.createOrReplaceTempView("Products")`
# MAGIC 
# MAGIC For our purposes, we will be creating a **global table**. This is because you will be executing a different notebook after you are done with this one, and you will want to have access to the data structures you define here when writing the data back to your Azure SQL Data Warehouse instance. This as opposed to creating the DataFrames all over again.
# MAGIC 
# MAGIC > Note: To avoid potential consistency issues, the best approach to replacing table contents is to overwrite the table. Just in case the "products" table already exists, we will overwrite its contents with our DataFrame.
# MAGIC 
# MAGIC Execute the cell below to create a new global "products" table from the `products` DataFrame:

# COMMAND ----------

products.write.mode("OVERWRITE").saveAsTable("products")

# COMMAND ----------

# MAGIC %md
# MAGIC Now, execute the following cell to create a new global "weblogs" table, overwriting any existing tables:

# COMMAND ----------

weblogSampleDF.write.mode("OVERWRITE").saveAsTable("weblogs")

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, execute the following cell to create a new global "users" table:

# COMMAND ----------

users.write.mode("OVERWRITE").saveAsTable("users")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run SQL queries
# MAGIC 
# MAGIC Execute the following cells to learn how to run Spark SQL queries.
# MAGIC 
# MAGIC Notice that each cell starts with the **%sql** magic. This tells the Spark engine to execute the cell using the SQL language, as opposed to the default python language.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from products
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select ProductName from products where ProductId = 30

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select Department, count(ProductId) as ProductCount
# MAGIC from products group by Department order by ProductCount desc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select w.SessionId, w.ProductId, p.ProductName, p.BasePrice, w.Quantity, (p.BasePrice * w.Quantity) as Total
# MAGIC from weblogs w Join products p on w.ProductId == p.ProductId

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Create visualizations
# MAGIC 
# MAGIC Azure Databricks provides easy-to-use, built-in visualizations for your data. 
# MAGIC 
# MAGIC Display the data by invoking the Spark `display` function.
# MAGIC 
# MAGIC Visualize the query below by selecting the down arrow button next to the bar graph icon once the table is displayed. Select **Area** to select the Area chart visualization:
# MAGIC 
# MAGIC <img src="https://databricksdemostore.blob.core.windows.net/images/02-SQL-DW/databricks-display-graph-button.png" style="border: 1px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC Configure the area chart settings by clicking the **Plot Options...** button below the chart. A few controls will appear on the screen. Set the values for these controls as shown below:
# MAGIC 
# MAGIC * Select **RegistrationDate** for **Keys**.
# MAGIC * Select **count(UserId)** for **Values**.
# MAGIC * Select **MAX** for **Aggregation**.
# MAGIC 
# MAGIC <img src="https://databricksdemostore.blob.core.windows.net/images/02-SQL-DW/databricks-display-graph-options.png" style="border: 1px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>
# MAGIC 
# MAGIC You should see an area chart that looks like the following:
# MAGIC 
# MAGIC <img src="https://databricksdemostore.blob.core.windows.net/images/02-SQL-DW/databricks-display-graph.png" style="border: 1px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(UserId), Year(RegisteredDate) as RegistrationDate FROM users
# MAGIC GROUP BY Year(RegisteredDate) ORDER BY Year(RegisteredDate)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, create a bar chart of the number of registrations by age.

# COMMAND ----------

# MAGIC %md
# MAGIC Execute the cell below, then select the **Bar** chart this time.
# MAGIC 
# MAGIC Configure the bar chart settings by clicking the **Plot Options...** button below the chart. Set the values for these controls as shown below:
# MAGIC 
# MAGIC * Select **Age** for **Keys**.
# MAGIC * Select **count(UserId)** for **Values**.
# MAGIC * Select **MAX** for **Aggregation**.
# MAGIC 
# MAGIC <img src="https://databricksdemostore.blob.core.windows.net/images/02-SQL-DW/databricks-display-chart-options.png" style="border: 1px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>
# MAGIC 
# MAGIC You should see a bar chart that looks like the following:
# MAGIC 
# MAGIC <img src="https://databricksdemostore.blob.core.windows.net/images/02-SQL-DW/databricks-display-chart.png" style="border: 1px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(UserId), Age FROM Users GROUP BY Age ORDER BY Age

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC By this point, you should have a good grasp on how to process data using Spark on Azure Databricks. The next lesson will build upon these concepts by connecting to your Azure SQL Data Warehouse instance and writing this data to a new external table.
# MAGIC 
# MAGIC Start the next lesson, [Understanding the SQL DW Connector]($./02-Understanding-the-SQL-DW-Connector ).