# Databricks notebook source
# MAGIC %md # Visualizing with Power BI

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying Data 
# MAGIC This lesson uses the `people-10m` data set, which is in Parquet format.
# MAGIC This data could be coming with Parquet, CSV, JSON and other formats ...
# MAGIC 
# MAGIC The data is fictitious; in particular, the Social Security numbers are fake.

# COMMAND ----------

# MAGIC %fs ls /mnt/training/dataframes/people-10m.parquet

# COMMAND ----------

peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")
display(peopleDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the schema with the printSchema method. This tells you the field name, field type, and whether the column is nullable or not (default is true).

# COMMAND ----------

peopleDF.printSchema()

# COMMAND ----------

# MAGIC %md ## Reference tables using DirectQuery in Power BI

# COMMAND ----------

# MAGIC %md Now that we have access to the data, we can start building the charts in Power BI. Power BI will allow us to quickly create charts and share them with other users.

# COMMAND ----------

# MAGIC %md ## Connect Power BI Desktop to your Databricks cluster
# MAGIC 
# MAGIC Complete the following steps to get started:
# MAGIC 
# MAGIC * Download and install [Power BI Desktop](https://powerbi.microsoft.com/desktop/)
# MAGIC * In your Databricks workspace, go to Clusters and select the cluster you want to connect
# MAGIC * On the cluster page, scroll down and select the **JDBC/ODBC** tab, then copy the JDBC URL
# MAGIC 
# MAGIC ![Cluster JDBC URL](https://raw.githubusercontent.com/solliancenet/Databricks-Labs/master/Labs/Lab02/images/databricks-cluster-jdbc-url.png)
# MAGIC 
# MAGIC * Now, you need to modify the JDBC URL to construct the JDBC server address that you will use to set up your Spark cluster connection in Power BI Desktop
# MAGIC * In the JDBC URL:
# MAGIC   * Replace `jdbc:hive2` with `https`
# MAGIC   * Remove everything in the path between the port number and `sql`, retaining the components indicated by the boxes in the image below
# MAGIC 
# MAGIC ![Parsed Cluster JDBC URL](https://raw.githubusercontent.com/solliancenet/Databricks-Labs/master/Labs/Lab02/images/databricks-cluster-jdbc-url-parsed.png)
# MAGIC 
# MAGIC   * In our example, the server address would be: `https://eastus2.azuredatabricks.net:443/sql/protocolv1/o/8821428530515879/0515-194607-bails931`
# MAGIC   
# MAGIC * Copy your server address

# COMMAND ----------

# MAGIC %md ## Configure and make the connection in Power BI Desktop
# MAGIC 
# MAGIC * From Power BI Desktop, select the dropdown under **Get Data**, then **More...**. In the Get Data window, select Other, then Spark
# MAGIC ![GetData](https://github.com/Microsoft/MCW-Big-data-and-visualization/raw/master/Hands-on%20lab/media/image178.png)
# MAGIC * Select Continue on the Preview connector dialog
# MAGIC 
# MAGIC ![Get Data](https://raw.githubusercontent.com/solliancenet/Databricks-Labs/master/Labs/Lab02/images/power-bi-get-data-dialog.png)
# MAGIC 
# MAGIC * Enter the Databricks server address you created above into the Server field
# MAGIC * Set the protocl to HTTP
# MAGIC * Select DirectQuery as the data connectivity mode, then select OK
# MAGIC 
# MAGIC ![Power BI Desktop Spark Connection](https://raw.githubusercontent.com/solliancenet/Databricks-Labs/master/Labs/Lab02/images/power-bi-spark-connection.png)
# MAGIC 
# MAGIC * Before you can enter credentials on the next screen, you need to create an Access token in Databricks
# MAGIC * In your Databricks workspace, select the Account icon in the top right corner, then select User settings from the menu.
# MAGIC   
# MAGIC ![Account menu](https://raw.githubusercontent.com/solliancenet/Databricks-Labs/master/Labs/Lab02/images/databricks-user-menu.png)
# MAGIC 
# MAGIC * On the User Settings page, select Generate New Token, enter "Power BI Desktop" in the comment, and select Generate
# MAGIC   
# MAGIC ![Account menu](https://raw.githubusercontent.com/solliancenet/Databricks-Labs/master/Labs/Lab02/images/databricks-generate-token.png)
# MAGIC 
# MAGIC * Copy the generated token, and save it as you will need it more than once below. **NOTE**: You will not be able to access the token in Databricks once you close the Generate token dialog, so be sure to save this value to a text editor or another location you can access during this lab.
# MAGIC 
# MAGIC * Back in Power BI Desktop, enter "token" for the user name, and paste the access token you copied from Databricks into the password field.
# MAGIC 
# MAGIC ![Power BI Desktop Spark Connection Login](https://raw.githubusercontent.com/solliancenet/Databricks-Labs/master/Labs/Lab02/images/power-bi-spark-connection-login.png)
# MAGIC 
# MAGIC * After authenticating, continue to the next step and check the box next to the `people10m` table, then select Load.
# MAGIC 
# MAGIC ![Get Data](https://databricksdemostore.blob.core.windows.net/images/07/PowerBINavigator.png)

# COMMAND ----------

# MAGIC %md ## Create the Power BI Report

# COMMAND ----------

# MAGIC %md Once the data finishes loading, you will see the fields appear on the far side of the Power BI Desktop client window
# MAGIC 
# MAGIC ![Fields](https://databricksdemostore.blob.core.windows.net/images/07/PowerBI-Fields.png)
# MAGIC 
# MAGIC From the Visualizations area, next to Fields, select the Stacked Column Chart icon to add it to the report design surface
# MAGIC 
# MAGIC ![Globe](https://databricksdemostore.blob.core.windows.net/images/07/PowerBI-Visualization.png)
# MAGIC 
# MAGIC With the stacked Column Chart still selected, drag the 'gender' field to the Axis field under Visualizations. Then Next, drag the salary field to the Value field under Visualizations.
# MAGIC 
# MAGIC ![numdays](https://databricksdemostore.blob.core.windows.net/images/07/PowerBI-stackedColumn.png)
# MAGIC 
# MAGIC You should now see a stacked Column Chart that looks similar to the following (resize and zoom on your map if necessary):
# MAGIC 
# MAGIC ![map](https://databricksdemostore.blob.core.windows.net/images/07/PowerBI-Graph1.png)
# MAGIC 
# MAGIC Unselect the Stacked Column Chart visualization by selecting the white space next to the chart in the report area
# MAGIC 
# MAGIC From the Visualizations area, select the Pie icon to add a Pie chart visual to the report's design surface
# MAGIC 
# MAGIC ![stackedColumn](https://databricksdemostore.blob.core.windows.net/images/07/PowerBI-Pie.png)
# MAGIC 
# MAGIC With the Pie Chart still selected, drag the gender field and drop it into the details field located under Visualizations
# MAGIC 
# MAGIC Next, drag the salary field over, and drop it into the Values field
# MAGIC 
# MAGIC ![dragnumdelays](https://databricksdemostore.blob.core.windows.net/images/07/PowerBI-Graph2.png)
# MAGIC 
# MAGIC You should see a pie chart that looks similar to the following
# MAGIC 
# MAGIC ![pie](https://databricksdemostore.blob.core.windows.net/images/07/PowerBI-Pie2.png)
# MAGIC 
# MAGIC You can save the report, by choosing Save from the File menu, and entering a name and location for the file
# MAGIC 
# MAGIC ![savereport](https://github.com/Microsoft/MCW-Big-data-and-visualization/raw/master/Hands-on%20lab/media/image197.png)

# COMMAND ----------

# MAGIC %md ## Conclusion

# COMMAND ----------

# MAGIC %md In this lab, you have learned how to:
# MAGIC 
# MAGIC * Set up a DirectQuery connection in Power BI to Spark
# MAGIC * Created visualizations in Power BI directly from the Spark data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Matplotlib]($./04-Matplotlib).