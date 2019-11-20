# Databricks notebook source
# MAGIC %md 
# MAGIC # Data Transformation via Azure Data Factory
# MAGIC 
# MAGIC As you saw at the end of the previous lesson, different cities use different field names and values to indicate crimes, dates, etc. within their crime data.
# MAGIC 
# MAGIC For example:
# MAGIC * Some cities use the value "HOMICIDE", "CRIMINAL HOMICIDE" or "MURDER".
# MAGIC * In the New York data, the column is named `offenseDescription` while in the Boston data, the column is named `OFFENSE_CODE_GROUP`.
# MAGIC * In the New York data, the date of the event is in the `reportDate`, while in the Boston data, there is a single column named `MONTH`.
# MAGIC 
# MAGIC In the case of New York and Boston, here are the unique characteristics of each data set:
# MAGIC 
# MAGIC | | Offense-Column        | Offense-Value          | Reported-Column  | Reported-Data Type |
# MAGIC |-|-----------------------|------------------------|-----------------------------------|
# MAGIC | New York | `offenseDescription`  | starts with "murder" or "homicide" | `reportDate`     | `timestamp`    |
# MAGIC | Boston | `OFFENSE_CODE_GROUP`  | "Homicide"             | `MONTH`          | `integer`      |
# MAGIC 
# MAGIC In this notebook, we will use an ADF Databricks Notebooks activity to perform transformations on and extract homicide statistics from the crime data being ingested via ADF.
# MAGIC 
# MAGIC In this lesson you:
# MAGIC 1. Create Databricks Access Token.
# MAGIC 2. Add Databricks Notebook activity to pipeline.
# MAGIC 3. Connect Copy Activities to Notebook Activity.
# MAGIC 4. Publish the updated pipeline.
# MAGIC 5. Trigger and Monitor the pipeline run.
# MAGIC 6. Verify transformations of data by looking at the generated table in Databricks.
# MAGIC 7. Perform a simple aggregation of the data.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 1: Create Databricks Access Token
# MAGIC 
# MAGIC In this step, you will generate an access token that will be used by ADF to access your Databricks workspace.
# MAGIC 
# MAGIC In the top right corner of your Databricks workspace (this window), select the Account icon, and then select **User Settings**.
# MAGIC   
# MAGIC   ![Databricks account menu](https://databricksdemostore.blob.core.windows.net/images/03/03/databricks-account-menu.png "Databricks account menu")
# MAGIC   
# MAGIC On the User Settings screen, select Generate New Token.
# MAGIC   
# MAGIC   ![User Settings](https://databricksdemostore.blob.core.windows.net/images/03/03/user-settings.png)
# MAGIC 
# MAGIC In the Generate New Token dialog, enter a comment, such as "ADF access", leave the Lifetime set to 90 days, and select **Generate**.
# MAGIC   
# MAGIC   ![Generate New Token](https://databricksdemostore.blob.core.windows.net/images/03/03/generate-new-token.png)
# MAGIC   
# MAGIC Copy the generated token and paste it into a text editor, such as Notepad.exe, for use when setting up the Databricks Linked Service in ADF.
# MAGIC 
# MAGIC > Note: Do not close the dialog containing the generated token until have have save it in a text editor, as it will not be visible again once you close the dialog.
# MAGIC   
# MAGIC   ![Copy Token](https://databricksdemostore.blob.core.windows.net/images/03/03/copy-token.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 2: Add Databricks Notebook Activity To Pipeline
# MAGIC 
# MAGIC ADF provides several Databricks-related activities, including Notebook, Jar, and Python activities, design to allow automated transformation of data using ADF and Databricks. In this exercise, you will add a Databricks Notebook activity to your ADF pipeline which will run the [Databricks-Data-Transformations.dbc]($./includes/Databricks-Data-Transformations) notebook, located in the `/includes` directory of the lab files you uploaded to your Databricks workspace.
# MAGIC 
# MAGIC Take a few minutes to review the contents of this notebook to understand what transformations are being applied to the datasets being ingested into your Blob storage account via ADF. Also, notice that the notebook defines widgets named `accountKey`, `accountName`, and `containerName`. You will be passing these values in as parameters from ADF so the notebook can connect to your Azure Storage account.
# MAGIC 
# MAGIC ![ADF Widgets](https://databricksdemostore.blob.core.windows.net/images/03/03/adf-widgets.png "ADF Widgets")
# MAGIC 
# MAGIC To modify your pipeline, return to the ADF UI in a web browser and select the Author (pencil) icon in the left-hand menu, and then select your pipeline under Pipelines.
# MAGIC 
# MAGIC ![ADF UI Author blade](https://databricksdemostore.blob.core.windows.net/images/03/03/adf-author-blade.png "ADF UI Author blade")
# MAGIC 
# MAGIC Once there, expand Databricks under Activities, then drag the Notebook activity onto the design surface, and drop it to the right of the existing copy activity.
# MAGIC 
# MAGIC ![Add Databricks Notebook activity](https://databricksdemostore.blob.core.windows.net/images/03/03/adf-add-activity-databricks-notebook.png "Add Databricks Notebook activity")
# MAGIC 
# MAGIC Select the Notebook activity on the design surface to display tabs containing its properties and settings at the bottom of the screen, then enter the following:
# MAGIC 
# MAGIC ### General tab
# MAGIC 
# MAGIC 1. Enter "LabNotebook" into the Name field
# MAGIC 
# MAGIC   ![Notebook Activity General Tab](https://databricksdemostore.blob.core.windows.net/images/03/03/notebook-activity-general.png)
# MAGIC 
# MAGIC ### Azure Databricks tab
# MAGIC 
# MAGIC 1. Select **+New** next to Databricks Linked Service to create a new Linked Service
# MAGIC 
# MAGIC   ![Create New Linked Service](https://databricksdemostore.blob.core.windows.net/images/03/03/notebook-activity-azure-databricks-new.png "Create New Linked Service")
# MAGIC 
# MAGIC 2. On the New Linked Service dialog, enter the following:
# MAGIC   - **Name**: enter a name, such as AzureDatabricks. 
# MAGIC   - **Databricks workspace**: Select the workspace you are using for this lab.
# MAGIC   - **Select cluster**: Choose Existing interactive cluster.
# MAGIC   - **Access token**: Paste the token you generated in step one into this field.
# MAGIC   - **Existing cluster id**: Select the Databricks cluster you are using for this lab.
# MAGIC   - Select **Test connection** and ensure you get a Connection successful message.
# MAGIC   - Select **Finish**.
# MAGIC   
# MAGIC   ![New Linked Service (Azure Databricks)](https://databricksdemostore.blob.core.windows.net/images/03/03/adf-linked-service-databricks-notebook.png "New Linked Service (Azure Databricks)")
# MAGIC 
# MAGIC 3. You should now see the AzureDatabricks Linked Service selected in the **Databricks Linked Service** drop down.
# MAGIC 
# MAGIC   ![Databricks Notebook activity Azure Databricks tab](https://databricksdemostore.blob.core.windows.net/images/03/03/adf-activity-databricks-notebook-databricks-tab.png "Databricks Notebook activity Azure Databricks tab")
# MAGIC 
# MAGIC ### Settings tab
# MAGIC 
# MAGIC 1. **Notebook path**: Enter "/Users/[your-user-account]/03-Data-Ingestion-Via-ADF/includes/Databricks-Data-Transformations" or select Browse, and select the `Databricks-Data-Transformations` notebook from the `/Users/[your-user-account]/03-Data-Ingestion-Via-ADF/includes` folder.
# MAGIC 2. Expand Base Parameters.
# MAGIC 3. Select **+New**.
# MAGIC 4. Enter "accountName" as the parameter Name, and the name of the Storage account you created into the Value.
# MAGIC 5. Select **+New** again, and enter "accountKey" as the Name of the second parameter, and paste in the Key for your storage account into the Value.
# MAGIC 6. Select **+New** again, and enter "containerName" as the name of the second parameter, and enter "dwtemp" for its Value.
# MAGIC 
# MAGIC ![Databricks Notebook activity Settings tab](https://databricksdemostore.blob.core.windows.net/images/03/03/adf-activity-databricks-notebook-settings.png "Databricks Notebook activity Settings tab")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Connect Copy Activity to Notebook Activity
# MAGIC 
# MAGIC The final step is to connect the Copy activity with the Notebook activity. Select the small green box on the right-hand side of the copy activity, and drag the arrow onto the Notebook activity on the design surface. Creating this connection sets the output from the copy activity as required input for the Databricks Notebook activity. What this means is that the copy activity has to successfully complete processing and generate its files in your storage account before the Notebook activity runs, ensuring the files required by that Notebook activity are in place at the time of execution.
# MAGIC 
# MAGIC The finished pipeline should look like the following.
# MAGIC 
# MAGIC ![Complete pipeline](https://databricksdemostore.blob.core.windows.net/images/03/03/data-ingestion-pipeline-complete.png "Complete pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Publish Pipeline
# MAGIC 
# MAGIC In the ADF UI toolbar, select **Validate All** to validate the pipeline, and then select **Publish All** to save the changes to the pipeline.
# MAGIC 
# MAGIC ![Publish All](https://databricksdemostore.blob.core.windows.net/images/03/03/adf-publish-all.png "Publish All")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Trigger and Monitor the Pipeline Run
# MAGIC 
# MAGIC Once the changes have been published, trigger the pipeline by selecting **Trigger**, then **Trigger Now** on your pipeline.
# MAGIC 
# MAGIC ![Trigger ADF Pipeline](https://databricksdemostore.blob.core.windows.net/images/03/03/adf-pipeline-trigger.png "Trigger ADF Pipeline")
# MAGIC 
# MAGIC Next, navigate to the Monitor screen by selecting the Monitor icon in the left-hand menu.
# MAGIC 
# MAGIC ![Monitor ADF Pipeline](https://databricksdemostore.blob.core.windows.net/images/03/03/adf-monitor-pipeline.png "Monitor ADF Pipeline")
# MAGIC 
# MAGIC Select the View Activity Runs icon under Actions for the active pipeline run. Here you can observe the ADF pipeline activities running. Notice the Databricks Notebook activity will not run until of the copy activity has completed. Once done, you will see a Succeeded message next to each activity.
# MAGIC 
# MAGIC ![ADF Activity Runs](https://databricksdemostore.blob.core.windows.net/images/03/03/databricks-activity-runs.png "ADF Activity Runs")
# MAGIC 
# MAGIC To view the notebook output of the run, you can select the Output Action next to the LabNotebook activity, and then select the runPageUrl value. This will open the executed notebook within Databricks, and you can view details of the run in each cell's output.
# MAGIC 
# MAGIC ![Databricks Notebook Output](https://databricksdemostore.blob.core.windows.net/images/03/03/adf-output-databricks-notebook.png "Databricks Notebook output")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 6: Verify transformations of data by looking at the generated table in Databricks
# MAGIC 
# MAGIC Once the ADF pipleline has finished running, you can check the table by running the following command.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/>Remember to attach your notebook to a cluster before running any cells in your notebook. In the notebook's toolbar, select the drop down arrow next to Detached, then select your cluster under Attach to.
# MAGIC 
# MAGIC ![Attached to cluster](https://databricksdemostore.blob.core.windows.net/images/03/03/databricks-cluster-attach.png)

# COMMAND ----------

# Display that list of tables, filtering for users, products, and weblogs tables, to ensure they aren't buried among other tables in the results
sqlContext.tables().filter("tableName = 'homicides_2016'").show()

# COMMAND ----------

# MAGIC %md 
# MAGIC You should see the `homicides_2016` tables for `weblogs`, `users` and `products`. You can inspect the data in each table by running the cells below.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM homicides_2016 LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Aggregate the data
# MAGIC 
# MAGIC Now that you have a normalized dataset containing the data for all cities, you can aggregate the data to compute the number of homicides per month, by city.
# MAGIC 
# MAGIC Start by creating a new DataFrame containing the homicide data.

# COMMAND ----------

homicidesDf = spark.sql("SELECT * FROM homicides_2016")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Perform a simple aggregation to see the number of homicides per month by city.
# MAGIC 
# MAGIC After running the below query, play around with using Visualizations to represent the data, such as the following:
# MAGIC 
# MAGIC   ![Databricks visualizations](https://databricksdemostore.blob.core.windows.net/images/03/03/databricks-aggregation-visualization.png "Databricks visualizations")

# COMMAND ----------

display(homicidesDf.select("city", "month").orderBy("city", "month").groupBy("city", "month").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulations! 
# MAGIC 
# MAGIC You have completed the Data Ingestion via ADF module.
# MAGIC 
# MAGIC In the lab you have learned how to use Azure Data Factory to ingest a remote dataset, and use Azure Databricks activities to address issues with the data, clean and format the data and load it into a global table to support downstream analytics.