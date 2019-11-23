# Databricks notebook source
# MAGIC %md 
# MAGIC # Key Vault-backed secret scopes
# MAGIC 
# MAGIC Azure Databricks has two types of secret scopes: Key Vault-backed and Databricks-backed. These secret scopes allow you to store secrets, such as database connection strings, securely. If someone tries to output a secret to a notebook, it is replaced by `[REDACTED]`. This helps prevent someone from viewing the secret or accidentally leaking it when displaying or sharing the notebook.
# MAGIC 
# MAGIC In this lesson, you will learn how to configure a Key Vault-backed secret scope. You will use Key Vault to store sensitive connection information for Azure SQL Database and Azure Cosmos DB. This notebook walks you through the required steps to create the Key Vault, Azure SQL Database, and Cosmos DB resources, and how to create a Key Vault-backed secret scope.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Setup
# MAGIC 
# MAGIC To begin, you will need to create the required resources in the Azure portal. Follow the steps below to create a Key Vault service, Azure SQL Database, and Azure Cosmos DB.
# MAGIC 
# MAGIC ### Azure Key Vault
# MAGIC 
# MAGIC 1. Open <https://portal.azure.com> in a new browser tab.
# MAGIC 2. Select the **Create a resource** option on the upper left-hand corner of the Azure portal
# MAGIC 
# MAGIC     ![Output after Key Vault creation completes](https://databricksdemostore.blob.core.windows.net/images/04-MDW/create-keyvault.png)
# MAGIC 3. In the Search box, enter **Key Vault**.
# MAGIC 4. From the results list, choose **Key Vault**.
# MAGIC 5. On the Key Vault section, choose **Create**.
# MAGIC 6. On the **Create key vault** section provide the following information:
# MAGIC     - **Name**: A unique name is required. For this quickstart we use **Databricks-Demo**. 
# MAGIC     - **Subscription**: Choose a subscription.
# MAGIC     - Under **Resource Group** select the resource group under which your Azure Databricks workspace is created.
# MAGIC     - In the **Location** pull-down menu, choose the same location as your Azure Databricks workspace.
# MAGIC     - Check the **Pin to dashboard** checkbox.
# MAGIC     - Leave the other options to their defaults.
# MAGIC     
# MAGIC     ![Key Vault creation form](https://databricksdemostore.blob.core.windows.net/images/04-MDW/create-keyvault-form.png)
# MAGIC 7. After providing the information above, select **Create**.
# MAGIC 8. After Key Vault is created, open it and navigate to **Properties**.
# MAGIC 9. Copy the **DNS Name** and **Resource ID** properties and paste them to Notebook or some other text application that you can **reference later**.
# MAGIC 
# MAGIC     ![Key Vault properties](https://databricksdemostore.blob.core.windows.net/images/04-MDW/keyvault-properties.png)
# MAGIC     
# MAGIC ### Azure SQL Database
# MAGIC 
# MAGIC 1. In the Azure portal, select the **Create a resource** option on the upper left-hand corner.
# MAGIC 
# MAGIC     ![Output after SQL Database creation completes](https://databricksdemostore.blob.core.windows.net/images/04-MDW/create-sql-database.png)
# MAGIC 2. In the Search box, enter **SQL Database**.
# MAGIC 3. From the results list, choose **SQL Database**.
# MAGIC 4. On the SQL Database section, choose **Create**.
# MAGIC 5. On the **Create SQL database** section provide the following information:
# MAGIC     - **Name**: A unique name is required. For this quickstart we use **Databricks-SQL**. 
# MAGIC     - **Subscription**: Choose a subscription.
# MAGIC     - Under **Resource Group** select the resource group under which your Azure Databricks workspace is created.
# MAGIC     - In the **Location** pull-down menu, choose the same location as your Azure Databricks workspace.
# MAGIC     - In the **Select source** pull-down menu, choose **Sample (AdventureWorksLT)**.
# MAGIC     - Under **Server**, either select an existing server or create a new one.
# MAGIC     - Under **Pricing tier**, select **Standard S0: 10 DTUs, 250 GB**.
# MAGIC     - Leave the other options to their defaults.
# MAGIC     
# MAGIC     ![SQL Database creation form](https://databricksdemostore.blob.core.windows.net/images/04-MDW/create-sql-database-form.png)
# MAGIC 6. After providing the information above, select **Create**.
# MAGIC 7. Copy the server's **Host Name**, the SQL **Database Name**, **Username**, and **Password** and paste them to Notebook or some other text application that you can **reference later**.
# MAGIC 
# MAGIC ### Azure Cosmos DB
# MAGIC 
# MAGIC 1. In the Azure portal, select the **Create a resource** option on the upper left-hand corner.
# MAGIC 
# MAGIC     ![Output after Azure Cosmos DB creation completes](https://databricksdemostore.blob.core.windows.net/images/04-MDW/create-cosmos-db.png)
# MAGIC 2. In the Search box, enter **Cosmos DB**.
# MAGIC 3. From the results list, choose **Cosmos DB**.
# MAGIC 4. On the SQL Database section, choose **Create**.
# MAGIC 5. On the **Create Cosmos DB** section provide the following information:
# MAGIC     - **Subscription**: Choose a subscription.
# MAGIC     - Under **Resource Group** select the resource group under which your Azure Databricks workspace is created.
# MAGIC     - **Account Name**: A unique name is required. For this quickstart we use **databricks-demo**.
# MAGIC     - In the **API** pull-down menu, choose **Core (SQL)**.
# MAGIC     - In the **Location** pull-down menu, choose the same location as your Azure Databricks workspace.
# MAGIC     - For **Geo-Redundancy**, select **Enable**.
# MAGIC     - For **Multi-region Writes**, select **Enable**.
# MAGIC     - Leave the other options to their defaults.
# MAGIC     
# MAGIC     ![Cosmos DB creation form](https://databricksdemostore.blob.core.windows.net/images/04-MDW/create-cosmos-db-form.png)
# MAGIC 6. After providing the information above, select **Review + create**.
# MAGIC 7. In the review section, select **Create**.
# MAGIC 8. After Cosmos DB is created, navigate to it and select **Data Explorer** on the left-hand menu.
# MAGIC 9. Select **New Database**, then provide the following options in the form:
# MAGIC     - **Database id**: demos
# MAGIC     - **Provision throughput**: checked
# MAGIC     - **Throughput**: 10000
# MAGIC     
# MAGIC     ![Cosmos DB New Database creation form](https://databricksdemostore.blob.core.windows.net/images/04-MDW/cosmos-db-new-database.png)
# MAGIC 10. Select **OK**, then select **New Collection** to add a collection to the database, then provide the following options in the form:
# MAGIC     - **Database id**: select **Use existing** then select **demos**
# MAGIC     - **Collection id**: documents
# MAGIC     - **Partition key**: partitionId
# MAGIC     - **Provision dedicated throughput for this collection**: unchecked
# MAGIC     - **Utilize all of the available database throughput for this collection**: checked
# MAGIC     
# MAGIC     ![Cosmos DB New Collection creation form](https://databricksdemostore.blob.core.windows.net/images/04-MDW/cosmos-db-new-collection.png)
# MAGIC 11. Select **OK**.
# MAGIC 12. Select **Keys** on the left-hand menu of your new Cosmos DB instance and copy the **URI** and **Primary Key** (or Secondary Key) values and paste them to Notebook or some other text application that you can **reference later**. What you need for connecting to Cosmos DB are:
# MAGIC     - **Endpoint**: Your Cosmos DB url (i.e. https://youraccount.documents.azure.com:443/)
# MAGIC     - **Masterkey**: The primary or secondary key string for you Cosmos DB account
# MAGIC     - **Database**: The name of the database (`demos`)
# MAGIC     - **Collection**: The name of the collection that you wish to query (`documents`)
# MAGIC     
# MAGIC     ![Cosmos DB Keys](https://databricksdemostore.blob.core.windows.net/images/04-MDW/cosmos-db-keys.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Configure Azure Databricks
# MAGIC 
# MAGIC Now that you have an instance of Key Vault up and running, it is time to let Azure Databricks know how to connect to it.
# MAGIC 
# MAGIC 1. The first step is to open a new web browser tab and navigate to https://<your_azure_databricks_url>#secrets/createScope (for example, https://westus.azuredatabricks.net#secrets/createScope).
# MAGIC 2. Enter the name of the secret scope, such as `key-vault-secrets`.
# MAGIC 3. Select **Creator** within the Manage Principal drop-down to specify only the creator (which is you) of the secret scope has the MANAGE permission.
# MAGIC 
# MAGIC   > MANAGE permission allows users to read and write to this secret scope, and, in the case of accounts on the Azure Databricks Premium Plan, to change permissions for the scope.
# MAGIC 
# MAGIC   > Your account must have the Azure Databricks Premium Plan for you to be able to select Creator. This is the recommended approach: grant MANAGE permission to the Creator when you create the secret scope, and then assign more granular access permissions after you have tested the scope.
# MAGIC 
# MAGIC 4. Enter the **DNS Name** (for example, https://databricks-demo.vault.azure.net/) and **Resource ID** you copied earlier during the Key Vault creation step, for example: `/subscriptions/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx/resourcegroups/azure-databricks/providers/Microsoft.KeyVault/vaults/Databricks-Demo`. If this is a preconfigured environment, you do not need to complete this step.
# MAGIC 
# MAGIC   ![Create Secret Scope form](https://databricksdemostore.blob.core.windows.net/images/04-MDW/create-secret-scope.png 'Create Secret Scope')
# MAGIC 5. Select **Create**.
# MAGIC 
# MAGIC After a moment, you will see a dialog verifying that the secret scope has been created.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create secrets in Key Vault
# MAGIC 
# MAGIC To create secrets in Key Vault that can be accessed from your new secret scope in Databricks, you need to either use the Azure portal or the Key Vault CLI. For simplicity's sake, we will use the Azure portal:
# MAGIC 
# MAGIC 1. In the Azure portal, navigate to your Key Vault instance.
# MAGIC 2. Select **Secrets** in the left-hand menu.
# MAGIC 3. Select **+ Generate/Import** in the Secrets toolbar.
# MAGIC 4. Provide the following in the Create a secret form, leaving all other values at their defaults:
# MAGIC   - **Name**: sql-username
# MAGIC   - **Value**: the username for your Azure SQL server
# MAGIC   
# MAGIC   ![Create a secret form](https://databricksdemostore.blob.core.windows.net/images/04-MDW/create-secret.png 'Create a secret')
# MAGIC 5. Select **Create**.
# MAGIC 
# MAGIC **Repeat steps 3 - 5** to create the following secrets:
# MAGIC 
# MAGIC | Secret name | Secret value |
# MAGIC | --- | --- |
# MAGIC | sql-password | the password for your Azure SQL server |
# MAGIC | cosmos-uri | the URI value for your Azure Cosmos DB instance |
# MAGIC | cosmos-key | the key value for your Azure Cosmos DB instance |
# MAGIC 
# MAGIC When you are finished, you should have the following secrets listed within your Key Vault instance:
# MAGIC 
# MAGIC ![List of Key Vault secrets](https://databricksdemostore.blob.core.windows.net/images/04-MDW/keyvault-secrets.png 'Key Vault secrets')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Access Key Vault secrets in Azure Databricks
# MAGIC 
# MAGIC Now that you have created your secrets, it's time to access them from within Azure Databricks.
# MAGIC 
# MAGIC Run the following cell to retrieve and print a secret.

# COMMAND ----------

jdbcUsername = dbutils.secrets.get(scope = "key-vault-secrets", key = "sql-username")
print('Username: ' + jdbcUsername)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Notice that the value of `jdbcUsername` when printed out is `[REDACTED]`. This is to prevent your secrets from being exposed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [SQL Database - Connect using Key Vault]($./09-SQL-Database-Connect-Using-Key-Vault)