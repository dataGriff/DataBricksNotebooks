# Databricks notebook source
# MAGIC %md
# MAGIC #Databricks Notebook Fundamentals

# COMMAND ----------

# MAGIC %md A **notebook** is a collection **cells**. These cells are run to execute code, render formatted text or display graphical visualizations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding Code Cells and Markdown Cells

# COMMAND ----------

# MAGIC %md The following cell (with the gray text area) is a code cell.

# COMMAND ----------

# This is a code cell
# By default, a new cell added in a notebook is a code cell
1 + 1

# COMMAND ----------

# MAGIC %md In this case, the code is written in Python. The default language for a cell is provided by the notebook, as can be seen by observing the title of the notebook, near the top left of the window. In this case it reads `01 Notebook Fundamentals (Python)`. The notebook language is always shown in parenthesis following the notebook title.
# MAGIC 
# MAGIC In order to run a notebook cell, your notebook must be attached to a cluster. If your notebook is not attached to a cluster, you will be prompted to do so before the cell can run.
# MAGIC 
# MAGIC To attach a notebook to a cluster:
# MAGIC 
# MAGIC 1. In the notebook toolbar, click ![Clusters Icon Detached Cluster Dropdown](https://docs.azuredatabricks.net/_images/cluster-icon.png) **Detached** ![](https://docs.azuredatabricks.net/_images/down-arrow.png).
# MAGIC 2. From the drop-down, select a cluster.
# MAGIC 
# MAGIC The code cell above has not run yet, so the expressions of `1 + 1` has not been evaluated. To run the code cell, select the cell by placing your cursor within the cell text area and do any of the following:
# MAGIC - Press `Shift` + `Enter` (to run the current cell and advance to the next cell)
# MAGIC - Press `Ctrl` + `Enter` (to run the current cell, but keep the current cell selected)
# MAGIC - Use the cell actions menu that is found at the far right of the cell to select the run cell option: ![Run Cell](https://docs.azuredatabricks.net/_images/cmd-run.png)

# COMMAND ----------

# MAGIC %md The following cell is another example of a code cell. Run it to see it's output.

# COMMAND ----------

# This is also a code cell
print("Welcome to the fabulous world of Databricks!")

# COMMAND ----------

# MAGIC %md The following cell, which displays its output as formatted text is a [markdown](https://en.wikipedia.org/wiki/Markdown) cell. 

# COMMAND ----------

# MAGIC %md
# MAGIC This is a *markdown* cell.
# MAGIC 
# MAGIC To create a markdown cell you need to use a **"magic"** command which is the short name of a Databricks magic command. 
# MAGIC 
# MAGIC Magic commands start with ``%``. 
# MAGIC 
# MAGIC The magic for markdown is `%md`. 
# MAGIC 
# MAGIC The magic commmand must always be the first text within the cell.
# MAGIC 
# MAGIC The following provides the list of supported magics:
# MAGIC * `%python` - Allows you to execute **Python** code in the cell.
# MAGIC * `%r` - Allows you to execute **R** code in the cell.
# MAGIC * `%scala` - Allows you to execute **Scala** code in the cell.
# MAGIC * `%sql` - Allows you to execute **SQL** statements in the cell.
# MAGIC * `sh` - Allows you to execute **Bash Shell** commmands and code in the cell.
# MAGIC * `fs` - Allows you to execute **Databricks Filesystem** commands in the cell.
# MAGIC * `md` - Allows you to render **Markdown** syntax as formatted content in the cell.
# MAGIC * `run` - Allows you to **[run another notebook](https://docs.azuredatabricks.net/user-guide/notebooks/notebook-use.html#run-a-notebook-from-another-notebook)** from a cell in the current notebook.
# MAGIC 
# MAGIC To read more about magics see [here](https://docs.azuredatabricks.net/user-guide/notebooks/notebook-use.html#develop-notebooks).

# COMMAND ----------

# MAGIC %md Double click on the above cell (or select the cell and press `ENTER`) and notice how the cell changes to an editable code cell. Observe that the first line starts with `%md`. This magic instructs the notebook not to run the cell contents as python, but instead to render the contents from the markdown syntax.
# MAGIC 
# MAGIC To render the markdown, either run the cell, select another cell or press the `Esc` key. 

# COMMAND ----------

# MAGIC %md #### Supported Markdown content

# COMMAND ----------

# MAGIC %md Markdown notebook cells in Azure Databricks support a wide variety of content that help your notebook convey more than just code, these capabilities include:
# MAGIC - [Linking to other notebooks](https://docs.azuredatabricks.net/user-guide/notebooks/notebook-use.html#link-to-other-notebooks)
# MAGIC - [Displaying images](https://docs.azuredatabricks.net/user-guide/notebooks/notebook-use.html#display-images)
# MAGIC - [Displaying mathematical equations](https://docs.azuredatabricks.net/user-guide/notebooks/notebook-use.html#display-mathematical-equations) using the KaTeX syntax
# MAGIC - [Displaying HTML content (including SVG and D3)](https://docs.azuredatabricks.net/user-guide/visualizations/html-d3-and-svg.html)

# COMMAND ----------

# MAGIC %md Try running the cell below to see how display some HTML code that loads the Bing website in an iFrame.

# COMMAND ----------

displayHTML("<iframe src='https://bing.com' width='100%' height='350px'/>")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding cell output

# COMMAND ----------

# MAGIC %md By default, a notebook cell will output the value of evaluating the last line the cell.

# COMMAND ----------

# MAGIC %md Run the following cell. Observe that the entire cell is echoed in the output because the cell contains only one line.

# COMMAND ----------

"Hello Databricks world!"

# COMMAND ----------

# MAGIC %md Next, examine the following cell. What do you expect the output to be? Run the cell and confirm your understanding.

# COMMAND ----------

"Hello Databricks world!"
"And, hello Microsoft Ignite!"

# COMMAND ----------

# MAGIC %md If you want to ensure your output displays something, use the `print` method.

# COMMAND ----------

print("Hello Databricks world!")
print("And, hello Microsoft Ignite!")

# COMMAND ----------

# MAGIC %md Not all code lines return a value to be output. Run the following cell to see one such an example.

# COMMAND ----------

text_variable = "Hello, hello!"

# COMMAND ----------

# MAGIC %md ## Running multiple notebook cells

# COMMAND ----------

# MAGIC %md It is not uncommon to need to run (or re-run) a subset of all notebook cells in top to bottom order.
# MAGIC 
# MAGIC There are a few ways you can accomplish this. You can:
# MAGIC - Run all cells in the notebook. Select `Run All` in the notebook toolbar to run all cells starting from the first.
# MAGIC - Run cells above or below your current cell. To run all cells above or below a cell, go to the cell actions menu at the far right, select Run Menu, and then select Run All Above or Run All Below. ![](https://docs.azuredatabricks.net/_images/cmd-run.png)

# COMMAND ----------

# MAGIC %md ## Navigating Cells
# MAGIC 
# MAGIC You can quickly navigate thru the cells in the notebook by using the `up` or `down` arrows. If your cursor is within a code cell, keep pressing up or down until your reach the top or bottom edge of the cell respectively and the focus will automatically jump to the next cell.
# MAGIC 
# MAGIC When you navigate cells, observe the selected cell is highlighted with a darker black border.

# COMMAND ----------

# MAGIC %md Select this cell by single clicking. Then try navigating up 3 cells and then return to this cell once you have tried navigation out.

# COMMAND ----------

# MAGIC %md When you navigated between cells without entering into the mode where you could edit the cell, you were in `command mode`. 
# MAGIC 
# MAGIC When the editable text area has the cursor within it, you are in `edit mode`
# MAGIC 
# MAGIC There are different functions and keyboard shortcuts available depending on which mode you are in.

# COMMAND ----------

# MAGIC %md ## Managing notebook Cells

# COMMAND ----------

# MAGIC %md You can perform many tasks within a notebook by using the UI or by using the keyboard shortcuts.
# MAGIC 
# MAGIC To learn the shortcuts (or refresh your memory later), you can select the ![Shortcuts](https://docs.azuredatabricks.net/_images/keyboard.png) from the top right of notebook to display a dialog that lists all of the keyboard shortcuts.

# COMMAND ----------

# MAGIC %md ### Keyboard shortcuts for adding and removing cells

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC When a cell is in command mode press the following keys:
# MAGIC 
# MAGIC * `A` (insert a cell above)
# MAGIC * `B` (insert a cell below)
# MAGIC * `D`,`D` (delete current cell)
# MAGIC 
# MAGIC Try the following in your notebook:
# MAGIC 1. Insert a cell below this cell (`B`)
# MAGIC 2. Insert a cell above that new cell (`Esc`, `A`)
# MAGIC 3. Delete both new cells (`Esc`, `D`,`D`)

# COMMAND ----------

# MAGIC %md ### Adding and removing cells using the UI

# COMMAND ----------

# MAGIC %md You can also add and remove cells using the UI. 
# MAGIC 
# MAGIC To add a cell, mouse over a cell at the top or bottom and click the ![Add Cell icon](https://docs.azuredatabricks.net/_images/add-cell.png).
# MAGIC 
# MAGIC Alternately, access the notebook cell menu at the far right, select the down `caret`, and select `Add Cell Above` or `Add Cell Below` ![](https://docs.azuredatabricks.net/_images/cmd-edit.png)
# MAGIC 
# MAGIC  

# COMMAND ----------

# MAGIC %md ### Adjusting cell order

# COMMAND ----------

# MAGIC %md You can move cells up or down within the notebook to fit your needs. 
# MAGIC 
# MAGIC You can do so using the UI or via keyboard shortcuts.
# MAGIC 
# MAGIC When in command mode, you can cut and paste entire cells using keyboard shortcuts. To do so, select the cell and then press `X` to cut it. Use the `up` or `down` arrow keys to find the cell around which it should be pasted. Press `V` to paste the cell below the selected cell or press `SHIFT` + `V` to paste the cell above the selected cell.
# MAGIC 
# MAGIC You can also move cells using the UI, by accessing the notebook cell menu at the far right, selecting the down `caret` and then selecting either `Move Up` or `Move Down` or the `Cut Cell` and `Paste Cell` options.

# COMMAND ----------

# MAGIC %md Experiment with the commands for re-ordering cells by adjusting the order of the following cells so that they appear in the order 1, 2, 3.

# COMMAND ----------

# MAGIC %md 1 - This cell should appear first.

# COMMAND ----------

# MAGIC %md 2 - This cell should appear second.

# COMMAND ----------

# MAGIC %md 3 - This cell should appear third.

# COMMAND ----------

# MAGIC %md ## Understanding notebook state

# COMMAND ----------

# MAGIC %md When you execute notebook cells, their execution is backed by a process running on a cluster. The state of your notebook, such as the values of variables, is maintained in the process. All variables default to a global scope (unless you author your code so it has nested scopes) and this global state can be a little confusing at first when you re-run cells.

# COMMAND ----------

# MAGIC %md Run the following two cells in order and take note of the value ouput for the variable `y`:

# COMMAND ----------

x = 10

# COMMAND ----------

y = x + 1
y

# COMMAND ----------

# MAGIC %md Next, run the following cell.

# COMMAND ----------

x = 100

# COMMAND ----------

# MAGIC %md Now select the cell that has the lines `y = x + 1` and `y`. And re-run that cell. Did the value of `y` meet your expectation? 
# MAGIC 
# MAGIC The value of `y` should now be `101`. This is because it is not the actual order of the cells that determines the value, but the order in which they are run and how that affects the underlying state itself. To understand this, realize that when the code `x = 100` was run, this changed the value of `x`, and then when you re-ran the cell containing `y = x + 1` this evaluation used the current value of x which is 100. This resulted in `y` having a value of `101` and not `11`. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clearing state and output
# MAGIC 
# MAGIC You can use the **Clear** dropdown on the notebook toolbar remove output (results) or remove output and clear the underlying state. ![](https://docs.databricks.com/_images/clear-notebook.png)
# MAGIC 
# MAGIC * Clear -> Clear Results (removes the displayed output for all cells)
# MAGIC * Clear -> Clear State (removes all cell states)
# MAGIC 
# MAGIC You typically do this when you want to cleanly re-run a notebook you have been working on and eliminate any accidental changes to the state that may have occured while you were authoring the notebook.
# MAGIC 
# MAGIC Read more about execution context [here](https://docs.azuredatabricks.net/user-guide/notebooks/notebook-use.html#run-notebooks).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Introducing Spark DataFrames

# COMMAND ----------

# MAGIC %md Let's create a simple DataFrame containing one column

# COMMAND ----------

df = spark.range(1000).toDF("number")
display(df)

# COMMAND ----------

# MAGIC %md Let's take a look at its content

# COMMAND ----------

df.describe().show()

# COMMAND ----------

# MAGIC %md Get access to a specific column

# COMMAND ----------

df["number"]

# COMMAND ----------

# MAGIC %md And to all columns

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md Take a look at the schema of the DataFrame

# COMMAND ----------

df.schema

# COMMAND ----------

# MAGIC %md Apply a simple projection and a simple filter to the DataFrame

# COMMAND ----------

df.select("number").show(15)

# COMMAND ----------

# MAGIC %md Apply a more explicit filter

# COMMAND ----------

df.where("number > 10 and number < 14").show()

# COMMAND ----------

# MAGIC %md Notice the difference between `df.where("number > 10 and number < 14")` and the same expression followed by `.show()` or `.count()`. This is one core property of DataFrames called lazy evaluation (more about it later in the day). Try it yourself in the next cell.

# COMMAND ----------

# Your code goes here
df.where("number > 10 and number < 14").count()

# COMMAND ----------

# MAGIC %md And finally, let's have some fun with sampling

# COMMAND ----------

seed = 10
withReplacement = False
fraction = 0.02
df.sample(withReplacement, fraction, seed).show(100)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Introducting Pandas DataFrames

# COMMAND ----------

# MAGIC %md So far we've used Spark DataFrames which are the de facto choice in Databricks. Unless you are used to work with Pandas, in which case the next line will make you feel at home:

# COMMAND ----------

pdf = df.toPandas()
pdf.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC Let's create a new Pandas DataFrame

# COMMAND ----------

import pandas as pd
import numpy as  np
pdf = pd.DataFrame(data={'ColumnA':np.linspace(0, 1, 11),
                        'ColumnB':['red', 'yellow','blue', 'green', 'red', \
                                   'green','green','red', 'yellow','blue', 'green'],
                        'ColumnC': np.random.randint(11)}) 
pdf

# COMMAND ----------

#One column
pdf['ColumnA']

# COMMAND ----------

#Single value
pdf.loc[0, 'ColumnA']

# COMMAND ----------

#Slicing by index
pdf.loc[3:7:2]

# COMMAND ----------

pdf['ColumnB']=='red'

# COMMAND ----------

#Boolean mask
pdf[pdf['ColumnB']=='red']

# COMMAND ----------

#Boolean mask multiple conditions
pdf[(pdf['ColumnB']=='red') & (pdf['ColumnA']==0.4)]

# COMMAND ----------

# MAGIC %md 
# MAGIC This concludes the Notebook Fundamentals lab.
# MAGIC 
# MAGIC Through this lab you've been introduced to the core concepts related to Databricks notebooks structure and execution (cluster connections, cells, execution flow, and state).
# MAGIC 
# MAGIC You have also tested some simple features of the core Databricks data modeling element - the Spark DataFrame and of it's traditional Python counterpart, the Pandas DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Why Apache Spark?]($./02%20Why%20Spark)