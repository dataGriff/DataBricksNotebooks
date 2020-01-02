# Databricks notebook source
# MAGIC %md
# MAGIC # Creating Visualizations using Matplotlib
# MAGIC Use the popular 3rd party Matplotlib library to expand your options for creating useful visualizations in Azure Databricks.
# MAGIC 
# MAGIC ## Instructions
# MAGIC 
# MAGIC In the Exploratory Data Analysis lesson, you normalized crime data, joined it with city population data to discover the per-capita crime rate, and saved the results to the `RobberyRatesByCity` table. You will use Matplotlib to visualize this data in different ways.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Data overview
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> When you completed the [Exploratory Data Analysis]($./02-Exploratory-Data-Analysis) notebook, you should have created the `RobberyRatesByCity` table. If not, please take a moment to complete that notebook before continuing.
# MAGIC 
# MAGIC Display the data stored in the `RobberyRatesByCity` table for the three cities (Los Angeles, Dallas, and Philadelphia) as a reminder of the nature of the robbery data we will display. The key columns to pay attention to are as follows:
# MAGIC 
# MAGIC | Column                    | Description |
# MAGIC | ------------------------- | ----------------------- |
# MAGIC | `robberies`               | Simple count of robberies for a given month |
# MAGIC | `city`                    | The city in which the robbery took place |
# MAGIC | `robberyRate`             | Indicates the robberies per capita (`robberies`/`estPopulation2016`) |

# COMMAND ----------

robberyAll = spark.sql("select * from RobberyRatesByCity")
display(robberyAll)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualizing your data with Matplotlib
# MAGIC 
# MAGIC You can display Matplotlib libraries within a Python notebook in Azure Databricks. Databricks saves plots as images in The FileStore. Because of this, you do not display a plot with the standard `plt.show()` command. Instead, use the `display(fig)` command.
# MAGIC 
# MAGIC Start out by importing all the libraries that will be used in this lesson.

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import itertools
from collections import OrderedDict
from functools import partial

import matplotlib.ticker as mticker
import matplotlib.cm as cm
from cycler import cycler

# COMMAND ----------

# MAGIC %md
# MAGIC ## Boxplot chart of all robberies
# MAGIC 
# MAGIC Using the Matplotlib library, we can display a Boxplot chart to give us a sense of the range of data included for all robberies. When you render the chart below, you will see that the central mark of the box is the median value, the top and bottom edges of the box are the lower hinge (25th percentile) and the upper hinge (75th percentile), and the top and bottom standalone lines (or whiskers) extend to the most extreme data points not considered outliers.
# MAGIC 
# MAGIC Your boxplot chart should look similar to the following:
# MAGIC 
# MAGIC ![Boxplot chart](https://databricksdemostore.blob.core.windows.net/images/07-MDW/boxplot-chart.png)

# COMMAND ----------

fig, ax = plt.subplots()

# create a new Pandas data frame from the roberyAll Spark Data Frame
pdf = robberyAll.toPandas()

# TODO

# Complete the line below by setting the plot kind to box and setting the title parameter):
pdf['robberies'].plot( # TODO: COMPLETE CODE

# TODO: DISPLAY THE CHART (fig)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Heatmap
# MAGIC 
# MAGIC Using Pandas, it is very easy to calculate the correlations between all features:
# MAGIC 
# MAGIC ```python
# MAGIC dataframe.corr()
# MAGIC ```
# MAGIC 
# MAGIC We only want the correlation for features that are not categorical (remember that we consider binary features as categorical).   
# MAGIC In our dataset, this corresponds to the features month, robberies, rankIn2016, and estPopulation2016.
# MAGIC 
# MAGIC When you run the cell below, you should see a chart similar to the following:
# MAGIC 
# MAGIC ![Boxplot chart](https://databricksdemostore.blob.core.windows.net/images/07-MDW/heatmap-chart.png)

# COMMAND ----------

fig, ax = plt.subplots()
sns.heatmap(pdf[['month','robberies','rankIn2016','estPopulation2016']].corr(),annot=True, center=0, cmap='BrBG', annot_kws={"size": 14})
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC We see that there's a strong correlation between the estimated 2016 population and the number of robberies. However, this is not telling us the whole story. Let's try a different type of chart.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grouped chart with custom visualizations
# MAGIC 
# MAGIC Let's compare the three cities' number of robberies by grouping them together within a bar chart visualization. Matplotlib gives you many options for customizing the color and even texture of chart elements. In this exercise, we will use the `cycler` library to define and cycle through different colors and hatches (crosshatch, vertical hatch, stars, etc.) as styles that can be applied to the chart.
# MAGIC 
# MAGIC Run the cell below to define these styles.

# COMMAND ----------

# set up style cycles
color_cycle = cycler(facecolor=plt.rcParams['axes.prop_cycle'][:4])
hatch_cycle = cycler(hatch=['/', '*', '+', '|'])

# COMMAND ----------

# MAGIC %md
# MAGIC Next, define an ordered dictionary collection (`OrderedDict`) that has the `city` column as the key and `robberies` as the value.

# COMMAND ----------

pdf = robberyAll.select('city', 'robberyRate', 'month', 'robberies').toPandas()

from collections import defaultdict
mydict = defaultdict(list)
for k, v in zip(pdf.city.values,pdf.robberies.values):
    mydict[k].append(v)
mydict = OrderedDict(mydict)

print(mydict)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, build and display a new bar chart that uses the dictionary to construct the bar chart into groups based on the index (city), cycling through the color and hatch styles you defined earlier.
# MAGIC 
# MAGIC The x-axis should show months 1 - 12, and the y axis should display the number of robberies. Also, be sure to display the legend as well.
# MAGIC 
# MAGIC Your bar chart should look similar to the following:
# MAGIC 
# MAGIC ![Boxplot chart](https://databricksdemostore.blob.core.windows.net/images/07-MDW/bar-chart.png)

# COMMAND ----------

# define the iterator for the custom dictionary we created
loop_iter = enumerate((mydict[lab], lab, s)
                              for lab, s in zip(mydict.keys(), color_cycle + hatch_cycle))

fig, ax = plt.subplots()
N = 12
ind = np.arange(N)    # the x locations for the groups
width = 0.28         # the width of the bars

arts = {}
for j, (data, label, sty) in loop_iter:
    arts[label] = ax.bar(ind + (j*width), data, width, bottom=0, label=label, **sty)

# TODO: SET THE TITLE ON THE ax OBJECT
ax.# TODO: COMPLETE THIS LINE

# set the ticks along the x axis
ax.set_xticks(ind + width / 2)

# TODO: SET THE xticklabes ON THE ax OBJECT TO 1 - 12
ax.# TODO: COMPLETE THIS LINE

ax.grid('on')

# position the legend to the right
ax.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

# adjust the plot size to fit legend
ax.autoscale_view()
fig.subplots_adjust(right=0.8)
fig.set_size_inches(12, 7)

display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Improved heatmap using grouping and per-capita data
# MAGIC 
# MAGIC The heatmap we created earlier was not particularly helpful when it came to comparing robberies across the cities. It was more focused on correlation.
# MAGIC 
# MAGIC Heatmaps can also be used to quickly discern how data is distributed across features and categories. This time, we will group all robbery data by month and city and use the `robberyRate` values to compare the number of robberies per capita by month per city.

# COMMAND ----------

gb = pdf.groupby(["month", "city"])

# sort the categories
categories = sorted(list(set(pdf.city.tolist())))
# get the number of categories
n_categories = len(categories)
# matrix where the x-axis is the month, and y-axis is the category
img_src = np.zeros((n_categories, 12))

# add the frequencies of robberies per month on the matrix
for group, values in gb:
    month = group[0] - 1 # subtract one since months are one-based
    category = group[1]
    value = values.robberyRate.values[0]
    img_src[categories.index(category)][month] = value

# COMMAND ----------

def preparePlot(xticks, yticks, figsize=(10.5, 6), hideLabels=False, gridColor='#999999',
                gridWidth=1.0):
    plt.close()
    fig, ax = plt.subplots(figsize=figsize, facecolor='white', edgecolor='white')
    ax.axes.tick_params(labelcolor='#999999', labelsize='10')
    for axis, ticks in [(ax.get_xaxis(), xticks), (ax.get_yaxis(), yticks)]:
        axis.set_ticks_position('none')
        axis.set_ticks(ticks)
        axis.label.set_color('#999999')
        if hideLabels: axis.set_ticklabels([])
    plt.grid(color=gridColor, linewidth=gridWidth, linestyle='-')
    map(lambda position: ax.spines[position].set_visible(False), ['bottom', 'top', 'left', 'right'])
    return fig, ax

# COMMAND ----------

# draw grid on axes
fig, ax = preparePlot(np.arange(.5, 12, 1), np.arange(.5, n_categories-1, 1), figsize=(16,6), hideLabels=True,
                      gridColor='#eeeeee', gridWidth=1.1)

# interpolate robberies per month on the grid
image = plt.imshow(img_src, interpolation='nearest', aspect='auto', cmap=cm.Greys)

# x-axis labels
for x, y, s in zip(np.arange(-.125, 12, 1), np.repeat(-.75, 12), [str(x+1) for x in range(12)]):
    plt.text(x, y, s, color='#999999', size='10')
# y-axis labels
for x, y, s in zip(np.repeat(-.75, n_categories), np.arange(.125, n_categories, 1), categories):
    plt.text(x, y, s, color='#999999', size='10', horizontalalignment="right")
plt.title("Distribution of robberies per capita by city and month", size=20, y=1.00)
plt.xlabel("Month", color='#999999', size="20")
plt.ylabel("Robberies per City", color='#999999', size="20")
ax.yaxis.set_label_position("right")

# plot the colobar to show scale
cbar = fig.colorbar(image, ticks=[0, 0.5, 1], shrink=0.25, orientation='vertical')
cbar.ax.set_yticklabels(['Low', 'Medium', 'High'])

display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC You should notice right away that, although Los Angeles had the highest overall robberies per month, as seen in the bar chart above, it actually has generally the lowest robberies per capita when compared to Philadelphia and Dallas. The darker the block in the heatmap, the higher the robberies per capita.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: 3d charts
# MAGIC 
# MAGIC You can use the `mpl_toolkits.mplot3d` library to create 3d charts.
# MAGIC 
# MAGIC Run the cell below to create a chart that displays data in the x, y, and z dimensions.

# COMMAND ----------

from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
import numpy as np

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
for c, z in zip(['r', 'g', 'b', 'y'], [30, 20, 10, 0]):
    xs = np.arange(20)
    ys = np.random.rand(20)

    # You can provide either a single color or an array. To demonstrate this,
    # the first bar of each set will be colored cyan.
    cs = [c] * len(xs)
    cs[0] = 'c'
    ax.bar(xs, ys, zs=z, zdir='y', color=cs, alpha=0.8)

ax.set_xlabel('X')
ax.set_ylabel('Y')
ax.set_zlabel('Z')

display(fig)