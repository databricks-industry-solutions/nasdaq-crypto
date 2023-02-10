# Databricks notebook source
# MAGIC %sql
# MAGIC select * from hive_metastore.default.amberdata_sample

# COMMAND ----------

import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

data_month = data[(data["year"] == 2020) & (data["month"] == 7)]

# COMMAND ----------

data_month.describe()

# COMMAND ----------

display(data_month)

# COMMAND ----------

fig, ax = plt.subplots()

sns.scatterplot(x="price", y="size", hue="isBuySide", data=data_month, ax=ax)

ax.set_xlabel("Price")
ax.set_ylabel("Quantity")

plt.show()

# COMMAND ----------

data.dtypes

# COMMAND ----------

data_bid = data.loc[data["isBuySide"]==True]
display(data_bid)

# COMMAND ----------

data_ask = data.loc[data["isBuySide"]==False]
display(data_ask)

# COMMAND ----------

data = pd.concat([data_bid,data_ask],axis="index", 
                 ignore_index=True, sort=True).drop_duplicates().reset_index(drop=True)

# COMMAND ----------

display(data)

# COMMAND ----------

frames = {side: pd.DataFrame(data=data[side], columns=["price", "quantity"],
                             dtype=float)
          for side in ["bid", "ask"]}
