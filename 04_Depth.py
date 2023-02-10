# Databricks notebook source
# import mplfinance as mpf
import pandas as pd 
from datetime import datetime
import datetime as dt 
from functools import reduce
# Use environment variables 
from os import environ 
# plotting packages 
import plotly.graph_objects as go
import plotly.express as px
import matplotlib.pyplot as plt
import matplotlib.dates as dates
import seaborn as sns

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
df = spark.sql("select exchange_timestamp, exchange, pair, bid_volume, bid, ask_volume, ask, date from hive_metastore.default.ndlda_bbo_dev where exchange = 'binanceus' and pair = 'btc_usd' and date ='2022-10-09'")
df = df.select("exchange_timestamp", "pair","exchange", "pair", "bid_volume", "bid", "ask_volume", "ask", "date", from_unixtime(df.exchange_timestamp.cast('bigint')/1000).cast('timestamp').alias('datetime'))
df = df.select("exchange_timestamp", "pair","exchange", "bid_volume", "bid", "ask_volume", "ask", "date","datetime").withColumn("unix_timestamp", unix_timestamp("datetime", "yyyy-MM-dd HH:mm:ss")).withColumn("new_exchange_timestamp", from_unixtime("unix_timestamp"))

df = df.withColumn("date",to_date("new_exchange_timestamp"))
df = df.withColumn("hour", hour(col("new_exchange_timestamp")))\
    .withColumn("minute", minute(col("new_exchange_timestamp")))\
    .withColumn("second", second(col("new_exchange_timestamp")))
display(df)

# COMMAND ----------

df = df.filter((df.date == "2022-10-09") & (df.hour == "0") & (df.minute == "0") & (df.second == "27") )
display(df)

# COMMAND ----------

pdf = df.toPandas()

fig, ax = plt.subplots(figsize=(30,20))

sns.ecdfplot(x="bid", weights="bid_volume", stat="count", complementary=True, data=pdf, ax=ax)
sns.ecdfplot(x="ask", weights="ask_volume", stat="count", data=pdf, ax=ax)

ax.set_xlabel("Price")
ax.set_ylabel("Quantity")

plt.show()

# COMMAND ----------

frames = {side: pd.DataFrame(data=results[side], columns=["price", "quantity"],
                             dtype=float)
          for side in ["bids", "asks"]}
