# Databricks notebook source
# MAGIC %pip install yfinance
# MAGIC %pip install mplfinance

# COMMAND ----------

# processing the results 

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

# MAGIC %md ##Basic data cleansing to format date

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct(exchange) from hive_metastore.default.ndlda_trades_dev

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace table hive_metastore.default.trades_btc_usd as 
# MAGIC select *
# MAGIC from hive_metastore.default.ndlda_trades_prod
# MAGIC where pair = 'btc_usd'

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
df = spark.sql("select exchange_timestamp, exchange, price, pair, is_buy_side, volume  from hive_metastore.default.trades_btc_usd  where exchange = 'binanceus' ")
df = df.select("exchange_timestamp", "price", "pair", "is_buy_side","volume", "exchange", from_unixtime(df.exchange_timestamp.cast('bigint')/1000).cast('timestamp').alias('datetime'))
df = df.select("exchange_timestamp", "price", "pair", "is_buy_side","exchange","volume","datetime").withColumn("unix_timestamp", unix_timestamp("datetime", "yyyy-MM-dd HH:mm:ss")).withColumn("new_exchange_timestamp", from_unixtime("unix_timestamp"))
display(df)


# COMMAND ----------

from pyspark.sql import functions as F

df = df.withColumn("date",to_date("new_exchange_timestamp"))
df = df.withColumn("hour", hour(col("new_exchange_timestamp")))\
    .withColumn("minute", minute(col("new_exchange_timestamp")))\
    .withColumn("second", second(col("new_exchange_timestamp")))
display(df)

# COMMAND ----------

df = df.filter((df.exchange == "binanceus") &  (df.date == "2022-08-08") & (df.hour == "0") & (df.minute == "0") )

display(df)

# COMMAND ----------

pdf = df.toPandas()

# COMMAND ----------

pdf.groupby("is_buy_side").price.describe()

# COMMAND ----------

fig, ax = plt.subplots(figsize=(20,10))
sns.histplot(x="price", weights="volume", hue="is_buy_side", binwidth=50, data=pdf, ax=ax)
sns.scatterplot(x="price", y="volume", hue="is_buy_side", data=pdf, ax=ax)

ax.set_xlabel("Price")
ax.set_ylabel("Quantity")

plt.show()

# COMMAND ----------

# MAGIC %md ##Bids and Asks Scatter plot

# COMMAND ----------

fig, ax = plt.subplots(figsize=(20,10))
ax.set_title(f"BTCUSDT Order Book - Scatterplot")
sns.scatterplot(x="price", y="volume", hue="is_buy_side", 
                data=pdf, ax=ax, palette=["green", "red"])
ax.set_xlabel("Price")
ax.set_ylabel("Quantity")

# COMMAND ----------

# MAGIC %md ##Visualize Orderbook For ETH/USD Pair

# COMMAND ----------

fig, ax = plt.subplots()
ax.set_title(f"ETHUSDT Order Book - Weighted Histogram")
sns.histplot(x="price", hue="is_buy_side", weights="size", 
             binwidth=1, data=df, palette=["green", "red"], 
             ax=ax)
ax.set_xlabel("Price")
ax.set_ylabel("Quantity")

# COMMAND ----------

# MAGIC %md ##Plot Daily Open Data

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

df = spark.sql("select exchange_timestamp, exchange, price, pair, volume  from hive_metastore.default.ndlda_trades")
df = df.select("exchange_timestamp", "price", "pair", "volume", from_unixtime(df.exchange_timestamp.cast('bigint')/1000).cast('timestamp').alias('datetime'))
df = df.select("exchange_timestamp", "price", "pair", "volume","datetime").withColumn("unix_timestamp", unix_timestamp("datetime", "yyyy-MM-dd HH:mm:ss")).withColumn("new_exchange_timestamp", from_unixtime("unix_timestamp"))
display(df)

tick_data = df.toPandas()

# COMMAND ----------

timeframe = '1min'
tick_data['datetime'] = pd.to_datetime(tick_data['new_exchange_timestamp'])
tick_data.set_index('datetime', inplace=True)

# COMMAND ----------

timeframe = '1min'

ohlcv_data = pd.DataFrame(columns=[
    'pair',
    'open',
    'high',
    'low',
    'close'])

for pair in tick_data['pair'].unique():
    ohlcv_symbol =  tick_data.loc[tick_data['pair'] == pair, 'price'].resample(timeframe).ohlc()
    ohlcv_symbol['pair'] = pair
    ohlcv_data = ohlcv_data.append(ohlcv_symbol, sort=False)

print(ohlcv_data)

# COMMAND ----------

# MAGIC %md ##OHLC Plots With Slider

# COMMAND ----------

import plotly.graph_objects as go
import pandas as pd

# use go.OHLC function and pass the date, open,
# high, low and close price of the function
fig = go.Figure(data=go.Ohlc(x=ohlcv_data.index,
                             open=ohlcv_data['open'],
                             high=ohlcv_data['high'],
                             low=ohlcv_data['low'],
                             close=ohlcv_data['close']))
  
# show the figure
fig.show()

# COMMAND ----------

import mplfinance as mpf
mpf.plot(ohlcv_data, # the dataframe containing the OHLC (Open, High, Low and Close) data
         type='candle', # use candlesticks 
         volume=False, # also show the volume
         mav=(3,6,9), # use three different moving averages
         figratio=(3,1), # set the ratio of the figure
         style='yahoo',  # choose the yahoo style
         title='Bitcoin-ETH Candle NASDAQ Data Link');

# COMMAND ----------

# MAGIC %md ## Bitcoin ticker in USD

# COMMAND ----------

mpf.plot(ohlcv_data,type='candle',volume=True,figratio=(3,1),style='yahoo', title='BTC_USD Latest');
