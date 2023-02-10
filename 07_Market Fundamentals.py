# Databricks notebook source
# MAGIC %pip install yfinance
# MAGIC %pip install mplfinance

# COMMAND ----------

import mplfinance as mpf
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

# MAGIC %md ##Let's look at available datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   hive_metastore.default.ndlda_trades_prod

# COMMAND ----------

# MAGIC %md ##### The above dataset contains tick-by-tick transaction information across all exchanges and across various cryptocurrency pairs. The dataset can be valuable in understanding price movements within a specific timeframe using OHLC charts across various exchanges. The dataset is stored in Delta Lake file format partition by exchange and crypto pair which will significanlty increase read performance due efficient data skipping at the file level.

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions hive_metastore.default.ndlda_trades_prod

# COMMAND ----------

# MAGIC %md ##Basic data cleansing to format date

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pyspark.sql.types as T

df = spark.sql(
    "select exchange_timestamp, exchange, price, pair, volume from hive_metastore.default.ndlda_trades_prod where pair = 'btc_usd'"
)
df = df.select(
    "exchange_timestamp",
    "exchange",
    "price",
    "pair",
    "volume",
    from_unixtime(df.exchange_timestamp.cast("bigint") / 1000)
    .cast("timestamp")
    .alias("datetime"),
)
df = (
    df.select("exchange_timestamp", "exchange", "price", "pair", "volume", "datetime")
    .withColumn("unix_timestamp", unix_timestamp("datetime", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("exchange_timestamp_parsed", from_unixtime("unix_timestamp"))
)
df = df.withColumn("date", to_date("exchange_timestamp_parsed"))
df = (
    df.withColumn("hour", hour(col("exchange_timestamp_parsed")))
    .withColumn("minute", minute(col("exchange_timestamp_parsed")))
    .withColumn("second", second(col("exchange_timestamp_parsed")))
)

display(df)

# COMMAND ----------

# MAGIC %md #####Comparing average price of btc_usd across various exchanges

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct(exchange), avg(price) as AveragePrice from hive_metastore.default.ndlda_trades_prod
# MAGIC where date = '2022-06-08' and pair = 'btc_usd'
# MAGIC group by exchange

# COMMAND ----------

# MAGIC %md ##### To further analyze intraday price changes, let's build OHLC daataset from our tick data 

# COMMAND ----------

tick_data = df.limit(100000).toPandas()

timeframe = "1min"
tick_data["datetime"] = pd.to_datetime(tick_data["exchange_timestamp_parsed"])
tick_data.set_index("datetime", inplace=True)

# COMMAND ----------

# Setting a interval time of 5 minutes
timeframe = "5min"

ohlcv_data = pd.DataFrame(columns=["pair", "open", "high", "low", "close"])

for pair in tick_data["pair"].unique():
    ohlcv_symbol = (
        tick_data.loc[tick_data["pair"] == pair, "price"].resample(timeframe).ohlc()
    )
    ohlcv_symbol["pair"] = pair
    ohlcv_data = ohlcv_data.append(ohlcv_symbol, sort=False)

# COMMAND ----------

# MAGIC %md ####Let's look at a simple candlestick pattern as well as an overlay with moving averages which tell us price movements during the day 

# COMMAND ----------

import plotly.graph_objects as go
import pandas as pd

fig = go.Figure(
    data=go.Ohlc(
        x=ohlcv_data.index,
        open=ohlcv_data["open"],
        high=ohlcv_data["high"],
        low=ohlcv_data["low"],
        close=ohlcv_data["close"],
    )
)
fig.show()

# COMMAND ----------

import mplfinance as mpf

mpf.plot(
    ohlcv_data,
    type="candle",
    volume=False,
    mav=(3, 6, 9),  
    figratio=(3, 1),
    style="yahoo",
    title="Bitcoin-USD Candle on June 7th, 2022",
);

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's take a different angle and explore other materic of an asset such as market depth and bid/ask spreads etc. For this analysis, we will use the orderbook data.

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
df = spark.sql("select exchange_timestamp, exchange, pair, bid_volume, bid, ask_volume, ask, date from hive_metastore.default.bbo_prod where exchange = 'binanceus' and pair = 'btc_usd' ")
df = df.select("exchange_timestamp", "pair","exchange", "pair", "bid_volume", "bid", "ask_volume", "ask", "date", from_unixtime(df.exchange_timestamp.cast('bigint')/1000).cast('timestamp').alias('datetime'))
df = df.select("exchange_timestamp", "pair","exchange", "bid_volume", "bid", "ask_volume", "ask", "date","datetime").withColumn("unix_timestamp", unix_timestamp("datetime", "yyyy-MM-dd HH:mm:ss")).withColumn("new_exchange_timestamp", from_unixtime("unix_timestamp"))
df = df.withColumn('Spread', round(((df.ask - df.bid)/df.ask)*100,4))
df = df.withColumn("date",to_date("new_exchange_timestamp"))
df = df.withColumn("hour", hour(col("new_exchange_timestamp")))\
    .withColumn("minute", minute(col("new_exchange_timestamp")))\
    .withColumn("second", second(col("new_exchange_timestamp")))
display(df)

# COMMAND ----------

# Let's randomly pick a date and time to get a look at the market depth
sdf = df.filter((df.date == "2022-10-09") & (df.hour == "0") & (df.minute == "0") & (df.second == "27") )
pdf = sdf.toPandas()

# COMMAND ----------

# MAGIC %md ####Understanding market depth at a given time snapshot

# COMMAND ----------

fig, ax = plt.subplots(figsize=(20, 10))
sns.ecdfplot(
    x="bid", weights="bid_volume", stat="count", complementary=True, data=pdf, ax=ax
)
sns.ecdfplot(x="ask", weights="ask_volume", stat="count", data=pdf, ax=ax)
ax.set_xlabel("Price")
ax.set_ylabel("Quantity")

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC As we know bid-ask spread is the liquidity measure of a given security. Let's look at bid-ask spread percentage various exchanges.

# COMMAND ----------

df = spark.sql(
    "select exchange_timestamp, exchange, pair, bid_volume, bid, ask_volume, ask, date from hive_metastore.default.bbo_prod "
)
df = df.select(
    "exchange_timestamp",
    "pair",
    "exchange",
    "pair",
    "bid_volume",
    "bid",
    "ask_volume",
    "ask",
    "date",
    from_unixtime(df.exchange_timestamp.cast("bigint") / 1000)
    .cast("timestamp")
    .alias("datetime"),
)
df = (
    df.select(
        "exchange_timestamp",
        "pair",
        "exchange",
        "bid_volume",
        "bid",
        "ask_volume",
        "ask",
        "date",
        "datetime",
    )
    .withColumn("unix_timestamp", unix_timestamp("datetime", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("new_exchange_timestamp", from_unixtime("unix_timestamp"))
)
df = df.withColumn("Spread", round(((df.ask - df.bid) / df.ask) * 100, 4))
display(df)
