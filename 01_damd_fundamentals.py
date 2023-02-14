# Databricks notebook source
# MAGIC %md
# MAGIC # Exploratory data analysis
# MAGIC We're analyzing two of the available tables in Nasdaq’s Digital Assets Market Data suite: Top of book (BBO) & Depth of Book (OB updates), which are 3 terabytes and 30+ terabytes respectively. Before diving into this much data, it's important to have a basic understanding of its structure and schema. This notebook will cover basic exploratory data analysis and visualization techniques (using [mplfinance](https://pypi.org/project/mplfinance/) package) that can be efficiently applied at that scale.

# COMMAND ----------

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

# MAGIC %md 
# MAGIC ##Basic data cleansing to format date
# MAGIC Since our delta tables were mapped to a given catalog (here `hive_metastore`) using delta sharing, we can simply retrieve informations about different crypto pairs found across different exchanges using simple SQL. 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from hive_metastore.default.ndlda_trades_prod

# COMMAND ----------

# MAGIC %md
# MAGIC The above dataset contains tick-by-tick transaction information across all exchanges and across various cryptocurrency pairs. The dataset can be valuable in understanding price movements within a specific timeframe using OHLC charts across various exchanges. The dataset is stored in Delta Lake file format partition by exchange and crypto pair which will significanlty increase read performance due efficient data skipping at the file level.

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions hive_metastore.default.ndlda_trades_prod

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Basic data cleansing to format date
# MAGIC Since our delta tables were mapped to a given catalog (here `hive_metastore`) using delta sharing, we can simply retrieve informations about different crypto pairs found across different exchanges using simple SQL. 

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pyspark.sql.types as T

df = spark.sql("""
select 
  exchange_timestamp, 
  exchange, 
  price, 
  pair, 
  volume
from hive_metastore.default.ndlda_trades_prod 
where pair = 'btc_usd'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Within the two datasets, the most important columns for segmenting and grouping data are the "pair," "exchange," and "exchange_timestamp" columns. These columns allow for examination of the cryptocurrency pair, the exchange on which the quote or trade is listed, and the timestamp of the exchange the TAQ data was marked. It is important to note that the timestamp column should be transformed to be human-readable, as it is a long value in UNIX time. Using Spark, it is easy to convert the column and add new columns to make the data more readable, as well as break out different columns by date, hours, minutes, and seconds

# COMMAND ----------

def enrich_with_time(df):
  return df \
  .withColumn("datetime", from_unixtime(df.exchange_timestamp.cast("bigint") / 1000).cast("timestamp")) \
  .withColumn("unix_timestamp", unix_timestamp("datetime", "yyyy-MM-dd HH:mm:ss")) \
  .withColumn("exchange_timestamp_parsed", from_unixtime("unix_timestamp")) \
  .withColumn("date", to_date("exchange_timestamp_parsed")) \
  .withColumn("hour", hour(col("exchange_timestamp_parsed"))) \
  .withColumn("minute", minute(col("exchange_timestamp_parsed"))) \
  .withColumn("second", second(col("exchange_timestamp_parsed")))

# COMMAND ----------

df = df.transform(enrich_with_time)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC We can compare average price of `btc_usd` across various exchanges

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   distinct(`exchange`), 
# MAGIC   avg(`price`) as AveragePrice 
# MAGIC from hive_metastore.default.ndlda_trades_prod
# MAGIC where `date` = '2022-06-08' 
# MAGIC and `pair` = 'btc_usd'
# MAGIC group by `exchange`

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Plot Daily Open Data
# MAGIC With our data reduced to a manageable size, this information can be quickly and easily visualized by converting spark to pandas dataframes. 
# MAGIC To further analyze intraday price changes, let's build OHLC dataset from our tick data, sampling for only 100,000 rows and applying a 1mn window aggregation.

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

# MAGIC %md
# MAGIC Let's look at a simple candlestick pattern as well as an overlay with moving averages which tell us price movements during the day. We can use additional packages such as mplfinance for more complex visualization such as candlestick (with slider window) directly on a same notebook.

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
# MAGIC Now let's take a different angle and explore other materic of an asset such as market depth and bid/ask spreads. For this analysis, we will use the orderbook data, applying same date / time transformations as introduced earlier

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
df = spark.sql("""
select 
  exchange_timestamp, 
  exchange, 
  pair, 
  bid_volume, 
  bid, 
  ask_volume, 
  ask, 
  date 
from hive_metastore.default.bbo_prod 
and pair = 'btc_usd'
""")

# COMMAND ----------

df = df.transform(enrich_with_time)
df = df.withColumn('spread', round(((df.ask - df.bid)/df.ask)*100,4))
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Understanding market depth at a given time snapshot
# MAGIC To truly understand how a cryptocurrency instrument trades, it is important to consider not just the trades themselves, but also the liquidity and timing of those trades. This notebook provides a way to visualize the timings of when the most quotes are made, giving insight into the peak times of day when there is the most potential for trades. By drilling down into specific dates or times, it is possible to see specific information about price and quantity, and how the instrument is trading overall. Using tools like Pandas, this information can be quickly and easily visualized.

# COMMAND ----------

sdf = df.filter((df.exchange == "binanceus") & (df.date == "2022-10-09") & (df.hour == "0") & (df.minute == "0") & (df.second == "27") )
pdf = sdf.toPandas()

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

df = spark.sql("""
select 
  exchange_timestamp, 
  exchange, 
  pair, 
  bid_volume, 
  bid, 
  ask_volume, 
  ask, 
  date 
from hive_metastore.default.bbo_prod""")

df = df.transform(enrich_with_time)
df = df.withColumn("spread", round(((df.ask - df.bid) / df.ask) * 100, 4))
display(df)
