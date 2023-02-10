# Databricks notebook source
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

# COMMAND ----------

df = spark.sql("select * from default.ndlda_trades_dev limit 10000").toPandas()
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
df = spark.sql("select exchange_timestamp, exchange, bid, ask, mid, pair, last, bid_volume, date from default.ndlda_bbo_revisions_dev limit 10000")
df = df.select("exchange_timestamp", "bid","ask","mid", "pair", "exchange","bid_volume", "last", "date", from_unixtime(df.exchange_timestamp.cast('bigint')/1000).cast('timestamp').alias('datetime'))
df = df.select("exchange_timestamp", "bid","ask","mid", "pair", "last","exchange","bid_volume", "datetime", "date").withColumn("unix_timestamp", unix_timestamp("datetime", "yyyy-MM-dd HH:mm:ss")).withColumn("new_exchange_timestamp", from_unixtime("unix_timestamp"))

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumn('Spread', round(((df.ask - df.bid)/df.ask)*100,4))
display(df)

# COMMAND ----------

pdf = df.toPandas()

# COMMAND ----------

# MAGIC %md ## Change this to overlay across exchanges

# COMMAND ----------

pdf['Spread'].plot(figsize=(20,10), title='Bid-Ask Spread across exchanges')

# COMMAND ----------

from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
ob = spark.sql("select * from hive_metastore.default.ndlda_bbo_dev_v2 limit 1000").toPandas()
display(ob)

# COMMAND ----------

ob['date'] = pd.to_datetime(ob['date']) 
ob.dtypes

# COMMAND ----------

ob2 = ob[(ob['date'] > "2022-09-01") & (ob['date'] < "2021-09-01")]

# COMMAND ----------

import matplotlib.pyplot as plt
plt.figure()
ob['mid'].plot()
plt.legend(['mid'])
plt.show()
