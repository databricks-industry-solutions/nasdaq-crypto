# Databricks notebook source
test = spark.read.table("hive_metastore.default.ndlda_trades_dev")
test.write.format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .partitionBy("exchange", "pair")\
  .saveAsTable("hive_metastore.default.ndlda_trades_prod")

# COMMAND ----------

display(test)

# COMMAND ----------

df = spark.read.table("hive_metastore.default.ndlda_bbo_dev")

df.write.format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", "true")\
  .partitionBy("exchange", "pair")\
  .saveAsTable("hive_metastore.default.bbo_prod")
