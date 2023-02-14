# Databricks notebook source
# MAGIC %md
# MAGIC <img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fsi/fs-lakehouse-logo-transparent.png width="600px">
# MAGIC 
# MAGIC [![DBR](https://img.shields.io/badge/DBR-11.3ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/11.3.html)
# MAGIC [![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://databricks.com/try-databricks)
# MAGIC [![POC](https://img.shields.io/badge/POC-3 days-green?style=for-the-badge)](https://databricks.com/try-databricks)
# MAGIC 
# MAGIC **Data exploration** *is the process of investigating a new data set by asking basic questions in order to gain insights for further analysis. Delivering seamless access to vast digital asset data–blockchain networks, crypto markets, and decentralized finance–is essential to support trading and risk strategies. [Nasdaq Data Link Digital Assets](https://data.nasdaq.com/publishers/NDLDA) is a part of Nasdaq's Investment Intelligence suite of products, designed to provide significant value to customers in making informed decisions. As the creator of the world's first electronic stock market, Nasdaq technology powers more than 70 marketplaces in 50 countries, and one in ten of the world's securities transactions. Built in partnership with [Amberdata](https://www.amberdata.io/), Digital Assets Market Data covers trades, top-of-book and depth-of-book data for 2,000+ cryptocurrency pairs across 15 exchanges. This is a petabyte-scale suite of products that can be overwhelming for many to navigate*
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC Mitch Tessier, Josh Seidel, Sri Ghattamaneni, Vionna Lo, Joe Widen

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://raw.githubusercontent.com/databricks-industry-solutions/nasdaq-crypto/main/images/high_level_workflow.png' width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | mplfinance                             | Visualization library   | BSD        | https://pypi.org/project/mplfinance/                |
