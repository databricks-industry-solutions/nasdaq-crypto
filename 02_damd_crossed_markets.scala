// Databricks notebook source
var bboDf = spark.table("main.ndlda.ndlda_bbo")
var tradesDf = spark.table("main.ndlda.ndlda_trades")

// COMMAND ----------

bboDf.printSchema

// COMMAND ----------

tradesDf.printSchema

// COMMAND ----------

display(bboDf.limit(1000))

// COMMAND ----------

display(tradesDf.limit(1000))

// COMMAND ----------

val distinctExchangesDf = bboDf.select($"exchange").distinct.sort($"exchange")
display(distinctExchangesDf)

// COMMAND ----------

val distinctQuotePairsDf = bboDf.select($"pair").distinct
display(distinctQuotePairsDf.sort($"pair"))

// COMMAND ----------

print(distinctQuotePairsDf.count)

// COMMAND ----------

val quotesCrossedDf = bboDf.filter($"bid" >= $"ask")

/* Display the exchanges with crossed quotes in their data */
display(quotesCrossedDf.select($"exchange").distinct.sort($"exchange"))

// COMMAND ----------

val quotesCrossedCountedDf = quotesCrossedDf.groupBy($"exchange", $"pair").count.sort($"exchange", $"pair")
display(quotesCrossedCountedDf)

// COMMAND ----------

import org.apache.spark.sql.functions.{ sum }

display(quotesCrossedCountedDf.agg(sum($"count")))

// COMMAND ----------

/* Please see, and search, LAST_WIN: https://spark.apache.org/docs/latest/sql-migration-guide.html */
/* Code: https://github.com/apache/spark/blob/0fa9c554fc0b3940a47c3d1c6a5a17ca9a8cee8e/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala, Lines: 3505 - 3518 */
spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.expressions.{ Window }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{ abs, col, greatest, last, least, lit, regexp_replace, struct, when }
import scala.collection.mutable

var bboDf = spark.table("main.ndlda.ndlda_bbo")

val distinctExchanges = bboDf.select($"exchange").distinct.sort($"exchange").as[String].collect.toList
val pairTimeOrderedWindow = Window.partitionBy($"pair").orderBy($"exchange_timestamp".asc, $"exchange_timestamp_nanoseconds".asc)

val mapExchangeBidColumns = distinctExchanges.map(exchange => col(s"${exchange}_bid")).toSeq
val mapExchangeBidStrings = distinctExchanges.map(exchange => s"${exchange}_bid").toSeq
val mapExchangeAskColumns = distinctExchanges.map(exchange => col(s"${exchange}_ask")).toSeq
val mapExchangeAskStrings = distinctExchanges.map(exchange => s"${exchange}_ask").toSeq

// Explode out columns based on the exchange and populate the column
val explodedExchangesBBODf = distinctExchanges.foldLeft(bboDf)(
  (df, c) => df
    .withColumn(s"${c}_bid", when($"exchange" === c, $"bid").otherwise(null))
    .withColumn(s"${c}_ask", when($"exchange" === c, $"ask").otherwise(null))
)

// Last over the window for all the exchange specific columns getting the previous value for the column if the value is null.
val lastBBODf = distinctExchanges.foldLeft(explodedExchangesBBODf)(
  (df, c) => df
    .withColumn(s"${c}_bid", last(s"${c}_bid", true).over(pairTimeOrderedWindow))
    .withColumn(s"${c}_ask", last(s"${c}_ask", true).over(pairTimeOrderedWindow))
)

// Create a map from the non-null values/prices with the key being the exchange
def mapExchangesQuotesSide(columns: Seq[String]): Column = {
  def getPairs(columnName: String): Seq[Column] = Seq(lit(columnName), when(col(columnName).isNotNull, col(columnName)).otherwise(lit(null)))
  
  map_filter(map(columns.flatMap(getPairs): _*), (k, v) => !(v.isNull || v < 0))
}

// Return a collection of the high and low map entries
val bestWorstPrice = udf[(String, Double, String, Double), Map[String, Double]]((m: Map[String, Double]) => {
  var high_name, low_name: String = null;
  var high_val, low_val: Double = 0d;
  
  m.foreach { case (k,v) => {
    if (v > high_val || high_name == null || high_name.isEmpty) {
      high_val = v
      high_name = k
    }
    if (v < low_val || low_name == null || low_name.isEmpty) {
      low_val = v
      low_name = k
    }
  }}
  
  (high_name, high_val, low_name, low_val)
})

val marketSystemQuoteValuesDf = lastBBODf
  .withColumn("bid_exchanges", mapExchangesQuotesSide(mapExchangeBidStrings))
  .withColumn("high_low_bids", bestWorstPrice($"bid_exchanges"))
  .withColumn("best_bid_exchange", regexp_replace($"high_low_bids._1", "_bid", ""))
  .withColumn("best_bid_price", $"high_low_bids._2")
  .withColumn("worst_bid_exchange", regexp_replace($"high_low_bids._3", "_bid", ""))
  .withColumn("worst_bid_price", $"high_low_bids._4")
  .withColumn("ask_exchanges", mapExchangesQuotesSide(mapExchangeAskStrings))
  .withColumn("high_low_asks", bestWorstPrice($"ask_exchanges"))
  .withColumn("best_ask_exchange", regexp_replace($"high_low_asks._3", "_ask", ""))
  .withColumn("best_ask_price", $"high_low_asks._4")
  .withColumn("worst_ask_exchange", regexp_replace($"high_low_asks._1", "_ask", ""))
  .withColumn("worst_ask_price", $"high_low_asks._2")
  .withColumn("difference_bid", $"best_bid_price" - $"worst_bid_price")
  .withColumn("abs_difference_bid", abs($"difference_bid"))
  .withColumn("difference_ask", $"best_ask_price" - $"worst_ask_price")
  .withColumn("abs_difference_ask", abs($"difference_ask"))
  .withColumn("best_quote_spread", $"best_ask_price" - $"best_bid_price")
  .withColumn("abs_best_quote_spread", abs($"best_quote_spread"))
  .withColumn("worst_quote_spread", $"worst_ask_price" - $"worst_bid_price")
  .withColumn("abs_worst_quote_spread", abs($"worst_quote_spread"))
  .drop(mapExchangeBidStrings: _*)
  .drop($"bid_exchanges")
  .drop($"high_low_bids")
  .drop(mapExchangeAskStrings: _*)
  .drop($"ask_exchanges")
  .drop($"high_low_asks")

// COMMAND ----------

display(marketSystemQuoteValuesDf)

// COMMAND ----------

val bboMarketsCrossedDf = marketSystemQuoteValuesDf.filter($"best_bid_price" >= $"best_ask_price")

// COMMAND ----------

display(bboMarketsCrossedDf)

// COMMAND ----------

val bboMarketsBidsCrossedCountedDf = bboMarketsCrossedDf.groupBy($"best_bid_exchange", $"pair").count.sort($"best_bid_exchange", $"pair")

display(bboMarketsBidsCrossedCountedDf)

// COMMAND ----------

val bboMarketsAsksCrossedCountedDf = bboMarketsCrossedDf.groupBy($"best_ask_exchange", $"pair").count.sort($"best_ask_exchange", $"pair")

display(bboMarketsAsksCrossedCountedDf)

// COMMAND ----------

val exchangesBboMarketsQuotesCrossedCountedDf = bboMarketsCrossedDf.groupBy($"exchange").count.sort($"exchange")

display(exchangesBboMarketsQuotesCrossedCountedDf)

// COMMAND ----------

val bboMarketsQuotesCrossedCountedDf = bboMarketsCrossedDf.groupBy($"best_bid_exchange", $"pair").count.sort($"exchange", $"pair")

display(bboMarketsQuotesCrossedCountedDf)

// COMMAND ----------

import org.apache.spark.sql.functions.mean

val bboMarketsQuotesCrossedMeanDf = bboMarketsCrossedDf.groupBy($"best_bid_exchange", $"pair").agg(mean("abs_best_quote_spread")).sort($"exchange", $"pair")

display(bboMarketsQuotesCrossedMeanDf)

// COMMAND ----------

import org.apache.spark.sql.functions.stddev

val bboMarketsQuotesCrossedStdDevDf = bboMarketsCrossedDf.groupBy($"best_bid_exchange", $"pair").agg(stddev("abs_best_quote_spread")).sort($"exchange", $"pair")

display(bboMarketsQuotesCrossedStdDevDf)
