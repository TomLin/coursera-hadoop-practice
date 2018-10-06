df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("/databricks-datasets/definitive-guide/data/retail-data/all/*.csv")\
        .coalesce(5) # repartition the RDD to 5 partitions

# df:pyspark.sql.dataframe.DataFrame -> schema
# InvoiceNo:string
# StockCode:string
# Description:string
# Quantity:integer
# InvoiceDate:string
# UnitPrice:double
# CustomerID:integer
# Country:string

df.cache()
df.createOrReplaceTempView("dfTable")


# COMMAND ----------

from pyspark.sql.functions import count
df.select(count("StockCode")).show() # 541909

# count(*) will count in the Null values,
# but when specifying specific columns, count('column_name') will exclude Null values


# COMMAND ----------

from pyspark.sql.functions import countDistinct
df.select(countDistinct("StockCode")).show() # 4070


# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("StockCode", 0.1)).show() # 3364

# the approximation will enhance much better performance on large dataset
# here we specify 0.1 be the maximum estimation error rate

# COMMAND ----------

from pyspark.sql.functions import first, last
df.select(first("StockCode"), last("StockCode")).show()

# Output:
# +-----------------------+----------------------+
# |first(StockCode, false)|last(StockCode, false)|
# +-----------------------+----------------------+
# |                 85123A|                 22138|
# +-----------------------+----------------------+

# COMMAND ----------

from pyspark.sql.functions import min, max
df.select(min("Quantity"), max("Quantity")).show()


# COMMAND ----------

from pyspark.sql.functions import sum
df.select(sum("Quantity")).show() # 5176450


# COMMAND ----------

from pyspark.sql.functions import sumDistinct
df.select(sumDistinct("Quantity")).show() # 29310

# summing up a distinc set of values in the column Quantity -> it won't include duplicated values

# COMMAND ----------

from pyspark.sql.functions import sum, count, avg, expr

df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))\
  .selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases").show()


# COMMAND ----------

from pyspark.sql.functions import var_pop, stddev_pop
from pyspark.sql.functions import var_samp, stddev_samp
df.select(var_pop("Quantity"), var_samp("Quantity"),
  stddev_pop("Quantity"), stddev_samp("Quantity")).show()

# by default, if we use function `variance` or `stddev`,
# Spark performs the formula for sample variance or standard deviation

# COMMAND ----------

from pyspark.sql.functions import skewness, kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()


# COMMAND ----------

from pyspark.sql.functions import corr, covar_pop, covar_samp
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
    covar_pop("InvoiceNo", "Quantity")).show()

# covariance can also be calculated either as sample or population,
# while correlation doesn't have the notion and no need to calculate on sample or population 


# COMMAND ----------

# Aggregation to Complex Types
# In Spark, you can perform aggregations not just of numerical values using formulas, 
# you can also perform them on complex types. 
# For example, we can collect a list of values present in a given column or 
# only the unique values by collecting to a set. 
# You can use this to carry out some more programmatic access 
# later on in the pipeline or pass the entire collection in a user-defined function (UDF)

from pyspark.sql.functions import collect_set, collect_list
df.agg(collect_set("Country"), collect_list("Country")).show()
# noted here, we use `agg` 

# pyspark.sql.functions.collect_list(col): returns a list of objects with duplicates.
# pyspark.sql.functions.collect_set(col): returns a set of objects with duplicate elements eliminated.




# COMMAND ----------

from pyspark.sql.functions import count

df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show()


# COMMAND ----------

df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)"))\
  .show()


# COMMAND ----------

from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")
# `withColumn` is a formal method in DataFrame to add a column 

# COMMAND ----------

# 記住，window function通常不會改變原有的dataframe的row數量，而是在原有資料表中，多增加一個欄位
# 通常它會出現在select()裡面

from pyspark.sql.window import Window
from pyspark.sql.functions import desc
windowSpec = Window\
  .partitionBy("CustomerId", "date")\
  .orderBy(desc("Quantity"))\
  .rowsBetween(Window.unboundedPreceding, Window.currentRow) # a window expression


# COMMAND ----------

from pyspark.sql.functions import max
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec) # an expression


# COMMAND ----------

from pyspark.sql.functions import dense_rank, rank
purchaseDenseRank = dense_rank().over(windowSpec) # an expression
purchaseRank = rank().over(windowSpec) # an expression


# COMMAND ----------

from pyspark.sql.functions import col

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
  .select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    purchaseRank.alias("quantityRank"),
    purchaseDenseRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()


# COMMAND ----------

dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")


# COMMAND ----------

rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))\
  .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")\
  .orderBy("Date")
rolledUpDF.show()

# 使用rollup的時候，需要先將原資料中的null值去掉
# 這時在使用rollup時，就會出現依照(Date, Country)的Quantity加總
# 還會出現依照(Date)的Quantity加總->這時Country欄位會是Null值
# 還會出現全部的加總->這時同時Date和Country欄位都會是Null值，代表全部的值都rollup起來了

# COMMAND ----------

from pyspark.sql.functions import sum

dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))\
  .select("Date", "Country", "sum(Quantity)").orderBy("Date").show()

# 和rollup不同的地方在於，使用cube的話，它就不是按照hierarchically,
# 而是會針對每一個dimension都rollup起來，
# 它會在Date上，有rollup起來的Quantity值
# 它也會在Country上，有rollup起來的Quantity值 -> 在使用rollup時，就不會有這個值


# COMMAND ----------

pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
# from long dateset to wide dataset

# pivot() will use the groupby column as rows and pivot column as columns, 
# and compute all numerical columns for the (gorupby row- pivot column) pair.

# pivot(pivot_col, values=None):
# we can also specify what values should be included in the pivot column,
# for example, if we only need the pivot for UK, USA, then we can specify in this way:
# pivot("Country", ["UK", "USA"])




# COMMAND ----------