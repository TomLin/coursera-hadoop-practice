df = spark.read.format("json").load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")

df.printSchema()

# COMMAND ----------

spark.read.format("json").load("/data/flight-data/json/2015-summary.json").schema


# COMMAND ----------

# we want to enforce a specific schema on the dataframe
from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False, metadata={"hello":"world"})
])
df = spark.read.format("json").schema(myManualSchema)\
  .load("/data/flight-data/json/2015-summary.json")


# A schema is a StructType made up of a number of fields, StructFields, that have a name, type, a Boolean flag which specifies
# whether that column can contain missing or null values and, finally, some user-defined metadata information for that column.
# Schemas can contain other StructTypes(such as Spark complex type), it is discussed in chapter 6.


# COMMAND ----------

from pyspark.sql.functions import col, column
col("someColumnName")
column("someColumnName")

df.columns # to access all columns 

df.col('column_name') # explicit reference on a column from the dataframe

# COMMAND ----------

from pyspark.sql.functions import expr
expr("(((someCol + 5) * 200) - 6) < otherCol")

# columns are just expressions.
# columns and transformations of those columns compile to the same logical plan as parsed expressions.

# COMMAND ----------

from pyspark.sql import Row
myRow = Row("Hello", None, 1, False)

# Rows do not have schema, only DataFrame has schema.

# COMMAND ----------

myRow[0]
myRow[2]


# COMMAND ----------

df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json")
df.createOrReplaceTempView("dfTable")


# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
myManualSchema = StructType([
  StructField("some", StringType(), True),
  StructField("col", StringType(), True),
  StructField("names", LongType(), False)
])
myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema) # create a DataFrame out of Rows
myDf.show()


# COMMAND ----------

df.select("DEST_COUNTRY_NAME").show(2)


# COMMAND ----------

df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)


# COMMAND ----------

from pyspark.sql.functions import expr, col, column
df.select(
    expr("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"))\
  .show(2)

# Noted:
# we cannot mix different Column object and Strings together in select.
# for example, df.select(col("DEST_COUNTRY_NAME"), "DEST_COUNTRY_NAME") will raise error.

# COMMAND ----------

df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)


# COMMAND ----------

df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))\
  .show(2)


# COMMAND ----------

df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)


# COMMAND ----------

df.selectExpr(
  "*", # all original columns
  "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
  .show(2)

# Output:
# +-----------------+-------------------+-----+-------------+
# |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|withinCountry|
# +-----------------+-------------------+-----+-------------+
# |    United States|            Romania|   15|        false|
# |    United States|            Croatia|    1|        false|
# |    United States|            Ireland|  344|        false|
# |            Egypt|      United States|   15|        false|
# |    United States|              India|   62|        false|
# +-----------------+-------------------+-----+-------------+



# COMMAND ----------

df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)



# COMMAND ----------

# lit is literals, it is an expression.
from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("One")).show(2) # here we create a column with constant value 1


# COMMAND ----------

df.withColumn("numberOne", lit(1)).show(2) # a formal way to create a column

# the first argument is the column name
# the second argument is the manipulative expression of the column

# COMMAND ----------

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))\
  .show(2)


# COMMAND ----------

df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns


# COMMAND ----------

dfWithLongColName = df.withColumn(
    "This Long Column-Name",
    expr("ORIGIN_COUNTRY_NAME"))


# COMMAND ----------

dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`")\
  .show(2)


# COMMAND ----------

dfWithLongColName.select(expr("`This Long Column-Name`")).columns


df.drop("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME") # drop out columns

# change a column's data type (cast)
df.withColumn("count2", col("count").cast("long"))


# COMMAND ----------

# we can chain multiple AND filters
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia")\
  .show(2)

# we can also use string expression
# remember when we use string expression, it's syntax should be SQL statement
df.where("count < 2").where("ORIGIN_COUNTRY_NAME != 'Croatia'").show(2)

# where = filter, we can use both key words in Spark

# COMMAND ----------

df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()


# COMMAND ----------

df.select("ORIGIN_COUNTRY_NAME").distinct().count()


# COMMAND ----------

seed = 5
withReplacement = False
fraction = 0.5
df.sample(withReplacement, fraction, seed).count()


# COMMAND ----------

dataFrames = df.randomSplit([0.25, 0.75], seed)
dataFrames[0].count() > dataFrames[1].count() # False


# COMMAND ----------

from pyspark.sql import Row
schema = df.schema
newRows = [
  Row("New Country", "Other Country", 5L),
  Row("New Country 2", "Other Country 3", 1L)
]
parallelizedRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizedRows, schema) # create new df with same schema of df


# COMMAND ----------

df.union(newDF)\
  .where("count = 1")\
  .where(col("ORIGIN_COUNTRY_NAME") != "United States")\
  .show()


# COMMAND ----------

df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

# sort = orderBy, we can use both key words in Spark

# COMMAND ----------

from pyspark.sql.functions import desc, asc
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)


# COMMAND ----------

spark.read.format("json").load("/data/flight-data/json/*-summary.json")\
  .sortWithinPartitions("count")


# COMMAND ----------

df.limit(5).show()


# COMMAND ----------

df.orderBy(expr("count desc")).limit(6).show()


# COMMAND ----------

df.rdd.getNumPartitions() # 1


# COMMAND ----------

df.repartition(5)


# COMMAND ----------

df.repartition(col("DEST_COUNTRY_NAME"))


# COMMAND ----------

df.repartition(5, col("DEST_COUNTRY_NAME"))


# COMMAND ----------

df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)

# repartition will incur a full shuffle of the data.
# coalesce will not incur a full shuffle and will try to combine partitions.


# COMMAND ----------

collectDF = df.limit(10)
collectDF.take(5) # take works with an Integer count
collectDF.show() # this prints it out nicely
collectDF.show(5, False)
collectDF.collect()


# COMMAND ----------

