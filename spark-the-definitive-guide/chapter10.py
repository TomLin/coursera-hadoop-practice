spark.read.json("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")\
  .createOrReplaceTempView("some_sql_view") # DF => SQL

spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count)
FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
""")\
  .where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")\
  .count() # SQL => DF

# spark.sql() method will return a DataFrame.
# Noted, you need to use multi-line quote for multi-line SQL

# COMMAND ----------

# DataFrame vs Table: 
# To do anything useful with Spark SQL, you first need to define tables. 
# Tables are logically equivalent to a DataFrame in that they are a structure of data against which you run commands. 
# We can join tables, filter them, aggregate them, and perform different manipulations that we saw in previous chapters. 
# The core difference between tables and DataFrames is this: you define DataFrames in the scope of a programming language, 
# whereas you define tables within a database. This means that when you create a table (assuming you never changed the database), 
# it will belong to the default database. We discuss databases more fully later on in the chapter. 
# An important thing to note is that in Spark 2.X, tables always contain data . 
# There is no notion of a temporary table, only a view, which does not contain data. 
# This is important because if you go to drop a table, you can risk losing the data when doing so.

# Noted that Spark doesn't have temporary table, only has temporary view.
# Besides, Spark can create a table and upload data at the same time,
# unlike traditional SQL, we need to define the table first.


# Global Temp View is a deviation from Temp View:
# Global temp views are resolved regardless of database and are viewable across the entire Spark application, 
# but they are removed at the end of the session just like Temp View.

# Create a Complex data
# 01. create a struct: you simply need to wrap a set of columns (or expressions) in parentheses.

# 02. create a list: You can use the collect_list function, which creates a list of values. 
# You can also use the function collect_set , which creates an array without duplicate values. 
# These are both aggregation functions and therefore can be specified only in aggregations.

