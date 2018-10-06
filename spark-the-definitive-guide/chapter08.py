person = spark.createDataFrame([
    (0, "Bill Chambers", 0, [100]),
    (1, "Matei Zaharia", 1, [500, 250, 100]),
    (2, "Michael Armbrust", 1, [250, 100])])\
  .toDF("id", "name", "graduate_program", "spark_status")
graduateProgram = spark.createDataFrame([
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley")])\
  .toDF("id", "degree", "department", "school")
sparkStatus = spark.createDataFrame([
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor")])\
  .toDF("id", "status")

person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")


# COMMAND ----------

joinExpression = person["graduate_program"] == graduateProgram['id']


# COMMAND ----------

wrongJoinExpression = person["name"] == graduateProgram["school"]

person.join(graduateProgram, joinExpression).show()
# default join is inner-join


# COMMAND ----------

joinType = "inner"
person.join(graduateProgram, joinExpression, joinType).show()


joinType = 'outter'
person.join(graduateProgram, joinExpression, joinType).show()
# For outer join, if there is no equivalent row in either the left or right DataFrame, Spark will insert null.

# Other join type:
# Left outer joins (keep rows with keys in the left dataset)
# Right outer joins (keep rows with keys in the right dataset)
# Left semi joins (keep the rows in the left, and only the left, dataset where the key appears in the right dataset)
# Left anti joins (keep the rows in the left, and only the left, dataset where they do not appear in the right dataset)
# Natural joins (perform a join by implicitly matching the columns between the two datasets with the same names)
# Cross (or Cartesian) joins (match every row in the left dataset with every row in the right dataset)


# COMMAND ----------

gradProgram2 = graduateProgram.union(spark.createDataFrame([
    (0, "Masters", "Duplicated Row", "Duplicated School")]))

gradProgram2.createOrReplaceTempView("gradProgram2")


# COMMAND ----------

# Joins on Complex Types
#Even though this might seem like a challenge, 
# it’s actually not. Any expression is a valid join expression, assuming that it returns a Boolean.
from pyspark.sql.functions import expr

person.withColumnRenamed("id", "personId")\
  .join(sparkStatus, expr("array_contains(spark_status, id)")).show()

# Output: Notice how the left rows are now duplicated to align with the id from right rows 
# +--------+----------------+----------------+---------------+---+--------------+
# |personId|            name|graduate_program|   spark_status| id|        status|
# +--------+----------------+----------------+---------------+---+--------------+
# |       0|   Bill Chambers|               0|          [100]|100|   Contributor|
# |       1|   Matei Zaharia|               1|[500, 250, 100]|500|Vice President|
# |       1|   Matei Zaharia|               1|[500, 250, 100]|250|    PMC Member|
# |       1|   Matei Zaharia|               1|[500, 250, 100]|100|   Contributor|
# |       2|Michael Armbrust|               1|     [250, 100]|250|    PMC Member|
# |       2|Michael Armbrust|               1|     [250, 100]|100|   Contributor|
# +--------+----------------+----------------+---------------+---+--------------+



# COMMAND ----------

graduateProgram = graduateProgram.withColumnRenamed("id", "graduate_program")
joinExpr = graduateProgram.graduate_program == person.graduate_program
person.join(graduateProgram, joinExpr).show()

# Handle keys with same name
# 01. different join expression
# When you have two keys that have the same name, probably the easiest fix is to change 
# the join expression from a Boolean expression to a string or sequence. 
# This automatically removes one of the columns for you during the join

person.join(graduateProgram, "graduate_program").show() # explicitly pass join string will drop off duplicated key in result dataset.

# 02. dorpping the column after join
person.join(graduateProgram, joinExpr).drop(person.graduate_program).show()

# 03. renaming a column before the join
gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
joinExpr = person.graduate_program == gradProgram3.grad_id
person.join(gradProgram3, joinExpr).show() 
