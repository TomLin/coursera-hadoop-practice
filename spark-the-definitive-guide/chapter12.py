spark.range(10).rdd


# COMMAND ----------

spark.range(10).toDF("id").rdd.map(lambda row: row[0])


# COMMAND ----------

spark.range(10).rdd.toDF()


# COMMAND ----------

myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"\
  .split(" ")
words = spark.sparkContext.parallelize(myCollection, 2) # 2 is the partition number


# COMMAND ----------

words.setName("myWords") # the name being set will be shown on Spark UI
words.name() # myWords


# COMMAND ----------

words.distinct().count()

# COMMAND ----------

def startsWithS(individual):
  return individual.startswith("S") # individual is a string


# COMMAND ----------

words.filter(lambda word: startsWithS(word)).collect() 
# filter is similar to Where clause in SQL
# the function in filter() needs to return Boolean value


# COMMAND ----------

words2 = words.map(lambda word: (word, word[0], word.startswith("S")))


# COMMAND ----------

words2.filter(lambda record: record[2]).take(5)


# COMMAND ----------

words.flatMap(lambda word: list(word)).take(5)


# COMMAND ----------

words.sortBy(lambda word: len(word) * -1).take(2) # sort by descending


# COMMAND ----------

fiftyFiftySplit = words.randomSplit([0.5, 0.5])


# COMMAND ----------

# reduce is action in spark
spark.sparkContext.parallelize(range(1, 21)).reduce(lambda x, y: x + y) # 210


# COMMAND ----------

def wordLengthReducer(leftWord, rightWord):
  if len(leftWord) > len(rightWord):
    return leftWord
  else:
    return rightWord

words.reduce(wordLengthReducer)


# COMMAND ----------

words.getStorageLevel()


# COMMAND ----------

words.mapPartitions(lambda part: [1]).sum() # 2


# COMMAND ----------

def indexedFunc(partitionIndex, withinPartIterator):
  return ["partition: {} => {}".format(partitionIndex, \
                                  x) for x in withinPartIterator]
words.mapPartitionsWithIndex(indexedFunc).collect()

# mapPartitionsWithIndex will return `partitionIndex` and the iterator within that partition
# Out:
# ['partition: 0 => Spark',
#  'partition: 0 => The',
#  'partition: 0 => Definitive',
#  'partition: 0 => Guide',
#  'partition: 0 => :',
#  'partition: 1 => Big',
#  'partition: 1 => Data',
#  'partition: 1 => Processing',
#  'partition: 1 => Made',
#  'partition: 1 => Simple']

# COMMAND ----------

spark.sparkContext.parallelize(["Hello", "World"], 2).glom().collect()
# [['Hello'], ['World']]


# COMMAND ----------