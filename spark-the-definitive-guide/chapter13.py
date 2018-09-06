myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"\
  .split(" ")
words = spark.sparkContext.parallelize(myCollection, 2)


# COMMAND ----------

words.map(lambda word: (word.lower(), 1))


# COMMAND ----------

keyword = words.keyBy(lambda word: word.lower()[0])


# COMMAND ----------

keyword.mapValues(lambda word: word.upper()).collect()


# COMMAND ----------

keyword.flatMapValues(lambda word: word.upper()).collect()


# COMMAND ----------

keyword.keys().collect()
keyword.values().collect()


# COMMAND ----------

import random
distinctChars = words.flatMap(lambda word: list(word.lower())).distinct()\
  .collect()
# Output: ['a', 'c', 'e', 'g', 'i', 'k', 'm', 'o', 's', 'u', 'b', 'd', 'f', 'h', 'l', 'n', 'p', 'r', 't', 'v', ':']
sampleMap = dict(map(lambda c: (c, random.random()), distinctChars))
# Out:
# {'a': 0.7948074373878998, 'c': 0.9076665348218508, 'b': 0.3831648889037895, 
# 'e': 0.04657666734974586, 'd': 0.6073179652617912, 'g': 0.8760752938051651, 
# 'f': 0.28560844318964873, 'i': 0.19678450530688507, 'h': 0.6158423569803951, 
# 'k': 0.9832064290350814, 'm': 0.20427680025924633, 'l': 0.93433954603612, 
# 'o': 0.14112183908414477, 'n': 0.20782906815306856, 'p': 0.36861610762594343, 
# 's': 0.4884122139642344, 'r': 0.14482218398924362, 'u': 0.7437036571781355, 
# 't': 0.694165892225837, 'v': 0.8356128622582889, ':': 0.358892352044211}
words.map(lambda word: (word.lower()[0], word))\
  .sampleByKey(True, sampleMap, 6).collect()
# sampleByKey(withReplacement, fractions, seed=None)
# which produces a sample of size that’s approximately equal to the sum of math.ceil(numItems * samplingRate) over all key values




# COMMAND ----------

chars = words.flatMap(lambda word: word.lower())
KVcharacters = chars.map(lambda letter: (letter, 1))
def maxFunc(left, right):
  return max(left, right)
def addFunc(left, right):
  return left + right
nums = sc.parallelize(range(1,31), 5)


# COMMAND ----------

KVcharacters.countByKey()


# COMMAND ----------

KVcharacters.groupByKey().map(lambda row: (row[0], reduce(addFunc, row[1])))\
  .collect()
# note this is Python 2, reduce must be imported from functools in Python 3


# COMMAND ----------

nums.aggregate(0, maxFunc, addFunc)


# COMMAND ----------

depth = 3
nums.treeAggregate(0, maxFunc, addFunc, depth)


# COMMAND ----------

KVcharacters.aggregateByKey(0, addFunc, maxFunc).collect()


# COMMAND ----------

def valToCombiner(value):
  return [value]
def mergeValuesFunc(vals, valToAppend):
  vals.append(valToAppend)
  return vals
def mergeCombinerFunc(vals1, vals2):
  return vals1 + vals2
outputPartitions = 6
KVcharacters\
  .combineByKey(
    valToCombiner,
    mergeValuesFunc,
    mergeCombinerFunc,
    outputPartitions)\
  .collect()


# COMMAND ----------

KVcharacters.foldByKey(0, addFunc).collect()


# COMMAND ----------

import random
distinctChars = words.flatMap(lambda word: word.lower()).distinct()
charRDD = distinctChars.map(lambda c: (c, random.random()))
charRDD2 = distinctChars.map(lambda c: (c, random.random()))
charRDD.cogroup(charRDD2).take(5)


# COMMAND ----------

keyedChars = distinctChars.map(lambda c: (c, random.random()))
outputPartitions = 10
KVcharacters.join(keyedChars).count()
KVcharacters.join(keyedChars, outputPartitions).count()

# KVcharacters.join(keyedChars) has the following output
# Out[27]: 
# [('a', (1, 0.9938556229112656)),
#  ('a', (1, 0.9938556229112656)),
#  ('a', (1, 0.9938556229112656)),
#  ('a', (1, 0.9938556229112656)),
#  ('i', (1, 0.2979858878187205)),
#  ('i', (1, 0.2979858878187205)),
#  ('i', (1, 0.2979858878187205)),
#  ('i', (1, 0.2979858878187205)),
#  ('i', (1, 0.2979858878187205)),
#  ('i', (1, 0.2979858878187205)),
#  ('i', (1, 0.2979858878187205)),
#  ('m', (1, 0.041322570703785755)),
#  ('m', (1, 0.041322570703785755))...



# COMMAND ----------

numRange = sc.parallelize(range(10), 2)
words.zip(numRange).collect()


# COMMAND ----------

words.coalesce(1).getNumPartitions() # 1


# COMMAND ----------

df = spark.read.option("header", "true").option("inferSchema", "true")\
  .csv("/data/retail-data/all/")
rdd = df.coalesce(10).rdd


# COMMAND ----------

def partitionFunc(key):
  import random
  if key == 17850 or key == 12583:
    return 0
  else:
    return random.randint(1,2)

# keyBy command reference: http://blog.cheyo.net/180.html
keyedRDD = rdd.keyBy(lambda row: row[6])
keyedRDD\
  .partitionBy(3, partitionFunc)\
  .map(lambda x: x[0])\
  .glom()\
  .map(lambda x: len(set(x)))\
  .take(5)


# COMMAND ----------