---
title: 'Transforming text into vector format'
description: ""
---

## (Capstone) Transforming text into vector format.

```yaml
type: NormalExercise
key: a3a2783a44
xp: 100
```

In the lesson we learned how to split sentences. We also learned how to transform an array of words into a numerical vector. We used the CountVectorizer model for this transform step. 

A dataframe `df` is provided having the following columns: 'sentence', 'in', and 'out'. Each column is an array of strings. `sentence` is a list of words representing a sentence from a text book. The 'out' column gives the last word of 'sentence'. The 'in' column is obtained by removing the last word from 'sentence'.

A CountVectorizer model has been created using the following two lines of code: 
`cv = CountVectorizer(inputCol='words', outputCol='vec')`
`model = cv.fit(df.select(col('sentence').alias('words')))`

`@instructions`
1. Create a dataframe called `result` by using `model` to transform `df`. `result` has the columns `sentence`, `in`, `out`, and  `invec`. `invec` is the vector transform of the `in` column.
2. Add a column to `result` called `outvec`. `result` now has the columns `sentence`, `in`, `out`, `invec`, and `outvec`.

`@hint`
First, use `model` to transform `df` after renaming `in` to `words`.  Then, perform the similar operation on the `out` column, but applying the transform to the `result` dataframe in order to preserve the result of the first operation.

`@pre_exercise_code`
```{python}
_init_spark = '/home/repl/.init-spark.py' 
with open(_init_spark) as f:
    code = compile(f.read(), _init_spark, 'exec')
    exec(code)
from pyspark.sql.types import StringType, ArrayType, BooleanType
from pyspark.sql.functions import split, explode, col, desc, lower, length, monotonically_increasing_id, regexp_replace, collect_list, concat_ws,trim, udf, size
from pyspark.ml.feature import CountVectorizer
sqlContext = SQLContext.getOrCreate(spark.sparkContext)
df = sqlContext.read.load('sherlock_sentences.parquet')
df = df.where(" id>100 and id<120 ")

#   Splits clauses on the end word
in_udf = udf(lambda x: x[0:len(x)-1] if x and len(x) > 1 else [], ArrayType(StringType())) # rm end word
out_udf = udf(lambda x: x[len(x)-1:len(x)] if x and len(x) > 1 else x, ArrayType(StringType())) # only end word
df = df.select(split('clause', ' ').alias('sentence')) \
        .where(size('sentence') > 1) \
        .withColumn('in', in_udf('sentence')) \
        .withColumn('out', out_udf('sentence'))\
        .where(size('out') > 0)
cv = CountVectorizer(inputCol='words', outputCol='vec')
model = cv.fit(df.select(col('sentence').alias('words')))
```

`@sample_code`
```{python}
# Transforms df using model.
# Has a column based on the 'in' column called 'invec'
result = model.____(____.withColumnRenamed('in', '____'))\
        .withColumnRenamed('____','in')\
        .withColumnRenamed('____', 'invec')
result.show(3,False)
#   Adds a column based on the 'out' column called 'outvec'
result = ____.transform(____.withColumnRenamed('____','words'))\
        .withColumnRenamed('words','____')\
        .withColumnRenamed('vec', '____')
result.show(3,False)
```

`@solution`
```{python}
# Transforms df using model.
# Has a column based on the 'in' column called 'invec'
result = model.transform(df.withColumnRenamed('in', 'words'))\
        .withColumnRenamed('words','in')\
        .withColumnRenamed('vec', 'invec')
result.show(3,False)
# Adds a column based on the 'out' column called 'outvec'
result = model.transform(result.withColumnRenamed('out','words'))\
        .withColumnRenamed('words','out')\
        .withColumnRenamed('vec', 'outvec')
result.show(3,False)
```

`@sct`
```{python}

```
