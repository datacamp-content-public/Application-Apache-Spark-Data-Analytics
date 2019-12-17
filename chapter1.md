---
title: 'Transforming the dataset into feature data'
description: 'Why is classification so useful.  The dataset.  Loading a CSV file. Transactional log data vs vectorized feature data. Transforming transactional user log data into tabular format. Analyzing data for suitability for training a classification model.  What is the power law concept and how to apply it to a candidate classification dataset.'
---

## The dataset

```yaml
type: VideoExercise
key: 0479d23060
xp: 50
```

`@projector_key`
3c3ec6dec8f48898b3b494b8e5e8720e

---

## Practice analyzing a dataset

```yaml
type: MultipleChoiceExercise
key: 5f05699dd2
xp: 50
```

<!-- Guidelines for the question: https://instructor-support.datacamp.com/en/articles/2375523-course-multiple-choice-with-console-exercises. -->

`@possible_answers`
- [Correct answer 1]
- Wrong answer 2
- Wrong answer 3

`@hint`
<!-- Examples of good hints: https://instructor-support.datacamp.com/en/articles/2379164-hints-best-practices. -->
- This is an example hint.
- This is an example hint.

`@pre_exercise_code`
```{python}

```

`@sct`
```{python}
# Check https://instructor-support.datacamp.com/en/articles/2375523-course-multiple-choice-with-console-exercises on how to write feedback messages for this exercise.
```

---

## Creating a schema

```yaml
type: VideoExercise
key: 1277f67760
xp: 50
```

`@projector_key`
1e00ff8db6bf6bb0f17ec58bdb83d881

---

## Practice creating a simple schema

```yaml
type: NormalExercise
key: bbf64a8d7e
xp: 100
```

It can be difficult to ascertain the data type of sparse transactional log file data. A schema allows us to tell Spark what to call a column, and what type to apply to the column. Here we do this for a schema having a single column

`@instructions`
<!-- Guidelines for instructions https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->
- Create a schema for field `uid` of type string

`@hint`
- Use `StructField` to specify the column.  Use `StringType()` to specify that it is a string.

`@pre_exercise_code`
```{python}
#_init_spark = '/home/repl/.init-spark.py' 
#with open(_init_spark) as f:
#    code = compile(f.read(), _init_spark, 'exec')
#    exec(code)
#from pyspark.sql import SparkSession
#spark = SparkSession.builder.getOrCreate()

from pyspark.sql.types import ArrayType, StringType, IntegerType, StructType, StructField

```

`@sample_code`
```{python}
# Create a schema for field uid of type string
schema = StructType([____("uid", ____())])
print(schema)

```

`@solution`
```{python}
# Create a schema for field uid of type string
schema = StructType([StructField("uid", StringType())])
print(schema)

```

`@sct`
```{python}
# Examples of good success messages: https://instructor-support.datacamp.com/en/articles/2299773-exercise-success-messages.
success_msg("That is correct.  This schema would work for a log file containing a single column of string data.")
```

---

## Practice creating a multi-column schema

```yaml
type: NormalExercise
key: 1db44462cc
xp: 100
```

In the previous exercise we created a schema containing a single column.  Now we will create a schema having multiple columns.

`@instructions`
- Create a schema having two fields, where the first field is called `uid` and is a string, and the second field is called `gender` and is an integer

`@hint`
- The schema is a `StructType`.  Use `IntegerType()` to specify an integer type.

`@pre_exercise_code`
```{python}
#_init_spark = '/home/repl/.init-spark.py' 
#with open(_init_spark) as f:
#    code = compile(f.read(), _init_spark, 'exec')
#    exec(code)
#from pyspark.sql import SparkSession
#spark = SparkSession.builder.getOrCreate()

from pyspark.sql.types import ArrayType, StringType, IntegerType, StructType, StructField

```

`@sample_code`
```{python}
# Schema with col uid type string and col gender type int
schema = ____([StructField("uid", ____),
                     StructField("gender", ____)])

```

`@solution`
```{python}
# Schema with col uid type string and col gender type int
schema = StructType([StructField("uid", StringType()),
                     StructField("gender", IntegerType())])

```

`@sct`
```{python}
# Examples of good success messages: https://instructor-support.datacamp.com/en/articles/2299773-exercise-success-messages.
```

---

## Loading comma delimited data

```yaml
type: VideoExercise
key: cf831c5999
xp: 50
```

`@projector_key`
4eff04f99cdf88f9d0903e9f917bd459

---

## Practice loading comma delimited data

```yaml
type: NormalExercise
key: 2b30554386
xp: 100
```

A variable `schema` is provided, specifying three fields:

```
schema = StructType([StructField("uid", StringType()),
                     StructField("rabbit", IntegerType()),
                     StructField("likes", StringType())])

```

We're going to use this schema to load a log file having three columns. The log file is in CSV format.

`@instructions`
<!-- Guidelines for instructions https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->
- Load the CSV file using the provided schema

`@hint`
<!-- Examples of good hints: https://instructor-support.datacamp.com/en/articles/2379164-hints-best-practices. -->
- use the `csv` function to read the file, and set the `schema` argument to the name of the provided schema.

`@pre_exercise_code`
```{python}
from pyspark.sql.types import ArrayType, StringType, IntegerType, StructType, StructField
schema = StructType([StructField("uid", StringType()),
                     StructField("rabbit", IntegerType()),
                     StructField("likes", StringType())])

# We might want to have the learner load a smaller version of rabbitduck.csv to avoid session timeout


```

`@sample_code`
```{python}
# Load the CSV file using the provided schema
df = spark.read.____('data/rabbitduck/rabbitduck.csv', header=True, schema=____)\

```

`@solution`
```{python}
# Load the CSV file using the provided schema
df = spark.read.csv('data/rabbitduck/rabbitduck.csv', header=True, schema=schema)\

```

`@sct`
```{python}
# Examples of good success messages: https://instructor-support.datacamp.com/en/articles/2299773-exercise-success-messages.
success_msg("That is correct.  This schema tells Spark to expect three columns, what to call them, and what type they should be.")
```

---

## Practice splitting comma delimited data

```yaml
type: NormalExercise
key: b5dc86e1a5
xp: 100
```

A dataframe `df` is provided. It has a column `likes` that contains comma delimited data. SQL functions have been imported as follows: 

```
import pyspark.sql.functions as fun

```


`@instructions`
<!-- Guidelines for instructions https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->
- Convert the column `likes` to an array of strings

`@hint`
<!-- Examples of good hints: https://instructor-support.datacamp.com/en/articles/2379164-hints-best-practices. -->
- Use `fun.split()` to convert the string to an array of strings.

`@pre_exercise_code`
```{python}
from pyspark.sql.types import ArrayType, StringType, IntegerType, StructType, StructField
import pyspark.sql.functions as fun

schema = StructType([StructField("uid", StringType()),
                     StructField("rabbit", IntegerType()),
                     StructField("likes", StringType())])

null_array_udf = fun.udf(lambda x:
                x if (x and type(x) is list and len(x)>0 )
                else [],
                ArrayType(StringType()))

df = spark.read.csv('data/rabbitduck/rabbitduck.csv', header=True, schema=schema)

# can minimize the code above by loading a precalculated version of df


```

`@sample_code`
```{python}
# Convert the likes column to an array of strings
df_result = df.withColumn('likes', fun.____('likes', ','))
```

`@solution`
```{python}
# Convert the likes column to an array of strings
df_result = df.withColumn('likes', fun.split('likes', ','))
```

`@sct`
```{python}
# Examples of good success messages: https://instructor-support.datacamp.com/en/articles/2299773-exercise-success-messages.
```

---

## Handling null entries

```yaml
type: VideoExercise
key: 366e37089b
xp: 50
```

`@projector_key`
37e898d3be8a730bd6610a6d15828596

---

## Practice using a UDF to handle null entries

```yaml
type: NormalExercise
key: ed8e9509f8
xp: 100
```

<!-- Guidelines for contexts: https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->

`@instructions`
<!-- Guidelines for instructions https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->
- Instruction 1
- Instruction 2

`@hint`
<!-- Examples of good hints: https://instructor-support.datacamp.com/en/articles/2379164-hints-best-practices. -->
- This is an example hint.
- This is an example hint.

`@pre_exercise_code`
```{python}
from pyspark.ml.feature import CountVectorizer
from pyspark.sql.types import ArrayType, StringType, IntegerType, StructType, StructField
from pyspark.ml.classification import LogisticRegression
import pyspark.sql.functions as fun
from pyspark import SQLContext

sqlContext = SQLContext.getOrCreate(spark.sparkContext)

schema = StructType([StructField("uid", StringType()),
                     StructField("rabbit", IntegerType()),
                     StructField("likes", StringType())])

null_array_udf = fun.udf(lambda x:
                x if (x and type(x) is list and len(x)>0 )
                else [],
                ArrayType(StringType()))

df_has_nulls = spark.read.csv('data/rabbitduck/rabbitduck.csv', header=True, schema=schema)\
           .withColumn('likes', fun.split('likes', ','))

#  Can remove much of the above by loading a precalculated version of df_has_nulls

           .withColumn('numlikes', fun.when(fun.col('likes').isNull(),0).otherwise(fun.size('likes')))\
           .withColumn('likes', null_array_udf('likes'))


```

`@sample_code`
```{python}

```

`@solution`
```{python}

```

`@sct`
```{python}
# Examples of good success messages: https://instructor-support.datacamp.com/en/articles/2299773-exercise-success-messages.
```

---

##  (OLD SPARE)

```yaml
type: NormalExercise
key: 782514209a
xp: 100
```

Hello Hello Hello Hello Hello Hello Hello Hello Hello Hello Hello

`@instructions`
Load csv data from the file "trainsched.txt" into a dataframe stored in a variable named 'df'.

`@hint`
A synonym for "load" is "read".  Don't forget to specify the format.

`@pre_exercise_code`
```{python}
_init_spark = '/home/repl/.init-spark.py' 
with open(_init_spark) as f:
    code = compile(f.read(), _init_spark, 'exec')
    exec(code)
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
```

`@sample_code`
```{python}
df=spark.____.___("trainsched.txt",header=True)
```

`@solution`
```{python}
df = spark.read.csv("trainsched.txt",header=True)
```

`@sct`
```{python}
#assert len(df.columns)==3, "Wrong number of columns -- did you use the right separator?"
#assert df.columns == ['train_id', 'station', 'time'], "Incorrect column names"
#assert df.count()==14, "The number of rows is incorrect"
#assert df.select("train_id").distinct().count()==2, "There should be two train_id's."
#assert sorted([x["train_id"] for x in df.select("train_id").distinct().collect()])[0]=='217',"Missing train 217"
#assert sorted([x["train_id"] for x in df.select("train_id").distinct().collect()])[1]=='324',"Missing train 324"


success_msg("Loading simple comma-separated text data into a dataframe is a breeze.")
```
