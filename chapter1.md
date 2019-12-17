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
success_msg("Perfect. You can specify additional columns by adding `StructType` elements to the `StructType` array.")
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

SQL functions have been imported as follows: 

```
import pyspark.sql.functions as fun

```

A dataframe `df` is provided. It has a column `likes` that contains comma delimited data. 

`@instructions`
<!-- Guidelines for instructions https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->
- Convert the `likes` column to an array of strings

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
success_msg("Correct. The SQL functions module contains many useful functions.")
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

## Practice using `when/otherwise` to handle null entries

```yaml
type: NormalExercise
key: ed8e9509f8
xp: 100
```

We are provided with a dataframe `df`, which contains a column `likes` that contains an array of strings.  However, sometimes the value of the `likes` column can be empty. We want to count the number of elements in the `likes` column, setting it to zero whenever it is empty. The SQL function `size` is suitable for counting the size of an array field. However, it requires that the field not be null. The SQL function `when` is suitable for handling the case where the `likes` field has a null value. The SQL functions are imported as follows: 

```
import pyspark.sql.functions as fun

```


<!-- Guidelines for contexts: https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->

`@instructions`
<!-- Guidelines for instructions https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->
- Use `when`/`otherwise` to add a column that counts the number of likes contained in the column `likes`, setting it to zero when the value of `likes` is null or an empty array.

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

df = spark.read.csv('data/rabbitduck/rabbitduck.csv', header=True, schema=schema)\
          .withColumn('likes', fun.split('likes', ','))

#  Can remove much of the above by loading a precalculated version of df


```

`@sample_code`
```{python}
# Add a column that counts the number of likes
df_result = \
    df.withColumn(\
  		'numlikes',\
        fun.____(fun.col('likes').isNull(), 0)\
           .____(fun.size('likes')))
```

`@solution`
```{python}
# Add a column that counts the number of likes
df_result = \
    df.withColumn(\
  		'numlikes',\
        fun.when(fun.col('likes').isNull(), 0)\
           .otherwise(fun.size('likes')))
```

`@sct`
```{python}
success_msg("Perfect. The SQL function `when` is useful for handling edge cases.")
# Examples of good success messages: https://instructor-support.datacamp.com/en/articles/2299773-exercise-success-messages.
```

---

## Using a UDF

```yaml
type: NormalExercise
key: 670f09864b
xp: 100
```

<!-- Guidelines for contexts: https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->
We are provided with a dataframe `df`. The dataframe has a column `likes`, which is an array of strings. However, the `likes` column has some null values. We have a UDF called `null_array_udf` that converts the null values to an empty array `[]`.  

`@instructions`
<!-- Guidelines for instructions https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->
- Add a column to the dataframe that converts all null values in `likes` to an empty array

`@hint`
<!-- Examples of good hints: https://instructor-support.datacamp.com/en/articles/2379164-hints-best-practices. -->
- The name of the function to use is `null_array_udf`.  Its argument is the name of the column on which it acts.

`@pre_exercise_code`
```{python}
from pyspark.ml.feature import CountVectorizer
from pyspark.sql.types import ArrayType, StringType, IntegerType, StructType, StructField
from pyspark.ml.classification import LogisticRegression
import pyspark.sql.functions as fun

schema = StructType([StructField("uid", StringType()),
                     StructField("rabbit", IntegerType()),
                     StructField("likes", StringType())])

null_array_udf = fun.udf(lambda x:
                x if (x and type(x) is list and len(x)>0 )
                else [],
                ArrayType(StringType()))

df = spark.read.csv('data/rabbitduck/rabbitduck.csv', header=True, schema=schema)\
           .withColumn('likes', fun.split('likes', ','))

# Can remove much of the above code by loading in a saved version of df
```

`@sample_code`
```{python}
df.show(5)

# Convert null values in likes to [] using null_array_udf
df_result = df.withColumn('likes', ____('____'))

df_result.show(5)
```

`@solution`
```{python}
df.show(5)

# Convert null values in likes to [] using null_array_udf
df_result = df.withColumn('likes', null_array_udf('likes'))

df_result.show(5)
```

`@sct`
```{python}
success_msg("Well done. User defined functions are used in much the same way as SQL functions.")
# Examples of good success messages: https://instructor-support.datacamp.com/en/articles/2299773-exercise-success-messages.
```

---

## Creating a UDF

```yaml
type: NormalExercise
key: 2d9d1aeadc
xp: 100
```

In the previous exercise we used a UDF called `null_array_udf` to convert null values to an empty array.  You will now create this UDF.

A dataframe `df` is provided.  It has a column `likes`, which contains values that are arrays of strings.  This column has some null values. 

The SQL functions are imported as follows: 

```
import pyspark.sql.functions as fun

```

You will use the SQL function `udf` to create `null_array_udf`. 

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

schema = StructType([StructField("uid", StringType()),
                     StructField("rabbit", IntegerType()),
                     StructField("likes", StringType())])

df = spark.read.csv('data/rabbitduck/rabbitduck.csv', header=True, schema=schema)\
           .withColumn('likes', fun.split('likes', ','))


```

`@sample_code`
```{python}
df.show(5)

# Create UDF replacing nulls with empty array of strings
null_array_udf = fun.____(lambda x:
                x if (x and type(x) is list and len(x)>0 )
                else ____,
                ArrayType(____))

df_result = df.withColumn('likes', null_array_udf('likes'))
df_result.show(5)
```

`@solution`
```{python}
df.show(5)

# Create UDF replacing nulls with empty array of strings
null_array_udf = fun.udf(lambda x:
                x if (x and type(x) is list and len(x)>0 )
                else [],
                ArrayType(StringType()))

df_result = df.withColumn('likes', null_array_udf('likes'))
df_result.show(5)
```

`@sct`
```{python}
success_msg("Well done. We used a lambda expression rather than a named function because this is more efficient.")
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
