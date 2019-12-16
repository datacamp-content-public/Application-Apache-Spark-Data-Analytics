---
title: 'Tuning the model'
description: 'What is tuning?  What are pipelines?  What is cross-validation?  What is automated tuning?  What is grid search?  Data selection.  How choice of data affects training.   Analyzing the sensitivity of accuracy to the number of likes. Comparing and contrasting to approaches to tuning.  How can we use sensitivity analysis to assess the goodness of a trained model.  The concepts of “coverage”, “accuracy threshold”, and “small sample performance”.'
---

## Tuning concepts

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
success_msg("Some praise! Then reinforce a learning objective from the exercise.")
```

---

## Practice tuning concepts

```yaml
type: DragAndDropExercise
key: d3ffa7949e
kind: Classify
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

`@solution`
```{python}
# Edit or remove this code to create your own exercise.
# This is 1 type of drag and drop exercise, there are 2 other types. See documentation:
# http://instructor-support.datacamp.com/en/articles/3039539-course-drag-drop-exercises

# Make sure you only use SPACES, NOT TABS in front of each line.

# Drag zone that holds all the options.
# Specify an ID for this zone to use in SCTs.
- id: options
  title: "Options" # Title of your zone This is not shown with more than 2 zones.

# You can keep adding drop zones to sort to.
# This example has 2 zones.
- id: dropzone_r
  title: "R"
  items: # Each drop zone has a list of items it contains. These will be shown in a random fashion.
    - content: "stringr" # Name of an item. Feel free to use markdown.
      id: stringr # ID of the item. This can be used in the SCTs.
    - content: "dplyr"
      id: dplyr

- id: dropzone_python
  title: "Python"
  items:
    - content: "pandas"
      id: pandas
    - content: "numpy"
      id: numpy
    
```

`@sct`
```{python}
checks: # Individual checks and custom messages per item. This is optional. Without it, it will check that the options are as in the solution code.
  - condition: check_target(pandas) == dropzone_python # Check that pandas is in dropzone_python.
    incorrectMessage: 'Hmm! Pandas is a Python package.' # If that condition is not true, show this message.
  - condition: check_target(numpy) == dropzone_python
    incorrectMessage: 'Damn, this is far from perfect!'
  - condition: check_target(dplyr) == dropzone_r
    incorrectMessage: "Hmm, keep doing R courses! :-)"
  - condition: check_target(stringr) == dropzone_r
    incorrectMessage: "How funny if stringr would be a Python package."
successMessage: "Congratulations" # Message shown when all is correct.
failureMessage: "Try again!" # Message shown when there are errors (and there is no specific error available).
isOrdered: false # Should the items in the zones be ordered as in the solution code?
```

---

## Analyzing model performance by number of likes

```yaml
type: VideoExercise
key: 98777d6e96
xp: 50
```

`@projector_key`
3d953865579dbf6fcd9c1bba0ed41a88

---

## Practice analyzing model performance by number of likes

```yaml
type: NormalExercise
key: 2dd86fc029
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
success_msg("Some praise! Then reinforce a learning objective from the exercise.")
```

---

## Testing on successively exclusive cohorts

```yaml
type: VideoExercise
key: 465eea9af1
xp: 50
```

`@projector_key`
9362759f91d79d6160e789858983d992

---

## Practice testing on successively exclusive cohorts

```yaml
type: NormalExercise
key: 3ab3df8297
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

## Does adjusting the min number of likes improve AUC?

```yaml
type: VideoExercise
key: fd4e7b9f99
xp: 50
```

`@projector_key`
8ac99ecbe5109f17b6451460269dd6c6

---

## Practice tuning the min number of likes

```yaml
type: NormalExercise
key: 9574eb3d82
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
