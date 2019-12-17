---
title: 'Vectorizing the feature data'
description: 'What is Extract, Transform, and Select (ETS).  What is the CountVectorizer model.  Fitting the CountVectorizer model.  Analyzing a vectorizer''s vocabulary.  Dataset subset selection.'
---

## Insert exercise title here

```yaml
type: NormalExercise
key: 7e929385b9
xp: 100
```

<!-- Guidelines for contexts: https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->

`@instructions`
<!-- Guidelines for instructions https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->
- Instruction 1
- Instruction 2

`@hint`
<!-- Examples of good hints: https://instructor-support.datacamp.com/en/articles/2379164-hints-best-practices. -->
- Use the transform function on `cv_model`.
- Use the `select` function.
- Use the `withColumnRenamed` function to rename a column.
- Provide the previous column name as the first argument and the new name as the second argument.

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
           .withColumn('likes', fun.split('likes', ','))\
           .withColumn('numlikes', fun.when(fun.col('likes').isNull(),0).otherwise(fun.size('likes')))\
           .withColumn('likes', null_array_udf('likes'))

# Can remove much of the above by loading df from file

cv = CountVectorizer(inputCol='likes', outputCol='likesvec')
cv_model = cv.fit(df)

```

`@sample_code`
```{python}
# Use cv_model to transform the data in df
df_t = ____.____(df)

# Select uid, rabbit, likesvec, and numlikes columns
df_s = df_t.____('uid', 'rabbit', 'likesvec', 'numlikes')

# Rename the likesvec column to features
df_f = df_s.withColumnRenamed('____', '____')

# Rename the rabbit column to label
df_labeled = df_f.____('____','____')

```

`@solution`
```{python}
# Use cv_model to transform the data in df
df_t = cv_model.transform(df)

# Select uid, rabbit, likesvec, and numlikes columns
df_s = df_t.select('uid', 'rabbit', 'likesvec', 'numlikes')

# Rename the likesvec column to features
df_f = df_s.withColumnRenamed('likesvec', 'features')

# Rename the rabbit column to label
df_labeled = df_f.withColumnRenamed('rabbit','label')

```

`@sct`
```{python}
success_msg("Good. You have vectorized the feature data and renamed its columns to what is needed for the training step.")
```

---

## Insert exercise title here

```yaml
type: NormalExercise
key: 7fded5cc0d
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
success_msg("Well done! Window function sql can be used in a subquery just like a regular sql query.")
```

---

## Insert exercise title here

```yaml
type: NormalExercise
key: 46f8917ace
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
success_msg("Well done! Window function sql can be used in a subquery just like a regular sql query.")
```

---

## Insert exercise title here

```yaml
type: NormalExercise
key: 129afc16fd
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
success_msg("Well done! Window function sql can be used in a subquery just like a regular sql query.")
```

---

## Insert exercise title here

```yaml
type: NormalExercise
key: 0e300faa39
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
success_msg("Well done! Window function sql can be used in a subquery just like a regular sql query.")
```
