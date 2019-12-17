---
title: 'Training a logistic regression model'
description: "What is classification? What is logistic regression? Why is it used for classification? What are hyperparameters?  Using a training summary object. Evaluating a fitted model on a test dataset. How to calculate the estimated prediction accuracy of the fitted model. \n"
---

## (Capstone) Practicing caching

```yaml
type: NormalExercise
key: 6b93918e78
xp: 100
```

This exercise practices caching two dataframes, one of which depends on the other. A dataframe `df1` is loaded from a csv file. Several processing steps are performed on this dataframe. `df1` has 606,568 rows. As `df1` is to be used more than once it is a candidate for caching.

A second dataframe `df2` is created by copying `df1`, then performing additional compute-intensive steps. It has 499,691 rows.  It is also a candidate for caching.  However, because `df2` depends on `df1` the question arises: should we cache `df1`, or cache `df2`, or cache both?  Note that caching incurs a cost. Caching costs run time as well as memory.

The two dataframes are created, Then, four operations are performed on the two dataframes, two on `df1` and two on `df2`.  Each operation is timed.

`@instructions`
* There are two cache statements called "Caching df1" and "Caching df2".  Initially the second one is commented out. 
* There are four timed operations captioned `df1_1st`, `df1_2nd`, `df2_1st`, and `df2_2nd`.
* Below the four timed operations are True or False questions.  
* Set the corresponding variable below the question to True if you believe the answer is True.  Otherwise set it to False.

`@hint`
Try enabling or disabling the commented out caching statements and running (without submitting). Try all four combinations.

`@pre_exercise_code`
```{python}
_init_spark = '/home/repl/.init-spark.py' 
with open(_init_spark) as f:
    code = compile(f.read(), _init_spark, 'exec')
    exec(code)
import time 
from pyspark.sql.functions import split, explode, col, desc, lower, length, monotonically_increasing_id, regexp_replace, collect_list, concat_ws,trim 
punctuation = "_|.\?\!\",\'()"
df1 = spark.read.text('sherlock.txt') \
    .limit(50000)\
    .select(regexp_replace('value', 'Mr\.', 'Mr').alias('v'))\
    .select(regexp_replace('v', 'Mrs\.', 'Mrs').alias('v'))\
    .select(regexp_replace('v', 'Dr\.', 'Dr').alias('v'))\
    .select(regexp_replace('v', 'St\.', 'St').alias('v'))\
    .select(regexp_replace('v', 'No\.', 'Number').alias('v'))\
    .select(regexp_replace('v', 'pp\.', 'pages').alias('v'))\
    .select(regexp_replace('v', "'ll", 'will').alias('v'))\
    .select(regexp_replace('v', "n't", 'not').alias('v')) \
    .select(lower(col('v')).alias('v'))\
    .select(split('v', '[ %s]' % punctuation).alias('word'))\
    .select(explode('word').alias('word'))
df2 = df1.select(lower(col('word')).alias('word'))\
    .where(length('word') > 0)

begin=time.time()

def run(df, name, elapsed=False):
  start=time.time()
  df.count()
  print("%s : %.1fs" % (name, (time.time()-start)))
  if elapsed:
    elapsed()

def prep(df1, df2):
  df1.unpersist()
  df2.unpersist()
  begin = time.time()

def elapsed():
  print("Overall elapsed : %.1f" % (time.time() - begin))

```

`@sample_code`
```{python}
prep(df1, df2)
df1.cache() # Caching df1
#df2.cache() # Caching df2
run(df1, "df1_1st")
run(df1, "df1_2nd")
run(df2, "df2_1st")
run(df2, "df2_2nd", elapsed=True)
# True or False: Caching df1 only (and not Caching df2) reduces df1_1st
a1 = ____
# Caching df1 and NOT Caching df2 reduces df2_1st
a2 = ____
# NOT Caching df1 and Caching df2 reduces df2_1st
a3 = ____
# Caching both df1 and df2 gives the best overall run time. 
a4 = ____
```

`@solution`
```{python}
prep(df1, df2)
df1.cache() # Caching df1
#df2.cache() # Caching df2
run(df1, "df1_1st")
run(df1, "df1_2nd")
run(df2, "df2_1st")
run(df2, "df2_2nd", elapsed=True)
# True or False: Caching df1 only (and not Caching df2) reduces df1_1st
a1 = False
# Caching df1 and NOT Caching df2 reduces df2_1st
a2 = True
# NOT Caching df1 and Caching df2 reduces df2_1st
a3 = False
# Caching both df1 and df2 gives the best overall run time. 
a4 = False
```

`@sct`
```{python}
#assert a1 == False
#assert a2 == True
#assert a3 == False
#assert a4 == False

success_msg("Good! Caching is useful but should not be overused.")

```

---

## Splitting the train data

```yaml
type: NormalExercise
key: 2f5ebc22a7
xp: 100
```

Split the training data into two subsets, one for training, and one held out for evaluating the trained model. We'll use a seed of 42 so that the result is the same each time.
<!-- Guidelines for contexts: https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->

`@instructions`
<!-- Guidelines for instructions https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->
- Split the dataset into 80% training, 20% test.  Use

`@hint`
<!-- Examples of good hints: https://instructor-support.datacamp.com/en/articles/2379164-hints-best-practices. -->
- This is an example hint.
- This is an example hint.

`@pre_exercise_code`
```{python}
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
cv = CountVectorizer(inputCol='likes', outputCol='likesvec')
model = cv.fit(df)
dfx = model.transform(df)\
           .select('uid','rabbit','likesvec','numlikes')\
           .withColumnRenamed('rabbit','label')\
           .withColumnRenamed('likesvec','features')

```

`@sample_code`
```{python}
# Split the dataset into 80% training, 20% test
split = ____
df_trainset, df_testset = dfx.randomSplit(split, 42)
```

`@solution`
```{python}
# Split the dataset into 80% training, 20% test
split = (0.80,0.20)
df_trainset, df_testset = dfx.randomSplit(split, 42)
```

`@sct`
```{python}
# Here let's use smart SCT rather than pattern matching to allow learners to use a broader array of solutions with less templating. 
# The solution should be either a tuple or a list T, with at least two elements, such that T[0]/(sum(T))=0.8, and T[1]/(sum(T))=0.2
# Examples of good success messages: https://instructor-support.datacamp.com/en/articles/2299773-exercise-success-messages.
success_msg("This works. The first argument can be either a list or tuple.  The elements can be integer or floats.  Weights are normalized if they don’t sum up to 1.0.")
```

---

## Insert exercise title here

```yaml
type: NormalExercise
key: 11c3448e3f
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
success_msg("Well done! Window function sql can be used in a subquery just like a regular sql query.")

```

---

## Insert exercise title here

```yaml
type: NormalExercise
key: 78a79135bd
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
success_msg("Well done! Window function sql can be used in a subquery just like a regular sql query.")

```

---

## Insert exercise title here

```yaml
type: NormalExercise
key: d5aa9a9f24
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
success_msg("Well done! Window function sql can be used in a subquery just like a regular sql query.")

```

---

## Insert exercise title here

```yaml
type: NormalExercise
key: ae948da413
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
success_msg("Well done! Window function sql can be used in a subquery just like a regular sql query.")

```
