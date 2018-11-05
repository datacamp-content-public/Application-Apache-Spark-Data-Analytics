---
title: 'Caching, logging, and query plans in Spark SQL'
description: ""
---

## Capstone

```yaml
type: NormalExercise
key: 6b93918e78
xp: 100
```

The lesson demonstrated how caching helps (or not) when a second dataframe depends on another dataframe. This exercise practices caching two dataframes, one of which depends on the other.

A dataframe `df1` is loaded from a csv file. Several processing steps are performed on this dataframe. `df1` has 606,568 rows. As `df1` is to be used more than once it is a candidate for caching.

A second dataframe `df2` is created by copying `df1`, then performing two additional compute-intensive steps. It has 499,691 rows.  It is also a candidate for caching.  However, because `df2` depends on `df1` the question arises: should we cache `df1`, or cache `df2`, or cache both?  Note that caching incurs a cost. Caching costs run time as well as memory.

`@instructions`
Four operations are performed on the two dataframes.  Each one is timed. These four operations are captioned as follows:

1. df1_1st
2. df1_2nd
3. df2_1st
4. df2_2nd

* There are two cache statements. 
* Initially the second one is commented out. 
* These statements are called "Caching df1" and "Caching df2". 

* Below the four timed operations are True or False questions.  
* Set the corresponding variable below the question if you believe the answer is True.  
* Otherwise set it to False.

`@hint`
Try enabling or disabling the commented out caching statements and running (without submitting). Try all four combinations.

`@pre_exercise_code`
```{python}
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
# Overall run times: 
# both off: 3.6
# df1: 1.6
# df2: 3.2
# df1 and df2: 2.0

assert a1 == False
assert a2 == True
assert a3 == False
assert a4 == False
```
