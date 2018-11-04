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

A dataframe `df1` is loaded from a csv file. Several processing steps are performed on this dataframe. `df1` has 1,352,686 rows. On a typical default Spark installation, this dataframe is recalculated every time. If `df1` is to be used repeatedly it is a candidate for caching. 

A second dataframe `df2` is created by copying `df1`, then performing two additional compute-intensive steps. It has 1,105,177 rows.  It is also a candidate for caching.  However, because `df2` depends on `df1` the question arises: should we cache `df1`, or cache `df2`, or cache both?  Keep in mind that caching incurs a cost. 

`@instructions`
Four operations are performed.  Each one is timed. These four operations are captioned:

1. df1_1st
2. df1_2nd
3. df2_1st
4. df2_2nd

There are two cache statements that initially commented out. These are called "Caching df1" and "Caching df2". 

Below the four timed operations are True or False questions.  Set the corresponding variable below the question if you believe the answer is True.  Otherwise set it to False.

`@hint`


`@pre_exercise_code`
```{python}
import time 
from pyspark.sql.functions import split, explode, col, desc, lower, length, monotonically_increasing_id, regexp_replace, collect_list, concat_ws,trim 
punctuation = "_|.\?\!\",\'()"
df1 = spark.read.text('sherlock.txt') \
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

```

`@sample_code`
```{python}
df1.cache() # Caching df1
df2.cache() # Caching df2

begin=time.time() 
start=time.time() 
df1.count()
print("df1_1st :  %.1f" % (time.time()-start))

start=time.time() 
df1.count()
print("df1_2nd :  %.1f" % (time.time()-start))

start=time.time() 
df2.count()
print("df2_1st : %.1f" % (time.time()-start))

start=time.time() 
df2.count()
print("df2_2nd : %.1f" % (time.time()-start))

# Ensures consistent answers when rerun 
df1.unpersist()
df2.unpersist()

print("Overall elapsed : %.1f" % (time.time() - begin))


######   ANSWER SECTION   #####

# True or False?
# Caching df1 reduces df1_1st
a1 = False

# Caching df1 reduces df2_1st
a2 = True

# Caching df2 and not Caching df1 reduces df2_1st
a3 = False

# Caching both df1 and df2 gives the best overall run time. 
a4 = False

```

`@solution`
```{python}
df1.cache() # Caching df1
df2.cache() # Caching df2

begin=time.time() 
start=time.time() 
df1.count()
print("df1_1st :  %.1f" % (time.time()-start))

start=time.time() 
df1.count()
print("df1_2nd :  %.1f" % (time.time()-start))

start=time.time() 
df2.count()
print("df2_1st : %.1f" % (time.time()-start))

start=time.time() 
df2.count()
print("df2_2nd : %.1f" % (time.time()-start))

# Ensures consistent answers when rerun 
df1.unpersist()
df2.unpersist()

print("Overall elapsed : %.1f" % (time.time() - begin))


######   ANSWER SECTION   #####

# True or False?
# Caching df1 reduces df1_1st
a1 = False

# Caching df1 reduces df2_1st
a2 = True

# Caching df2 and not Caching df1 reduces df2_1st
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
