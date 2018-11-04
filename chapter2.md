---
title: 'Using window function sql for natural language processing'
description: ""
---

## Capstone: 

```yaml
type: NormalExercise
key: a70a4bc009
xp: 100
```



`@instructions`


`@hint`


`@pre_exercise_code`
```{python}
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
punctuation = ".\?\!\",\'()"
df = spark.read.text("sherlock.txt") \
          .select(split('value', '[ %s]' % punctuation).alias('word'))\
          .select(explode('word').alias('word'))\
          .select(lower(col('word')).alias('word'))\
          .where(length('word') > 0)
df.createOrReplaceTempView("df")

```

`@sample_code`
```{python}

```

`@solution`
```{python}

```

`@sct`
```{python}

```
