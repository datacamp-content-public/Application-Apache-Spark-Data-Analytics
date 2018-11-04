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

Previously we saw how to perform a sliding window to find all word sequences of length three ("3-tuples").
Following is a sql query that finds all sequences of length four: 

```
sql_4tuples = """
select
word as w1,
lead(word,1) over(order by id) as w2,
lead(word,2) over(order by id) as w3,
lead(word,3) over(order by id) as w4
from df
"""
```

Previously we also saw how to use a traditional sql aggregation query that used a window query as a subquery.
That allowed us to find the most common 3-tuples in the text document. Now you'll perform the similar task 
to find the most common 4-tuples. 


`@instructions`
There is a dataframe called `df` having two columns: `word` and `id`. The id column is a integer such that a word that comes later in the document has a larger id. The dataframe `df` is also registered as temporary table called `df`.  Find the **15** most common 4-tuples in the dataset. Call the result df_a. It must have five columns named `w1`, `w2`, `w3`, `w4`, and `count`. (`w1`, `w2`, `w3`, `w4`) corresponds to a 4-tuple, and `count` indicates how many times it occurred in the dataset.

`@hint`


`@pre_exercise_code`
```{python}
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sqlContext = SQLContext.getOrCreate(spark.sparkContext)
df = sqlContext.read.load('sherlock.parquet')
df.createOrReplaceTempView('df')
spark.catalog.cacheTable('df')

#   Generates moving 4-tuple windows
sql_4tuples = """
select
word as w1,
lead(word,1) over(order by id) as w2,
lead(word,2) over(order by id) as w3,
lead(word,3) over(order by id) as w4
from df
"""
#   Tallies most frequent 4-tuples
sql_4tuple_counts = """
select w1,w2,w3,w4,count(*) as count
from
(
%s
)
group by w1,w2,w3,w4
order by count desc
limit 15
""" % sql_4tuples

df_correct = spark.sql(sql_4tuples)

```

`@sample_code`
```{python}

```

`@solution`
```{python}

query = """
select w1,w2,w3,w4,count(*) as count
from
(
%s
)
group by w1,w2,w3,w4
order by count desc
limit 15
""" % sql_4tuples

spark.sql(query)

```

`@sct`
```{python}

```
