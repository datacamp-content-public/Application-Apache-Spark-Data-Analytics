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

Previously we saw how to create a query that finds word sequences of length three ("3-tuples").
We used that query as a subquery in a traditional sql query 
to find the most common 3-tuples in the text document. 
Now you'll perform the similar task to find the most common 4-tuples. 
The following sql query finds all sequences of length four: 

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
There is a dataframe called `df` having two columns: `word` and `id`. The id column is a integer such that a word that comes later in the document has a larger id. The dataframe `df` is also registered as temporary table called `df`.  
 

`@instructions`
Create a query that finds the **15** most common 4-tuples in the dataset. 
Have it use sql_4tuples as a subquery. Call the result df_a. It must have five columns named `w1`, `w2`, `w3`, `w4`, and `count`. (`w1`, `w2`, `w3`, `w4`) corresponds to a 4-tuple, and `count` indicates how many times it occurred in the dataset.

`@hint`
You can peek at the answer by running the following command in the shell: df_answer.show()

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

df_correct = spark.sql(sql_4tuple_counts)
df_answer = spark.sql(sql_4tuple_counts)
```

`@sample_code`
```{python}
# Fill in the blanks
query = """
select ____,____,____,____,____(____) as ____
from
(
%s
)
group by ____,____,____,____
order by ____ desc
limit 15
""" % sql_4tuples

df_a = spark.sql(query)
```

`@solution`
```{python}
# Fill in the blanks
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

df_a = spark.sql(query)


```

`@sct`
```{python}
assert type(df_correct)==type(df_a), "Answer is wrong type"
df_a.cache()
assert df_a.columns==df_correct.columns, "Wrong columns"
assert df_a.count()==df_correct.count(), "Wrong number of data"
assert df_a.collect()==df_correct.collect(), "Wrong data"


```
