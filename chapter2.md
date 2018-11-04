---
title: 'Using window function sql for natural language processing'
description: ""
---

## (Capstone) Most common word sequences

```yaml
type: NormalExercise
key: a70a4bc009
xp: 100
```

Previously we saw how to create a query that finds word sequences of length three ("3-tuples").
We used that query as a subquery in a traditional sql query 
to find the most common 3-tuples in the text document. 
Now you'll perform the similar task to find the most common 5-tuples. 

Dataframe `df` has columns: `word`, `id`, `part`, `title`. The `id` column is a integer such that a word that comes later in the document has a larger id than a word that comes before it. The `part` column separates the data into chapters. The dataframe `df` is also registered as temporary table called `df`.


`@instructions`
Create a query that finds the **10** most common 5-tuples in the dataset. 
Have it use sql_4tuples as a subquery. Call the result df_a. It must have five columns named `w1`, `w2`, `w3`, `w4`, and `count`. (`w1`, `w2`, `w3`, `w4`) corresponds to a 4-tuple, and `count` indicates how many times it occurred in the dataset.

`@hint`
You can peek at the answer by running the following command in the shell: df_answer.show()

`@pre_exercise_code`
```{python}
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sqlContext = SQLContext.getOrCreate(spark.sparkContext)
df = sqlContext.read.load('sherlock_parts.parquet')
df.createOrReplaceTempView('df')
spark.catalog.cacheTable('df')
#   Tallies most frequent 5-tuples
sql_top_5tuples = """
select w1,w2,w3,w4,w5,count(*) as count
from
(
   select
   word as w1,
   lead(word,1) over(partition by part order by id ) as w2,
   lead(word,2) over(partition by part order by id ) as w3,
   lead(word,3) over(partition by part order by id ) as w4,
   lead(word,4) over(partition by part order by id ) as w5
   from df
)
group by w1,w2,w3,w4,w5
order by count desc
limit 10
"""


df_correct = spark.sql(sql_top_5tuples)
df_answer = spark.sql(sql_top_5tuples)

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
select w1,w2,w3,w4,w5,count(*) as count
from
(
   select
   word as w1,
   lead(word,1) over(partition by part order by id ) as w2,
   lead(word,2) over(partition by part order by id ) as w3,
   lead(word,3) over(partition by part order by id ) as w4,
   lead(word,4) over(partition by part order by id ) as w5
   from df
)
group by w1,w2,w3,w4,w5
order by count desc
limit 10
""" 


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
