---
title: Insert title here
key: 3556e62bc5ceb1e427a23c4bde60a5c3

---
## Dot notation and SQL

```yaml
type: "TitleSlide"
key: "25e2f4d95a"
```

`@lower_third`

name: Mark Plutowski
title: Algorithmic Econometrician


`@script`
Hello and welcome to this lesson about Spark SQL. In this lesson you will see how you can perform SQL operations using either SQL queries or using standard dataframe dot notation.


---
## Our table has 3 columns

```yaml
type: "FullCodeSlide"
key: "26bdf4d6ce"
```

`@part1`
```
>>> df
DataFrame[train_id: string, station: string, time: string]
```

```
>>> df.columns
['train_id', 'station', 'time']
```


```
>>> df.show(5)
+--------+-------------+-----+
|train_id|      station| time|
+--------+-------------+-----+
|     324|San Francisco|7:59a|
|     324|  22nd Street|8:03a|
|     324|     Millbrae|8:16a|
|     324|    Hillsdale|8:24a|
|     324| Redwood City|8:31a|
+--------+-------------+-----+
```


`@script`
For example, suppose you have a dataframe containing three columns. and you want to select two columns...


---
## We only need 2

```yaml
type: "FullCodeSlide"
key: "2e5dd9344c"
```

`@part1`
```
df.select('train_id','station')
  .show(5)

+--------+-------------+
|train_id|      station|
+--------+-------------+
|     324|San Francisco|
|     324|  22nd Street|
|     324|     Millbrae|
|     324|    Hillsdale|
|     324| Redwood City|
+--------+-------------+
```


`@script`
You could do this.


---
## 3 ways to select 2 columns

```yaml
type: "FullCodeSlide"
key: "f7546cbde8"
```

`@part1`
- df.select('**train_id**','station')
- df.select(**df.train_id**,df.station) {{1}}
- from pyspark.sql.functions import **col** {{2}} 
- df.select(**col**('train_id'), col('station')) {{3}}


`@script`
See that the column ‘train_id’ is a string given in quotes. 

You can also do this {{1}}, using dot notation.  This time the column is given in dot notation, as df.train_id.

You can also import this function {{2}}, which allows you to do this {{3}}.  This time, the name of the column is given as an argument to this new operator called col. Seems may seem more verbose in this case -- however, it is useful in other cases.


---
## 2 ways to rename a column

```yaml
type: "FullCodeSlide"
key: "7a4f94ad95"
```

`@part1`
```
df.select('train_id','station')
  .withColumnRenamed('train_id','train')
  .show(5)
```

```
+-----+-------------+
|train|      station|
+-----+-------------+
|  324|San Francisco|
|  324|  22nd Street|
|  324|     Millbrae|
|  324|    Hillsdale|
|  324| Redwood City|
+-----+-------------+
```

```
df.select(col('train_id').alias('train'), 'station')
```


`@script`
For example, to rename a column you can use the withColumnRenamed function.  But you could also use the col operator, like so {{1}}.  This is often handy.


---
## Don’t do this!

```yaml
type: "FullSlide"
key: "99484fa722"
center_content: true
```

`@part1`
df.select('**train_id**',  **df.**station,  **col**('time'))**


`@script`
Try not to use all three conventions at the same time.


---
## SQL queries using dot notation

```yaml
type: "FullCodeSlide"
key: "605f38f64b"
```

`@part1`
```
spark.sql('select train_id as train, station from df limit 5')
     .show()
```

```
+-----+-------------+
|train|      station|
+-----+-------------+
|  324|San Francisco|
|  324|  22nd Street|
|  324|     Millbrae|
|  324|    Hillsdale|
|  324| Redwood City|
+-----+-------------+
```

```
df.select(col('train_id').alias('train'), 'station')
  .limit(5)
  .show()
```
{{1}}


`@script`
Most Spark sql queries can be done in dot notation or sql notation.  Here’s an example.  Notice that the limit operation is done at query time instead of at show time.  We can do this exact same query using dot notation, like so {{1}}, giving the same result. Note how we used the col operator to select the train_id column, so that we could rename it in place.


---
## Window function SQL

```yaml
type: "FullCodeSlide"
key: "89001687d4"
```

`@part1`
```
query = """
select *, 
row_number() over(partition by train_id order by time) as id 
from df
"""
```

```
spark.sql(query)
     .show(11)
+--------+-------------+-----+---+
|train_id|      station| time| id|
+--------+-------------+-----+---+
|     217|       Gilroy|6:06a|  1|
|     217|   San Martin|6:15a|  2|
|     217|  Morgan Hill|6:21a|  3|
|     217| Blossom Hill|6:36a|  4|
|     217|      Capitol|6:42a|  5|
|     217|       Tamien|6:50a|  6|
|     217|     San Jose|6:59a|  7|
|     324|San Francisco|7:59a|  1|
|     324|  22nd Street|8:03a|  2|
|     324|     Millbrae|8:16a|  3|
|     324|    Hillsdale|8:24a|  4|
+--------+-------------+-----+---+
```


`@script`
Same goes for window functions.   This query adds a number to each stop on a train line in a new column called id. Note how the id column starts over for train_id 324.


---
## Window function using dot notation

```yaml
type: "FullSlide"
key: "7aec9a66f7"
```

`@part1`
```
from pyspark.sql import Window, 
from pyspark.sql.functions import row_number
df.withColumn("id", row_number()
                    .over(
                           Window.partitionBy('train_id')
                                 .orderBy('time')
                         )
  )
  .show()
```


`@script`



---
## Final Slide

```yaml
type: "FinalSlide"
key: "57db0d72de"
```

`@script`


