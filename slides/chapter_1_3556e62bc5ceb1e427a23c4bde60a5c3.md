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
## Final Slide

```yaml
type: "FinalSlide"
key: "57db0d72de"
```

`@script`


