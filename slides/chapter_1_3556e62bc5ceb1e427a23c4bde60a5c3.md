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
## Insert title here...

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
## Final Slide

```yaml
type: "FinalSlide"
key: "57db0d72de"
```

`@script`


