---
title: 'Insert title here'
key: f27002a697cdfa6ca618dc6e2f69c53e
video_link:
    hls: 'https://github.com/pluteski/datacamp/blob/master/video/DC_Sample.m3u8'
    mp4: 'https://github.com/pluteski/datacamp/blob/master/video/DC_Sample.mp4'
---

## Apache Spark Window Function SQL

```yaml
type: TitleSlide
key: 3afb3b80fe
```

`@lower_third`
name: Mark Plutowski
title: undefined

`@script`


---

## Inspect the table schema.

```yaml
type: FullCodeSlide
key: aa4094ad81
```

`@part1`
# Here are two ways you can inspect the structure of a Spark table named `df2` : 

- `spark.sql("describe df2 ").show()`

- `print(spark.sql("select

 * from df2 limit 0"))`

`@script`
There exists a table called `df2`. A `SparkSession` is also already available via the `spark` variable.  

Here are two ways to examine the schema of the `df2` table.  Can you think of any other ways?

---

## Inspect the table data

```yaml
type: TwoRowsTwoColumns
key: 89f9c88279
```

`@part1`
Following is a way to inspect the top 20 rows of a table called `df2`.

This approaches the following:

1. Runs a SQL query on the table that fetches all of its rows, returning it as a dataframe.
2. Selects the top 20 rows of the dataframe using dataframe dot notation.
3. Displays the result of step 2 to the console.

`@part2`
`spark.sql("select * from df2").limit(20).show()`

`@part3`
The following achieves the same result, having the limit operation performed within the SQL query.

`@part4`
`spark.sql("select * from df2 limit 20").show()`

`@script`
Here are two ways to inspect the data in a table called `df2`.  The first way uses the dataframe dot notation to perform part of the query, and the second way performs the entire query in SQL.

---

## 2-tuple query

```yaml
type: FullCodeSlide
key: 619d020a03
```

`@part1`
sql2 = """
    select
    id,
    word as w1,
    lead(word,1) over(order by id ) as w2
    from df2
"""
spark.sql(sql2)\
     .show()

`@script`
This code creates a variable of type `str` containing a window function SQL query that gives the word sequences of length two from the `df2` table. Each row of the result contains the word of a corresponding row in the `df2` in a column called `w1`, and the word from the following row in a column called `w2`. It gets the value of the `w2` field using a window clause using the `lead` operation, and an `over` clause ordered by the `id` field.

---

## Final Slide

```yaml
type: FinalSlide
key: 4c422c3019
```

`@script`
Now let’s try out what we just learned.
