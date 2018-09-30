---
title: Insert title here
key: f27002a697cdfa6ca618dc6e2f69c53e
video_link:
  hls: https://github.com/pluteski/datacamp/blob/master/video/DC_Sample.m3u8
  mp4: https://github.com/pluteski/datacamp/blob/master/video/DC_Sample.mp4

---
## Apache Spark Window Function SQL

```yaml
type: "TitleSlide"
key: "3afb3b80fe"
```

`@lower_third`

name: Mark Plutowski
title: undefined


`@script`
In the previous lessons you’ve learned how to convert Spark dataframes into an SQL table and run SQL queries on such a table. In this lesson, we’ll learn how to use a SQL window function to perform a sequence analysis.

What is sequence analysis and how is it useful?  Sequence analysis can be used on any data that is ordered, such as time series data. It can be used to do trend analysis, and anomaly analysis.  It can also be used to look at sequences of words in a text document. 

What is Window Function SQL and how is it useful?  SQL window functions perform certain very useful operations more easily than regular SQL queries or using dataframe operations.  When processing some rows, each row can use the values of other rows in calculating its value. 

For example, suppose you have a table containing a train schedule for a train line.   You could use a window function to calculate the time until the next stop and add that as a new column. 

Window functions operate on a set of rows and return a value for each row in the set. The term window describes the set of rows on which the function operates.  The value returned for each row can be a value from one of the rows in the “window”, or, a value from a “window function” that uses values from the rows in the window to calculate its value. 

Let’s look at our train schedule example again.  A window function sql query looking at the current row and the next row adds a column giving the time of the following row.  Now that each row can easily reference the time of the next row, it can now easily calculate a new value given by subtracting the time for the next row from the time of the current row. 

Doesn’t that sound like fun?  

Actually, instead of train schedules, we’re going to do something else that may hopefully be more interesting.

We are going to look at sequences of words in a text document. Extracting sequences of word from a document is a powerful technique that can be used for training a model to predict the next word in a sequence. It turns out that this can be used not only for natural language processing based on documents of words, but also for lists of tokens, where each token is an id representing a song, say, or a video -- and so can be used for recommending videos and songs from a user’s recent usage.  But for now, we are going to begin with an analytics problem based on a text document. 

We will find the most common sequences of words in a document.  Here is a set of 5-tuples, that is, word sequences of length 5, gleaned from a document having a million words using Spark SQL.  Let’s work up to this, starting with 3-tuples.  

Suppose we have a table, called df2, that looks like this. Note that it currently only has 14 rows. In the dataset we’ll use it actually has over a million words, but limiting it to 14 rows allows me to demonstrate a boundary case.  

Notice that each word in the document is on its own row, and each row has a row id. Now, imagine that you could pass a “window” of length 3 across this document, moving it one step, tallying the 3 words that you see, then moving it over by one word, tallying the 3 words that you can see, and so on, until the end of the document.  The result would look like this.  

Note how the second to last row has one empty column, and the last row has two empty columns?  That is because the window ran off the end of the document. The null values are how it deals with that boundary case.

What kind of query can we use to achieve this?  You guessed it -- a window function query. Here is the query I used.

This looks quite similar to a regular sql query -- except for the two lines containing what are sometimes called an “over” clause. I’m going to break this down, but first let’s go to the command line and try this out.


---
## Inspect the table schema.

```yaml
type: "FullCodeSlide"
key: "aa4094ad81"
```

`@part1`
`print(spark.sql("describe df2 "))`

`spark.sql("describe df2").show()

`print(spark.sql("select * from df2 limit 0"))`


`@script`
There exists a table called `df2`. A `SparkSession` is also already available via the `spark` variable.  

Here are three ways to examine the schema of the `df2` table.


---
## Inspect the table data

```yaml
type: "TwoRowsTwoColumns"
key: "89f9c88279"
```

`@part1`
Following is a way to inspect the top 20 rows of a table called `df2`.

This way runs a SQL query on this table that fetches all of the rows from the `df2` table, shows the result using the `show` operation, limiting the output to 20 rows by using dataframe dot notation.

Following is a way to fetch the first 20 rows by using a limit operation in the SQL query, then showing the result using the `show` operation.


`@part2`
`spark.sql("select * from df2").limit(20).show()`


`@part3`
The following achieves the same result, having the limit operation performed within the SQL query.


`@part4`
`spark.sql("select * from df2 limit 20").show()`


`@script`
Here are two ways to inspect the data in a table called `df2`.  The first way uses the dataframe dot notation to perform part of the query, and the second way performs the entire query in SQL.


---
## Quiz (test)

```yaml
type: "TwoRows"
key: "f3164800d5"
```

`@part1`
What is the schema of the `df2` table?


`@part2`
1. `word`
2. `word`, `id`
3. `word`: `text`, `id`: `int`
4. `word`: `string`, `id`: `bigint`
5. `string`, `bigint`


`@script`
< Instructor's note : Hmmm... I don't think I understand how to create a quiz slide. I don't think this is the way.>


---
## 2-tuple query

```yaml
type: "FullCodeSlide"
key: "619d020a03"
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
Create a variable of type `str` containing a window function SQL query that gives the word sequences of length two from the `df2` table. Each row of the result should contain the word of a corresponding row in the `df2` in a column called `w1`, and the word from the following row in a column called `w2`. Get the value of the `w2` field using the `lead` operation, and an `over` clause ordered by the `id` field.

Run this query using the SparkSession object `spark` and display its result.


---
## Final Slide

```yaml
type: "FinalSlide"
key: "4c422c3019"
```

`@script`
Let’s go to the command line and try this out.

