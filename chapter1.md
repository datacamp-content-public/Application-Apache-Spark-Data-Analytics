---
title: 'Chapter 1: Pyspark SQL'
description: ""
---

## Lesson 1.1 : Creating and querying a SQL table in Spark

```yaml
type: VideoExercise
key: e9da550220
xp: 50
```

`@projector_key`
50a513bf7e15bc7ea8559ce3382bd96c

---

## Exercise: Load dataframe with csv data

```yaml
type: NormalExercise
key: 782514209a
xp: 100
```

Spark has a command that reads delimited text data into a dataframe from a file. 
One of its options is to have it use the first row to define the names of the columns. 
It automatically splits each row into columns using the delimiter, which by default is 
a comma "," but which can be changed. It is called using an instance of a SparkSession object.
Some implementations of Spark, such as Pyspark Shell, and some Spark Notebook, automatically provide 
an instance of a SparkSession, which by convention is stored in a variable named 'spark'.

`@instructions`
Load csv data from the file "trainsched.txt" into a dataframe stored in a variable named 'df'.

`@hint`
A synonym for "load" is "read".  Don't forget to specify the format.

`@pre_exercise_code`
```{python}
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
```

`@sample_code`
```{python}
df=spark.____.___("trainsched.txt",header=True)
```

`@solution`
```{python}
df = spark.read.csv("trainsched.txt",header=True)
```

`@sct`
```{python}
assert len(df.columns)==3, "Wrong number of columns -- did you use the right separator?"
assert df.columns == ['train_id', 'station', 'time'], "Incorrect column names"
assert df.count()==14, "The number of rows is incorrect"
assert df.select("train_id").distinct().count()==2, "There should be two train_id's."
assert sorted([x["train_id"] for x in df.select("train_id").distinct().collect()])[0]=='217',"Missing train 217"
assert sorted([x["train_id"] for x in df.select("train_id").distinct().collect()])[1]=='324',"Missing train 324"

```

---

## Exercise: create a SQL table from a dataframe.

```yaml
type: NormalExercise
key: f1a04a3154
xp: 100
```

A dataframe can be used to create a **temporary table**. 
A _temporary_ table is one that will not exist after the session ends. 
Spark documentation also refers to this type of table as a _SQL temporary view_. 
In the documentation this is referred to as to _register the dataFrame as a SQL temporary view_.
This command is called on the dataframe itself, and creates a table if it does not already exist,
replacing it with the current data from the dataframe if it does already exist.

`@instructions`
A variable called '_df_' contains a dataframe.
Create a temporary table from _df_. 
Call the table 'table'.

`@hint`
Use the command _createOrReplaceTempView_.  Remember to provide it with the table name.

`@pre_exercise_code`
```{python}
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df=spark.read.csv("lesson1.txt",header=True)
```

`@sample_code`
```{python}
df.____________________(_____)
```

`@solution`
```{python}
df.createOrReplaceTempView("table")
```

`@sct`
```{python}
assert len([x for x in spark.catalog.listTables() if x.name=='table'])==1,"Table does not exist"
assert [x for x in spark.catalog.listTables() if x.name=='table'][0].isTemporary,"Expected table to be temporary, but it is not."
assert [x for x in spark.catalog.listTables() if x.name=='table'][0].tableType=='TEMPORARY',"Expected table type to be TEMPORARY, but it is not."

```

---

## Exercise: determine the column names of a table.

```yaml
type: NormalExercise
key: 47be04c489
xp: 100
```

Spark commonly provides several ways to achieve a result. 
If a table exists you can inspect its schema in several ways. 
There are several ways to determine the columns of this table using an sql query. 

Suppose there exists a table named 'table' having two columns, 'column1' and 'column2', each column containing string values. 
If all you need is to _see_ the names of its columns, you can do the following: 

```
spark.sql("show columns from table").show()
+--------+
|col_name|
+--------+
| column1|
| column2|
+--------+
```

Another:

```
spark.sql("select * from table limit 0").show()
+-------+-------+
|column1|column2|
+-------+-------+
+-------+-------+
```

Suppose you don't want to just _visually_ inspect the column names, but want to put the names of the columns into a variable that you can work with _programmatically_ :

```
>>> columns = spark.sql("show columns from table").collect()
>>> print(columns)
[Row(col_name='column1'), Row(col_name='column2')]
```

The columns variable contains a list of _Row_ objects, from which 
you can get a list of column names like so:

```
>>> [x.col_name for x in columns]
['column1', 'columns']
```


The result returned by a query on a table is a dataframe, so you can inspect its columns like so:

```
>>> spark.sql("select * from table limit 0").columns
['column1','column2']
```

Suppose you want to see the names of each column and the type of each column. 


```
>>> spark.sql("describe table").show()
+--------+---------+-------+
|col_name|data_type|comment|
+--------+---------+-------+
| column1|   string|   null|
| column2|   string|   null|
+--------+---------+-------+
```

You can also do:

```
>>> spark.sql("select * from table limit 0")
DataFrame[train_id: string, station: string, time: string]
```

or, if you are not in a shell, print the result, like so:

```
>>> print(spark.sql("select * from table limit 0"))
DataFrame[train_id: string, station: string, time: string]
```

`@instructions`
A table called 'df' exists.  Create a variable called 'columns' that contains a list of strings giving the names of the columns in the table 'df', sorted in ascending order. Print the value of the columns variable.

`@hint`
There should be three columns.

`@pre_exercise_code`
```{python}
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df=spark.read.csv("lesson1.txt",header=True)
df.createOrReplaceTempView("df")
```

`@sample_code`
```{python}
columns = ____________
print(columns)
```

`@solution`
```{python}
['station', 'time', 'train_id']
```

`@sct`
```{python}
assert type(columns) is list, "Expected type(columns) to be a list"
assert len(columns)==3, "There should be three columns"
assert 'station' in columns and 'time' in columns and 'train_id' in columns, "A column is missing from your list"

```

---

## Exercise: run an aggregate query on a table.

```yaml
type: NormalExercise
key: 938a563d45
xp: 100
```

SQL evolved over many years, and many versions emerged. 
This meant that a query you ran on one system might not run the same on a different system. 
The American National Standards Institute (ANSI) created specific standards for SQL to mitigate this problem.

Spark SQL is a ANSI compliant SQL that allows you to run the types of SQL queries that you may have learned elsewhere, 
including the full range of sql statements, such as the HAVING clause, which can be a bit trickier to implement using dataframe dot notation.

`@instructions`
There is a table called 'df' with three columns : train_id, station, time.
Using an sql query, find the station occurring in more than one row.
Set the value of the variable 'station' to the corresponding station value.

`@hint`
Try an aggregate -- grouping on station column, and finding the station having more than one row using the HAVING clause.

`@pre_exercise_code`
```{python}
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df=spark.read.csv("lesson1.txt",header=True)
df.createOrReplaceTempView("df")

```

`@sample_code`
```{python}
result = spark.sql("select station from df _____________ _____________ ")
station = result.collect()[0].station
```

`@solution`
```{python}
result = spark.sql("select station from df group by station having count(*) > 1")
station = result.collect()[0].station

```

`@sct`
```{python}
assert station=='San Jose', "Wrong value for station"

```

---

## Lesson 1.2 : Window function SQL

```yaml
type: VideoExercise
key: 240f6a6a10
xp: 50
```

`@projector_key`
457f3f91ff790f8250c2acdbc3a84559

---

## Exercise: Calculating the difference between values in adjacent rows.

```yaml
type: TabExercise
key: c94919f4fa
xp: 100
```

In our dataset the 'time' column is easy to read, but in a nonstandard format that is not the best for performing operations such as subtraction.  Fortunately Spark has the means to convert this data into a format it can more easily manipulate. 

The **to_timestamp** function takes two arguments: 

1. the name of the column, in our example, _‘time’_ 
2. a format string telling it how to extract the hours and minutes from the time column.

"**Unix time**" is the number of seconds (minus leap seconds) that have elapsed since 00:00:00 Coordinated Universal Time (UTC), which corresponds to Thursday, 1 January 1970. 

The "unix_timestamp" function converts the time data into unix time. This is given in seconds. It is called in a manner similar to the to_timestamp function, with the first argument giving the column name, and the second argument giving the format string.

Once we have the time column converted to seconds, it will be straightforward to calculate the difference between two different timestamps.

`@pre_exercise_code`
```{python}
df=spark.read.csv("lesson1.txt",header=True)
df.createOrReplaceTempView("sched")
```

***

```yaml
type: NormalExercise
key: ac466797a8
xp: 25
```

`@instructions`
Let's make sure that we can extract the time properly from the time column. 
Apply the to_timestamp function to the time field to convert the time into a proper timestamp. 
Call the new column 'ts'.

`@hint`
Fill the blank with the first function mentioned in the previous slide.

`@sample_code`
```{python}
query="""
select train_id,station,time,_____________(time, 'H:m') as ts 
from sched
"""
spark.sql(query).show()
```

`@solution`
```{python}
query="""
select train_id,station,time,to_timestamp(time,'H:m') as ts 
from sched
"""
spark.sql(query).show()
```

`@sct`
```{python}

```

***

```yaml
type: NormalExercise
key: 8eed5099a2
xp: 25
```

`@instructions`
The previous step confirmed that we can properly extract a timestamp from the time column. 
Let's convert the time column into a unix timestamp. We won't worry about the time zone, 
because the train lines in this dataset are all within the same time zone.

`@hint`
Use the unix_timestamp function, and the same format string used by the to_timestamp function.

`@sample_code`
```{python}
query="""
select train_id,station,time,to_timestamp(time,'H:m') as ts, ______________(time, ______) 
from sched
"""
spark.sql(query).show()
```

`@solution`
```{python}
query="""
select train_id,station,time,to_timestamp(time,'H:m') as ts, unix_timestamp(time,'H:m') as unixtime
from sched
"""
spark.sql(query).show()
```

`@sct`
```{python}

```

***

```yaml
type: NormalExercise
key: ae40725d3e
xp: 25
```

`@instructions`
In the lesson intro we learned how to get the value of the next row,
by using the _'lead'_ function over a window partitioned by train_id. 
Replace the blank with the expression that gives the value of the 'time' column for the next row.

`@hint`
Replace the blank with the same expression used for _'time_next'_ demonstrated in the intro.

`@sample_code`
```{python}
query="""
select train_id, station, time, 
unix_timestamp(time,'H:m') as unixtime1,
unix_timestamp(____________________________,'H:m') as unixtime2
from sched
"""
spark.sql(query).show()

```

`@solution`
```{python}
query="""
select train_id, station, time, 
unix_timestamp(time,'H:m') as unixtime1,
unix_timestamp(lead(time,1) over (partition by train_id order by time),'H:m') as unixtime2
from sched
"""
spark.sql(query).show()

```

`@sct`
```{python}

```

***

```yaml
type: NormalExercise
key: dba755cba7
xp: 25
```

`@instructions`
Combine what we've done thus far -- subtract the unix time of the next row from the unix time of the current row,
and divide it by 60 to convert it from seconds into minutes.

`@hint`
Replace the first blank with the unix time for the next row obtained over the window function we saw previously.

`@sample_code`
```{python}
query="""
select 
train_id,
station,
time,
(_______________________  - ______________________)/60 as diff_min
from sched
"""
spark.sql(query).show()

```

`@solution`
```{python}
query="""
select 
train_id,
station,
time,
(unix_timestamp(lead(time,1) over (partition by train_id order by time),'H:m') - unix_timestamp(time,'H:m'))/60 as diff_min
from sched
"""
spark.sql(query).show()

```

`@sct`
```{python}

```

---

## Exercise: Running sums using window function SQL

```yaml
type: NormalExercise
key: 20b6379019
xp: 100
```

A window function performs a calculation across a set of table rows that are related to the current row. 
This is analogous to the calculation done by an aggregate function.  However, whereas a regular aggregate function causes rows to become grouped into a single output row, a window function gives an output for every row. 

It turns out that you can use aggregation functions along with window functions. For example, you can easily do a running sum using a window function ("over clause"), using a sql query that is much simpler than what is required using joins. The query duration can also be much faster.

`@instructions`
There is a table called 'schedule', having columns 
* train_id
* station, 
* time, and 
* diff_min

The 'diff_min' column gives the elapsed time between the current station and the next station on the line.

Run a query that adds an additional column to the records in this dataset called running_total.
The column running__total sums the difference between station time given by the diff_min column.

`@hint`
The first blank is an aggregation function over the diff_min field.  We want the running sum to tally within each train line, so the window used here is the same as what was used in previous exercises.

`@pre_exercise_code`
```{python}
df=spark.read.csv("lesson1.txt",header=True)
df.createOrReplaceTempView("sched")
query="""select train_id,station, time,
(unix_timestamp(lead(time,1) over (partition by train_id order by time),'H:m') - unix_timestamp(time,'H:m'))/60 as diff_min
from sched
"""
schedule = spark.sql(query)
schedule.createOrReplaceTempView("schedule")

```

`@sample_code`
```{python}
query2="""
SELECT train_id, station, time, diff_min,
___(__________) over (________________________) AS running_total
from schedule
"""
spark.sql(query2).show()
```

`@solution`
```{python}
query2="""
SELECT train_id, station, time, diff_min,
sum(diff_min) over (partition by train_id order by time) AS running_total
from schedule
"""
spark.sql(query2).show()

```

`@sct`
```{python}

```

---

## Lesson 1.3: Dot notation and SQL

```yaml
type: VideoExercise
key: da6ad01721
xp: 50
```

`@projector_key`
3556e62bc5ceb1e427a23c4bde60a5c3

---

## Iterations on Aggregation

```yaml
type: NormalExercise
key: cfd2ee5ba4
xp: 100
```

The following line of code uses sql to set the value of a dataframe called df1.
df1 = spark.sql("select *, lead(time,1) over(partition by train_id order by time) as time_next from df")

Here are the first 8 rows of df1:

```
+--------+-------------+-----+---------+
|train_id|      station| time|time_next|
+--------+-------------+-----+---------+
|     217|       Gilroy|6:06a|    6:15a|
|     217|   San Martin|6:15a|    6:21a|
|     217|  Morgan Hill|6:21a|    6:36a|
|     217| Blossom Hill|6:36a|    6:42a|
|     217|      Capitol|6:42a|    6:50a|
|     217|       Tamien|6:50a|    6:59a|
|     217|     San Jose|6:59a|     null|
|     324|San Francisco|7:59a|    8:03a|
+--------+-------------+-----+---------+
```


`@instructions`
Create a dataframe called df2 that contains the identical result as df1, using dot notation instead of sql.

`@hint`
The LEAD clause has an equivalent function in pyspark.sql.functions. The PARTITION BY, and ORDER BY clauses each have an equivalent dot notation function that is called on the Window object.

`@pre_exercise_code`
```{python}
from pyspark.sql import Window 
from pyspark.sql.functions import lead  
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df=spark.read.csv("trainsched.txt",header=True)
df.createOrReplaceTempView("df")

```

`@sample_code`
```{python}
df2 = df.withColumn('time_next', ____('time',1).over(Window.____('train_id').____('time')))
```

`@solution`
```{python}
df2 = df.withColumn('time_next', lead('time',1).over(Window.partitionBy('train_id').orderBy('time')))
```

`@sct`
```{python}

```

---

## Aggregate Dot SQL

```yaml
type: BulletExercise
key: 1cf40a2c86
xp: 100
```

Whether to use dot notation or SQL is a personal preference. However, there are cases where dot notation can be more cumbersome. The following exercises calculate the departure time for the first train for each train line. 

`@pre_exercise_code`
```{python}
from pyspark.sql import Window 
from pyspark.sql.functions import lead  
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df=spark.read.csv("trainsched.txt",header=True)
df.createOrReplaceTempView("df")

```

***

```yaml
type: NormalExercise
key: e9cea6300a
xp: 25
```

`@instructions`
Fill in the blanks to get the second command to display the identical result to the first command.

`@hint`
groupBy('train_id').  The agg operator wants the know what aggregation operator ('min') to apply, and to which column ('time').

`@sample_code`
```{python}
spark.sql('select train_id, min(time) from df group by train_id').show()

df.groupBy('____').agg({'____':'____'}).show()

```

`@solution`
```{python}
spark.sql('select train_id, min(time) from df group by train_id').show()

df.groupBy('train_id').agg({'time':'min'}).show()

```

`@sct`
```{python}

```

***

```yaml
type: NormalExercise
key: 2706f1ee63
xp: 25
```

`@instructions`
Fill in the blanks to obtain an identical result, this time renaming the aggregate column.

`@hint`
The second blank is the desired column name.  The first blank is the column name that was assigned by default to the aggregation column.  If you forgot, try doing the following in the shell: df.groupBy('train_id').agg({'time':'min'}).show()

`@sample_code`
```{python}
# Fill in the blanks to achieve the identical result for both commands.

spark.sql('select train_id, min(time) as start from df group by train_id').show()

df.groupBy('train_id').agg({'time':'min'}).withColumnRenamed('____','____').show()

```

`@solution`
```{python}
# Fill in the blanks to achieve the identical result for both commands.

spark.sql('select train_id, min(time) as start from df group by train_id').show()

df.groupBy('train_id').agg({'time':'min'}).withColumnRenamed('min(time)','start').show()

```

`@sct`
```{python}

```

***

```yaml
type: NormalExercise
key: 4404476665
xp: 25
```

`@instructions`
Aggregating twice on the same column this way doesn’t work as expected. What is the 2nd column name?

`@hint`
Try the following in the shell: df.groupBy('train_id').agg({'time':'min','time':'max'}).columns

`@sample_code`
```{python}
spark.sql('select train_id, min(time), max(time) from df group by train_id').show()

# The following result is different from the one shown above. 
# Fill in the blank with the value of col2_name. 
col2 = df.groupBy('train_id').agg({'time':'min','time':'max'}).columns[1]
my_answer = '____'
assert my_answer==col2
```

`@solution`
```{python}
spark.sql('select train_id, min(time), max(time) from df group by train_id').show()

# The following result is different from the one shown above. 
# Fill in the blank with the value of col2_name. 
col2 = df.groupBy('train_id').agg({'time':'min','time':'max'}).columns[1]
my_answer = 'max(time)'
assert my_answer==col2
```

`@sct`
```{python}

```

***

```yaml
type: NormalExercise
key: 78b05cc617
xp: 25
```

`@instructions`
Write a sql query that gives an identical result to the dot notation query.

`@hint`
select train_id, min(____) as ____, min(____) as ____ from df group by train_id

`@sample_code`
```{python}
# This is the dot notation query
from pyspark.sql.functions import min, max, col
expr = [min(col("time")).alias('start'),max(col("time")).alias('end')]
result1 = df.groupBy("train_id").agg(*expr)

# Write a sql query giving result2 identical to result1
query = "____"
result2 = spark.sql(query)
assert result1.columns==result2.columns
assert result1.collect()==result2.collect()

```

`@solution`
```{python}
# This is the dot notation query
from pyspark.sql.functions import min, max, col
expr = [min(col("time")).alias('start'),max(col("time")).alias('end')]
result1 = df.groupBy("train_id").agg(*expr)

# Write a sql query giving result2 identical to result1
query = "select train_id, min(time) as start, max(time) as end from df group by train_id"
result2 = spark.sql(query)
assert result1.columns==result2.columns
assert result1.collect()==result2.collect()

```

`@sct`
```{python}

```

---

## Capstone: Convert Window function query from dot notation to SQL

```yaml
type: NormalExercise
key: bd150714e6
xp: 100
```

In Spark SQL, the `over` function corresponds to a OVER clause in SQL.  The class `pyspark.sql.window.Window` represents the inside of an OVER clause. A `WindowSpec` can be defined using the `Window` class, and then used subsequently as an argument to the `over` function in a window function query, like so:

window = Window.partitionBy('train_id').orderBy('time')
dfx = df.withColumn('next', lead('time',1).over(window))



`@instructions`
We have a dataframe df such that df.columns == ['train_id', 'station', 'time'].  The dataframe df is registered as a SQL table by the name 'df'. 

A window function query is provided in dot notation. Its result is contained in the variable df1. Create an equivalent SQL query. This query will be used to create a second dataframe called df2. The two dataframes df1 and df2 should contain identical data and metadata.

`@hint`
The remaining blanks correspond to Window function clauses covered in the lesson and map directly onto sql functions appearing in the dot notation query. 
  
query = "select *, (unix_timestamp(____(time,1) over (____ by train_id ____ by time),'H:m') - unix_timestamp(time,'H:m'))/60 as diff_min from df"

`@pre_exercise_code`
```{python}
from pyspark.sql import Window 
from pyspark.sql.functions import lead  
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df=spark.read.csv("trainsched.txt",header=True)
df.createOrReplaceTempView("df")

```

`@sample_code`
```{python}
# Here is a query using dot notation. 
window = Window.partitionBy('train_id').orderBy('time')
df1 = df.withColumn('diff_min', 
                    (unix_timestamp(lead('time',1).over(window),'H:m') 
                     - unix_timestamp('time','H:m'))/60)

# Fill in the blanks of this sql query to obtain an identical result to the above. 
query = """
select 
*, 
(____(____(time,1) ____ (____ by train_id ____ by time),'H:m') 
 - ____(time,'H:m'))/60 as diff_min 
from df 
"""

# df2 should contain data that is identical to df1
df2 = spark.sql(query)

assert df1.columns==df2.columns
assert df1.first()==df2.first()
assert df1.collect()==df2.collect()

```

`@solution`
```{python}
# Here is a query using dot notation. 
df1 = df.withColumn('diff_min', 
                    (unix_timestamp(lead('time',1).over(Window.partitionBy('train_id').orderBy('time')),'H:m') 
                     - unix_timestamp('time','H:m'))/60)

# Fill in the blanks of this sql query to obtain an identical result to the above. 
query = """
select 
*, 
(unix_timestamp(lead(time,1) over (partition by train_id order by time),'H:m') 
 - unix_timestamp(time,'H:m'))/60 as diff_min 
from df 
"""

# df2 should contain data that is identical to df1
df2 = spark.sql(query)

assert df1.columns==df2.columns
assert df1.first()==df2.first()
assert df1.collect()==df2.collect()

```

`@sct`
```{python}

```
