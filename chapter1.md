---
title: 'Chapter 1: Pyspark SQL'
description: ""
---

## Creating and querying a SQL table in Spark

```yaml
type: VideoExercise
key: e9da550220
xp: 50
```

`@projector_key`
50a513bf7e15bc7ea8559ce3382bd96c

---

## Load dataframe with csv (comma separated value) data

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

## Create a SQL table from a dataframe.

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

## Determine the column names of a table.

```yaml
type: NormalExercise
key: 47be04c489
xp: 100
```

If a table exists you can inspect its schema in a few ways. There are several ways to determine the columns of this table using an sql query. 

Suppose there exists a table named 'table'. 
If all you need is to _see_ the names of its columns, you can do the following: 

`spark.sql("show columns from table").show()`

Another:

`spark.sql("select * from table limit 0").show()`

If you want to fetch the names of the columns in a variable that you can work with programmatically. 

`spark.sql("show columns from table").collect()`

Note that the result of a query is a dataframe, so you can inspect its columns like so:

```columns = spark.sql("select * from table limit 0").columns```

`spark.sql("show columns from table").collect()`


One way is to run a "select *" query, store the results in a dataframe, and then inspect the columns of the dataframe. 


`@instructions`
A table called 'df' exists.  Store a list in a variable called 'columns' that is a list of strings, 
giving the names of the columns in the table 'df', sorted in ascending order.
Print the value of the columns variable.

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

## Run an aggregate query on a table.

```yaml
type: NormalExercise
key: 938a563d45
xp: 100
```

There is a table called 'df' having three columns : train_id, station, time.

`@instructions`
Using an sql query, find the station occurring in more than one row.
Set the value of the variable 'station' to the corresponding station value.

`@hint`
Try an aggregate -- grouping on station column, and finding the station having more than one

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
