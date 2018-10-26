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



`@instructions`
Load csv data from the file "trainsched.txt" into a dataframe stored in a variable named 'df'.

`@hint`
A synonym for "load" is "read".  Don't forget to specify the format.

`@pre_exercise_code`
```{python}

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

A dataframe can be used to create a temporary table. 
A temporary table is one that will not exist after the session ends. 


`@instructions`
A variable called 'df' contains a dataframe.
Create a temporary table from dataframe df. 
Call the table 'table'.

`@hint`
In the documentation this is referred to as to "Register the DataFrame as a SQL temporary view".

`@pre_exercise_code`
```{python}
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

A table called 'df' exists. There is more than one way to determine the columns of this table.

`@instructions`
Store a list in a variable called 'columns' that is a list of strings, 
giving the names of the columns in the table 'df', sorted in ascending order.
Print the value of the columns variable.

`@hint`
There should be three columns.

`@pre_exercise_code`
```{python}
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

## Insert exercise title here

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
