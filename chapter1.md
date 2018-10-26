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
