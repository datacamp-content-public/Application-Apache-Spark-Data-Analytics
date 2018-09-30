---
title: Test
description: Test
---

## Spark Window Functions

```yaml
type: VideoExercise
key: 3e04599626
xp: 50
```

`@projector_key`
f27002a697cdfa6ca618dc6e2f69c53e

---

## Inspect the `df2` table

```yaml
type: NormalExercise
key: 0fc705b773
lang: python
xp: 100
skills: 2
```

There is a table called `df2`, and a variable called `spark` giving an instance of the `SparkSession` object.

`@instructions`
- Create a SQL query that returns the rows of the `df2` table.
- Run this query by running the `sql` command with the `spark` session variable
- Display the result

`@hint`
- The table contains two columns, called `word` and `id`.

`@pre_exercise_code`
```{python}
# Load datasets and packages here.
```

`@sample_code`
```{python}
# Show first 20 rows from table df2.
# Replace QUERY with your sql query.
`spark.sql(" QUERY ").show(20)`

```

`@solution`
```{python}
# Show first 20 rows from table df2
`spark.sql("select * from df2").show(20)`

```

`@sct`
```{python}
# Update this to something more informative.
success_msg("Good - we now have an idea of what is in this table. Now let's run a window function query on it.")
```

---

## Quiz : inspecting the table structure

```yaml
type: MultipleChoiceExercise
key: 7b7f66d7ac
xp: 50
```

What is the structure of the `df2` table? 

`@possible_answers`
1. `word`
2. `word`, `id`
3. `word`: `text`, `id`: `int`
4. `word`: `string`, `id`: `bigint`
5. `string`, `bigint`
6. two columns

`@hint`
The table structure tells us the name and type of each column

`@pre_exercise_code`
```{python}
# 
```

`@sct`
```{python}

```

---

## Sequential challenge round : building our window sql

```yaml
type: TabExercise
key: d01ba44a66
xp: 100
```



`@pre_exercise_code`
```{python}

```

---

## Boss round : find the top 3 tuples.

```yaml
type: NormalExercise
key: 91d83e8800
xp: 100
```

TBD -- this will challenge the users to combine what they have learned previously, which involves regular SQL, with what they just learned, which involves window function sql. 



`@instructions`


`@hint`
# It would be nice if the hint could use a state variable whose value is set in the SCT.

`@pre_exercise_code`
```{python}
# Creates dataset
# Creates and runs a correct answer on the dataset, caching the result.
# Creates aggregates
```

`@sample_code`
```{python}

```

`@solution`
```{python}

```

`@sct`
```{python}
# Compares structure of the student's result with the structure of the correct result.
# Compares aggregates of the student's result with the correct result.
# Exhaustively compares the student's result with the correct result. 

```
