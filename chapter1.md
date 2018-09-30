---
title: Test
description: Test
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

## Spark Window Functions

```yaml
type: VideoExercise
key: 3e04599626
xp: 50
```

`@projector_key`
f27002a697cdfa6ca618dc6e2f69c53e

---

## Insert exercise title here

```yaml
type: MultipleChoiceExercise
key: 7b7f66d7ac
xp: 50
```

xxx

`@possible_answers`
1. `word`
2. `word`, `id`
3. `word`: `text`, `id`: `int`
4. `word`: `string`, `id`: `bigint`
5. `string`, `bigint`

`@hint`
The schema

`@pre_exercise_code`
```{python}

```

`@sct`
```{python}

```
