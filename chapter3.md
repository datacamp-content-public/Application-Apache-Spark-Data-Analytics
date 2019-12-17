---
title: 'Training a logistic regression model'
description: "What is classification? What is logistic regression? Why is it used for classification? What are hyperparameters?  Using a training summary object. Evaluating a fitted model on a test dataset. How to calculate the estimated prediction accuracy of the fitted model. \n"
---

## Training a machine learning model

```yaml
type: VideoExercise
key: ed29fab787
xp: 50
```

`@projector_key`
d463ed6481449bd40e704f26ab34014e

---

## Practice machine learning model training concepts

```yaml
type: DragAndDropExercise
key: e531611f61
kind: Classify
xp: 100
```

<!-- Guidelines for contexts: https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->

`@instructions`
<!-- Guidelines for instructions https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->
- Instruction 1
- Instruction 2

`@hint`
<!-- Examples of good hints: https://instructor-support.datacamp.com/en/articles/2379164-hints-best-practices. -->
- This is an example hint.
- This is an example hint.

`@solution`
```{python}
# Edit or remove this code to create your own exercise.
# This is 1 type of drag and drop exercise, there are 2 other types. See documentation:
# http://instructor-support.datacamp.com/en/articles/3039539-course-drag-drop-exercises

# Make sure you only use SPACES, NOT TABS in front of each line.

# Drag zone that holds all the options.
# Specify an ID for this zone to use in SCTs.
- id: options
  title: "Options" # Title of your zone This is not shown with more than 2 zones.

# You can keep adding drop zones to sort to.
# This example has 2 zones.
- id: dropzone_r
  title: "R"
  items: # Each drop zone has a list of items it contains. These will be shown in a random fashion.
    - content: "stringr" # Name of an item. Feel free to use markdown.
      id: stringr # ID of the item. This can be used in the SCTs.
    - content: "dplyr"
      id: dplyr

- id: dropzone_python
  title: "Python"
  items:
    - content: "pandas"
      id: pandas
    - content: "numpy"
      id: numpy
    
```

`@sct`
```{python}
checks: # Individual checks and custom messages per item. This is optional. Without it, it will check that the options are as in the solution code.
  - condition: check_target(pandas) == dropzone_python # Check that pandas is in dropzone_python.
    incorrectMessage: 'Hmm! Pandas is a Python package.' # If that condition is not true, show this message.
  - condition: check_target(numpy) == dropzone_python
    incorrectMessage: 'Damn, this is far from perfect!'
  - condition: check_target(dplyr) == dropzone_r
    incorrectMessage: "Hmm, keep doing R courses! :-)"
  - condition: check_target(stringr) == dropzone_r
    incorrectMessage: "How funny if stringr would be a Python package."
successMessage: "Congratulations" # Message shown when all is correct.
failureMessage: "Try again!" # Message shown when there are errors (and there is no specific error available).
isOrdered: false # Should the items in the zones be ordered as in the solution code?
```

---

## Splitting training data

```yaml
type: VideoExercise
key: fec319acfc
xp: 50
```

`@projector_key`
4431e9cf244429e6528873fcc513f9c8

---

## Practice splitting the training data

```yaml
type: NormalExercise
key: 2f5ebc22a7
xp: 100
```

Split the training data into two subsets, one for training, and one held out for evaluating the trained model. We'll provide a seed for sampling so that the result is the same each time.
<!-- Guidelines for contexts: https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->

`@instructions`
<!-- Guidelines for instructions https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->
- Split the data into training and test sets having 80% and 20% of the data, respectively. We'll use a seed of 42.

`@hint`
<!-- Examples of good hints: https://instructor-support.datacamp.com/en/articles/2379164-hints-best-practices. -->
- The first argument must be a list or tuple having at least two elements.

`@pre_exercise_code`
```{python}
schema = StructType([StructField("uid", StringType()),
                     StructField("rabbit", IntegerType()),
                     StructField("likes", StringType())])

null_array_udf = fun.udf(lambda x:
                x if (x and type(x) is list and len(x)>0 )
                else [],
                ArrayType(StringType()))

df = spark.read.csv('data/rabbitduck/rabbitduck.csv', header=True, schema=schema)\
           .withColumn('likes', fun.split('likes', ','))\
           .withColumn('numlikes', fun.when(fun.col('likes').isNull(),0).otherwise(fun.size('likes')))\
           .withColumn('likes', null_array_udf('likes'))
cv = CountVectorizer(inputCol='likes', outputCol='likesvec')
model = cv.fit(df)
dfx = model.transform(df)\
           .select('uid','rabbit','likesvec','numlikes')\
           .withColumnRenamed('rabbit','label')\
           .withColumnRenamed('likesvec','features')

# Can reduce the amount of calculation done here by having dfx loaded from precalculated dataframe.
```

`@sample_code`
```{python}
# Split the dataset into 80% training, 20% test
split = ____
df_trainset, df_testset = dfx.randomSplit(split, 42)
```

`@solution`
```{python}
# Split the dataset into 80% training, 20% test
split = (0.80,0.20)
df_trainset, df_testset = dfx.randomSplit(split, 42)
```

`@sct`
```{python}
# Here let's use smart SCT rather than pattern matching to allow learners to use a broader array of solutions with less templating. 
# The solution should be either a tuple or a list T, with at least two elements, such that T[0]/(sum(T))=0.8, and T[1]/(sum(T))=0.2
success_msg("This works. The first argument can be either a list or tuple.  The elements can be integer or floats.  Weights are normalized if they don’t sum up to 1.0.")
```

---

## Instantiating a logistic regression model

```yaml
type: VideoExercise
key: f449609098
xp: 50
```

`@projector_key`
a8ec4fd5af8dd4601917a62506b4a93e

---

## Practice instantiating a logistic regression model

```yaml
type: NormalExercise
key: 11c3448e3f
xp: 100
```

<!-- Guidelines for contexts: https://instructor-support.datacamp.com/en/articles/2375526-course-coding-exercises. -->
Import the logistic regression module and instantiate a logistic regression model. 

`@instructions`
- Import the logistic regression module
- Set the maximum iterations to 1000, the regularization parameter to 0.4, and the elastic net parameter to 0.

`@hint`
- Use the following arguments: maxIter, regParam, and elasticNetParam

`@pre_exercise_code`
```{python}

```

`@sample_code`
```{python}
# Import the logistic regression module
from pyspark.ml.classification import ____

# Set max iters=1000, regularization=0.4, elastic net to 0
logistic = LogisticRegression(____, ____, ____)
```

`@solution`
```{python}
# Import the logistic regression module
from pyspark.ml.classification import LogisticRegression

# Set max iters=1000, regularization=0.4, elastic net to 0
logistic = LogisticRegression(maxIter=1000, regParam=0.4, elasticNetParam=0)
```

`@sct`
```{python}
# Either use smart pattern matching, allowing spaces, arguments out of order,
# or, inspect the logistic variable.
success_msg("Good job. Hyperparameter argument names are often just abbreviated versions of the long name .")

```

---

## Fitting a logistic regression model on training data

```yaml
type: VideoExercise
key: f4e525ec4a
xp: 50
```

`@projector_key`
52490a7b002b562eb0311debaa38d13e

---

## Practice fitting a logistic regression model

```yaml
type: NormalExercise
key: 78a79135bd
xp: 100
```

An instance of a LogisticRegression object is provided in the `logistic` variable. A dataframe containing training data is provided in the `df_trainset` variable. Fit `logistic` on the training data.

`@instructions`
- Fit the logistic regression model provided by `logistic` on the training data provided in `df_trainset`

`@hint`
- Use the `fit` function to train the model.

`@pre_exercise_code`
```{python}
from pyspark.ml.classification import LogisticRegression

####

from pyspark.ml.feature import CountVectorizer
from pyspark.sql.types import ArrayType, StringType, IntegerType, StructType, StructField
import pyspark.sql.functions as fun
from pyspark import SQLContext

sqlContext = SQLContext.getOrCreate(spark.sparkContext)

schema = StructType([StructField("uid", StringType()),
                     StructField("rabbit", IntegerType()),
                     StructField("likes", StringType())])

null_array_udf = fun.udf(lambda x:
                x if (x and type(x) is list and len(x)>0 )
                else [],
                ArrayType(StringType()))

df = spark.read.csv('data/rabbitduck/rabbitduck.csv', header=True, schema=schema)\
           .withColumn('likes', fun.split('likes', ','))\
           .withColumn('numlikes', fun.when(fun.col('likes').isNull(),0).otherwise(fun.size('likes')))\
           .withColumn('likes', null_array_udf('likes'))

cv = CountVectorizer(inputCol='likes', outputCol='likesvec')
model = cv.fit(df)
dfx = model.transform(df)\
           .select('uid','rabbit','likesvec','numlikes')\
           .withColumnRenamed('rabbit','label')\
           .withColumnRenamed('likesvec','features')
df_trainset, df_testset = dfx.randomSplit((0.80,0.20), 42)

####

# Can remove most of the above by loading df_trainset from precomputed df saved to file

logistic = LogisticRegression(maxIter=1000, regParam=0.4, elasticNetParam=0)


```

`@sample_code`
```{python}
# Fit logistic on df_trainset
df_fitted = logistic.____(df_trainset)

```

`@solution`
```{python}
# Fit logistic on df_trainset
df_fitted = logistic.fit(df_trainset)

```

`@sct`
```{python}
success_msg("Yes. Once the model and training data are configured, training the model can be done in one line of code.")
```

---

## Evaluating a trained model on test data

```yaml
type: VideoExercise
key: 30919b23d5
xp: 50
```

`@projector_key`
464703834defe4caf41ad03582682533

---

## Practice evaluating a trained model on test data

```yaml
type: NormalExercise
key: d5aa9a9f24
xp: 100
```

A trained model is provided in `df_fitted`.  A dataframe containing test data is provided in `df_testset`.  

`@instructions`
- Evaluate the area under curve for the fitted model

`@hint`
<!-- Examples of good hints: https://instructor-support.datacamp.com/en/articles/2379164-hints-best-practices. -->
- This is an example hint.
- This is an example hint.

`@pre_exercise_code`
```{python}

####

from pyspark.ml.feature import CountVectorizer
from pyspark.sql.types import ArrayType, StringType, IntegerType, StructType, StructField
from pyspark.ml.classification import LogisticRegression
import pyspark.sql.functions as fun
from pyspark import SQLContext

sqlContext = SQLContext.getOrCreate(spark.sparkContext)

schema = StructType([StructField("uid", StringType()),
                     StructField("rabbit", IntegerType()),
                     StructField("likes", StringType())])

null_array_udf = fun.udf(lambda x:
                x if (x and type(x) is list and len(x)>0 )
                else [],
                ArrayType(StringType()))

df = spark.read.csv('data/rabbitduck/rabbitduck.csv', header=True, schema=schema)\
           .withColumn('likes', fun.split('likes', ','))\
           .withColumn('numlikes', fun.when(fun.col('likes').isNull(),0).otherwise(fun.size('likes')))\
           .withColumn('likes', null_array_udf('likes'))


cv = CountVectorizer(inputCol='likes', outputCol='likesvec')
model = cv.fit(df)
dfx = model.transform(df)\
           .select('uid','rabbit','likesvec','numlikes')\
           .withColumnRenamed('rabbit','label')\
           .withColumnRenamed('likesvec','features')
df_trainset, df_testset = dfx.randomSplit((0.80,0.20), 42)
logistic = LogisticRegression(maxIter=1000, regParam=0.4, elasticNetParam=0)
df_fitted = logistic.fit(df_trainset)

####

# Can eliminate most of the code above by loading df_fitted and df_testset from file
```

`@sample_code`
```{python}
# Evaluate the area under ROC curve on the test data
print("Test AUC: " + str(df_fitted.____(df_testset).____))

```

`@solution`
```{python}
# Evaluate the area under ROC curve on the test data
print("Test AUC: " + str(df_fitted.evaluate(df_testset).areaUnderROC))

```

`@sct`
```{python}
# SCT : pattern match on desired result (approximately 0.6454443704677009)
success_msg("Perfect. This model correctly classifies over 64% of the test data.")

```
