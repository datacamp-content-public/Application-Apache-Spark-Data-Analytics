---
key: de4c973a4184a72f5f2cca4e4e4a9c8b
title: 'Insert title here'
---

## Tuning a hyperparameter

```yaml
type: TitleSlide
key: a64e1de02b
```

`@lower_third`
name: Full Name
title: Instructor

`@script`


---

## Grid search on a hyperparameter

```yaml
type: FullCodeSlide
key: 1633893d1b
```

`@part1`
```
import numpy as np

# Trains on users having at least 4 likes.
dfx4 = model.transform(df.where('numlikes>=%d ' % 4 )) \
            .select('uid','rabbit','likesvec','numlikes')\
            .withColumnRenamed('rabbit','label')\
            .withColumnRenamed('likesvec','features')
df_train, df_test = dfx4.randomSplit(SPLIT, 42)
num_test=df_test.count()
for enp in np.linspace(0,1,11):
    lr = LogisticRegression(maxIter=1000, regParam=0.4, elasticNetParam=enp)
    df_fitted2 = lr.fit(df_train)
    testSum = df_fitted2.evaluate(df_test)
    num_tested = df_test.count()
    print("elasticNetParam : %.1f  :  test areaUnderROC: %.2f  " 
    		% ( enp, testSum.areaUnderROC))
    
```



`@script`


---

## Grid search on the elasticNetParam hyperparameter

```yaml
type: FullCodeSlide
key: 44a956d824
```

`@part1`
```
elasticNetParam : 0.0  :  test areaUnderROC: 0.90  

elasticNetParam : 0.1  :  test areaUnderROC: 0.86  

elasticNetParam : 0.2  :  test areaUnderROC: 0.75  

elasticNetParam : 0.3  :  test areaUnderROC: 0.64  

elasticNetParam : 0.4  :  test areaUnderROC: 0.50  

elasticNetParam : 0.5  :  test areaUnderROC: 0.50  

elasticNetParam : 0.6  :  test areaUnderROC: 0.50  

elasticNetParam : 0.7  :  test areaUnderROC: 0.50  

elasticNetParam : 0.8  :  test areaUnderROC: 0.50  

elasticNetParam : 0.9  :  test areaUnderROC: 0.50  

elasticNetParam : 1.0  :  test areaUnderROC: 0.50  


```

`@script`


---

## Let's practice!

```yaml
type: FinalSlide
key: e369dff4d5
```

`@script`
