#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.ml.linalg import Vectors
from pyspark.sql import Row
from pyspark.ml.classification import LogisticRegression

#双变量Logistic回归
bdf = sc.parallelize([Row(label=1.0,weight=2.0,features=Vectors.dense(1.0)),Row(label=0.0, weight=2.0, features=Vectors.sparse(1,[],[]))]).toDF()
bdf.show()
blor = LogisticRegression(maxIter=5, regParam=0.01,weightCol='weight')
blorModel = blor.fit(bdf)
blorModel.coefficients
blorModel.intercept  

#多元Logistic回归
mdf = sc.parallelize([Row(label=1.0,weight=2.0, features=Vectors.dense(1.0)),Row(label=0.0,weight=2.0, features=Vectors.sparse(1,[],[])),Row(label=2.,weight=2.0, features=Vectors.dense(3.0))]).toDF()
mlor=LogisticRegression(maxIter=5,regParam=0.01,weightCol='weight',family='multinomial')
mlorModel = mlor.fit(mdf)
print(mlorModel.coefficientMatrix)
mlorModel.interceptVector

#模型预测
test0=sc.parallelize([Row(features=Vectors.dense(-1.0))]).toDF()
result = blorModel.transform(test0).head()
result.prediction

result.probability  
result.rawPrediction

test1 = sc.parallelize([Row(features=Vectors.sparse(1,[0],[1.0]))]).toDF()
blorModel.transform(test1).head().prediction
blorModel.transform(test1).show()
#模型评估
blorModel.summary.roc.show()
blorModel.summary.pr.show()