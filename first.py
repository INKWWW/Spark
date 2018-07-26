#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkContext

sc = SparkContext('local')
old = sc.parallelize([1,2,3,4,5], 2)
newMap = old.map(lambda x:(x, x**2))
newReduce = old.reduce(lambda a, b : a+b)
print(newMap.glom().collect())
print(newReduce)

