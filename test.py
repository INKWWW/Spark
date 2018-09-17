#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SQLContext
import pyspark



def fun1():
    l = [('Alice', 1)]
    SQLContext.createDataFrame(l).collect()

def test_read():
    data = spark.read.text('sample_libsvm_data.txt')

if __name__ == '__main__':
    # fun1()
    test_read()



