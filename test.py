#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SQLContext


def fun1():
    l = [('Alice', 1)]
    SQLContext.createDataFrame(l).collect()



if __name__ == '__main__':
    fun1()