#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Exception: Python in worker has different version 2.7 than that in driver 3.6, 
# PySpark cannot run with different minor versions.Please check environment variables PYSPARK_PYTHON and 
# PYSPARK_DRIVER_PYTHON are correctly set.

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import BinaryClassificationEvaluator
# from pyspark import SparkContext, SparkConf
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorSlicer
from pyspark.ml.feature import SQLTransformer
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vector
import pandas as pd
# from time import time
import pdb
import os

# os.environ['PYSPARK_PYTHON']='/opt/anaconda3/bin/python'

# conf = SparkConf().setAppName("spark_tri_element").setMaster("local")
# sc = SparkContext(conf=conf)
spark = SparkSession.builder.master("spark://10.0.120.106:7077").appName("spark_tri_element").config("", "").getOrCreate()

# conf = SparkConf().setAppName("spark_tri_element").setMaster("spark://m120p106:6066")
# sc = SparkContext(conf=conf)
# spark = SparkSession.builder.master("spark://m120p106:6066").appName("spark_tri_element").config("", "").getOrCreate()



def getData(dataPath):
    # train dataset & test dataset
    data_df = spark.read.csv(dataPath)
    return data_df


def str2float(df):
    cols = df.columns
    transfer_cols = [i for i in cols if i not in ['_c0', '_c1', '_c2']]
    for col in transfer_cols:
        df = df.withColumn(col, df[col].cast('float'))
    return df


def deriveVar(df):
    """derive new variables - cnt相乘"""
    index_list = range(3, 81, 3)
    for i in index_list:
        for j in index_list:
            col_1 = '_c' + str(i)
            col_2 = '_c' + str(j)
            new_col_name = col_1 + col_2
            col_1_column = df[col_1]
            col_2_column = df[col_2]
            new_col_val = col_1_column * col_2_column
            df = df.withColumn(new_col_name, new_col_val)
    return df


def mlFormat(df):
    """reorganize the format"""
    del_cols = ['_c0', '_c1', '_c2']
    feature_cols = [i for i in df.columns if i not in del_cols]
    vecAssembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    feature_df = vecAssembler.transform(df)
    return feature_df


def predict_prob(lrModelPath, test_data):
    lrModel = LogisticRegressionModel.load(lrModelPath)
    predictions = lrModel.transform(test_data)
    result = predictions.select(['_c0', '_c1', '_c2', 'probability'])
    print('*************** result **************')
    print(result.show(5))
    # result.write.csv('file:///opt/int_group/result123')

    vs = VectorSlicer(inputCol="probability", outputCol="prob_1", indices=[1])
    prob_1 = vs.transform(result)
    print('*************** prob_1 **************')
    print(prob_1.show(5))
    result_prob1 = prob_1.select(['_c0', '_c1', '_c2', 'prob_1'])
    print('*************** result_prob1 **************')
    print(result_prob1.show(5))
    # for i in range(800, 802):
    #     g = i / 1000
    #     h = g + 0.001
    #     sqlTrans = SQLTransformer(statement="SELECT _c0, _c1, _c2, prob_1[0] AS prob FROM __THIS__ WHERE prob_1[0] < h  AND prob_1[0] >= g")
    #     dd = sqlTrans.transform(result_prob1)
    #     dd.write.csv('file:///opt/int_group/sql_test')

    new_result_prob1 = result_prob1.select(['_c0', '_c1', '_c2', result_prob1['prob_1'].cast('string').alias('prob_1_str')])
    print('*************** new_result_prob1 **************')
    print(new_result_prob1.show(5))
    print(new_result_prob1)

    dd = new_result_prob1.head(1000)
    dd_df = spark.createDataFrame(dd)
    dd_df.write.csv('file:///opt/int_group/head_1kw_test')
    # for i in [1,2,3,4,5]:
    #     dd = new_result_prob1.head(i)
    #     dd_df = spark.createDataFrame(dd)
    #     dd_df.write.csv('file:///opt/int_group/head_test', mode='append')
    

    # DataFrame[_c0: string, _c1: string, _c2: string, prob_1_str: string]
    
    ###
    '''
    Error:
    Exception: Python in worker has different version 2.7 than that in driver 3.6, 
    PySpark cannot run with different minor versions.Please check environment variables PYSPARK_PYTHON and 
    PYSPARK_DRIVER_PYTHON are correctly set.
    '''
    # new_result_prob1.toPandas().to_csv('file:///opt/int_group/result.csv')


    # new_result_prob1.toPandas().to_csv('hdfs://bcg/opt/int_group/result/result.csv')

    ###
    '''
    Error:
    py4j.protocol.Py4JJavaError: An error occurred while calling o3124.csv.
    : org.apache.spark.SparkException: Job aborted.
    '''
    # new_result_prob1.write.csv('hdfs://bcg/opt/int_group/result2')
    # new_result_prob1.write.csv('file:///opt/int_group/all_3key_flag3')


    # df.write.csv('/opt/int_group/whm1')
    # result_prob1_pd = result_prob1.toPandas()
    # print('*************** result_prob1_pd **************')
    # print(result_prob1_pd.show(5))
    # result_prob1.toPandas().to_csv('file:///opt/int_group/all_3key_flag.csv', index=False)
    # result_prob1.write.csv('file:///opt/int_group/all_3key_flag.csv')
    # result_prob1.toPandas().write.csv('file:///opt/int_group/all_3key_flag.csv')
    

def main():
    # 读取数据
    # testPath = 'file:///opt/int_group/dis_3key_6_flag.csv'
    # testPath = 'file:///opt/int_group/dis_3key_3_flag.csv'
    # testPath = 'file:///opt/int_group/agg_3key_feature.csv'
    # testPath = 'file:///opt/int_group/agg_3key_feature.csv_001'
    testPath = '/opt/int_group/agg_3key_feature.csv'
    test_df = getData(testPath)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step1:getData  ##################$$$$$$')
    # 转换数据格式
    test_df = str2float(test_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step2:str2float  ##################$$$$$$')
    # 变量衍生
    new_test_df = deriveVar(test_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step3:deriveVar  ##################$$$$$$')

    # re-format
    new_test_df = mlFormat(new_test_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step4:mlFormat  ##################$$$$$$')

    lrModelPath = 'hdfs://bcg/opt/int_group/lrModel_500'
    predict_prob(lrModelPath, new_test_df)
    print('######################### Successfully #########################')
    


if __name__ == '__main__':
    main()
