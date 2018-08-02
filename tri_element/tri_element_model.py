#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark import SparkContext, SparkConf
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
import pandas as pd

conf = SparkConf().setAppName("spark_tri_element").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.master("local").appName("spark_tri_element").config("", "").getOrCreate()


def getData(trainPath, testPath):
    # train dataset & test dataset
    # train = '/opt/int_group/dis_3key_45_flag.csv'
    # test = '/opt/int_group/dis_3key_6_flag.csv'
    train_df = spark.read.csv(trainPath)
    test_df = spark.read.csv(testPath)
    return train_df, test_df

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
            col_1_val = df.select(col_1).toPandas().values
            col_2_val = df.select(col_2).toPandas().values
            new_col_val = col_1_val * col_2_val
            new_col_val = spark.createDataFrame(pd.DataFrame(new_col_val))
            new_df = df.withColumn(new_col_name, new_col_val)
    return new_df


def mlFormat(df):
    """reorganize the format"""
    # delete the 'flag' column - index=81
    del_cols = ['_c0', '_c1', '_c2', '_c81']
    feature_cols = [i for i in df.columns if i not in del_cols]
    df_after_del = df.select(feature_cols)
    vecAssembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    feature_df = vecAssembler.transform(df_after_del)
    fea_flag_df = feature_df.withColumn('label', df['_c81'])
    return fea_flag_df


def buildModel(df):
    lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
    lr_model = lr.fit(df)
    return lr_model


def testJustify(lrModel, test_data):
    prediction = lrModel.transform(test_data)

    return prediction


    # trainingSummary = lrModel.summary

def main():
    trainPath = 'file:///opt/int_group/dis_3key_45_flag.csv'
    testPath = 'file:///opt/int_group/dis_3key_6_flag.csv'
    train_df, test_df = getData(trainPath, testPath)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step1:getData  ##################$$$$$$')
    train_df = str2float(train_df)
    test_df = str2float(test_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step2:str2float  ##################$$$$$$')
    # derive new variables
    new_train_df = deriveVar(train_df)
    new_test_df = deriveVar(test_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step3:deriveVar  ##################$$$$$$')
    # reformat
    new_train_df = mlFormat(new_train_df)
    new_test_df = mlFormat(new_test_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step4:mlFormat  ##################$$$$$$')
    # bulid model
    lr_model = buildModel(new_train_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step5:build  ##################$$$$$$')
    # predict
    prediction = testJustify(new_test_df)
    print('@@@@@@@@@@@@@@@@@@@@@@@@@@@############## sucess ###########################')


if __name__ == '__main__':
    main()



