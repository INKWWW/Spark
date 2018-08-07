#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark import SparkContext, SparkConf
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
import pandas as pd
# from time import time
import pdb

conf = SparkConf().setAppName("spark_tri_element").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.master("local").appName("spark_tri_element").config("", "").getOrCreate()


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
    # delete the 'flag' column - index=81
    del_cols = ['_c0', '_c1', '_c2', '_c81']
    feature_cols = [i for i in df.columns if i not in del_cols]
    df_after_del = df.select(feature_cols)
    vecAssembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    feature_df = vecAssembler.transform(df_after_del)
    fea_flag_df = feature_df.withColumn('label', df['_c81'])
    print(fea_flag_df)
    pdb.set_trace()
    return fea_flag_df


def mlFormat_V2(df):
    """reorganize the format"""
    # delete the 'flag' column - index=81
    del_cols = ['_c0', '_c1', '_c2', '_c81']
    feature_cols = [i for i in df.columns if i not in del_cols]
    vecAssembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    stringIndexer = StringIndexer(inputCol="_c81", outputCol="label")  # 这一步是按照frequency对label进行排序并标注
    pipeline = Pipeline(stages=[vecAssembler, stringIndexer])
    pipelineFit = pipeline.fit(df)
    feature_df = pipelineFit.transform(df)
    return feature_df


def mlFormat_V3(df):
    """reorganize the format"""
    del_cols = ['_c0', '_c1', '_c2', '_c81']
    feature_cols = [i for i in df.columns if i not in del_cols]
    vecAssembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    feature_df = vecAssembler.transform(df)
    fea_flag_df = feature_df.withColumn('label', feature_df['_c81'])
    # print(fea_flag_df)
    # pdb.set_trace()
    return fea_flag_df


def buildModel(df):
    """build model"""
    # lr = LogisticRegression(maxIter=5, regParam=0.3, elasticNetParam=0)
    # lr = LogisticRegression(maxIter=50, elasticNetParam=0.8)
    # lr = LogisticRegression(maxIter=100, elasticNetParam=0.8)
    # lr = LogisticRegression(maxIter=150, elasticNetParam=0.8)
    lr = LogisticRegression(maxIter=500, elasticNetParam=0.8)
    lr_model = lr.fit(df)
    lr_model_summary = lr_model.summary

    objectiveHistory = lr_model_summary.objectiveHistory
    print("objectiveHistory: ")
    for objective in objectiveHistory:
        print(objective)
    # print('Final Model\'s precisionByLabel: ')
    # print(lr_model_summary.precisionByLabel)
    return lr_model


def saveModel(model, modelPath):
    """保存模型"""
    model.save(modelPath)
    print('################# save successfully #################')


def testJustify(lrModelPath, test_data):
    lrModel = LogisticRegressionModel.load(lrModelPath)
    predictions = lrModel.transform(test_data)

    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")  # attention：the inputted parameter
    accuracy = evaluator.evaluate(predictions)
    print('############ accuracy: {} ############'.format(accuracy))
    return predictions


def main():
    # trainPath = 'file:///opt/int_group/dis_3key_45_flag.csv'
    # testPath = 'file:///opt/int_group/dis_3key_6_flag.csv'

    # reaa from hdfs
    trainPath = 'hdfs://bcg/opt/int_group/dis_3key_45_flag.csv'
    testPath = 'hdfs://bcg/opt/int_group/dis_3key_6_flag.csv'
    train_df = getData(trainPath)
    test_df = getData(testPath)

    print('@@@@@@@@@@@@@@@@@$$$$$$$ step1:getData  ##################$$$$$$')

    train_df = str2float(train_df)
    test_df = str2float(test_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step2:str2float  ##################$$$$$$')

    # derive new variables
    new_train_df = deriveVar(train_df)
    new_test_df = deriveVar(test_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step3:deriveVar  ##################$$$$$$')

    # reformat
    new_train_df = mlFormat_V3(new_train_df)
    new_test_df = mlFormat_V3(new_test_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step4:mlFormat  ##################$$$$$$')

    # bulid model
    lr_model = buildModel(new_train_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step5:build  ##################$$$$$$')

    # predict
    prediction = testJustify(lr_model, new_test_df)
    print('@@@@@@@@@@@@@@@@@####### sucess ###########################')


def buildSaveModel():
    # 读取数据
    trainPath = 'hdfs://bcg/opt/int_group/dis_3key_45_flag.csv'
    train_df = getData(trainPath)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step1:getData  ##################$$$$$$')

    # 转换数据格式
    train_df = str2float(train_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step2:str2float  ##################$$$$$$')

    # 变量衍生
    new_train_df = deriveVar(train_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step3:deriveVar  ##################$$$$$$')

    # re-format
    new_train_df = mlFormat_V3(new_train_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step4:mlFormat  ##################$$$$$$')

    # 建立模型
    lr_model = buildModel(new_train_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step5:build  ##################$$$$$$')

    # 保存模型
    saveModel(lr_model, 'hdfs://bcg/opt/int_group/lrModel_500')



if __name__ == '__main__':
    # start = time.time()
    # main()
    # end = time.time()
    # elapse = end - start
    # print('******** Elapsed time: {}********'.format(elapse))

    # 训练并保存模型
    buildSaveModel()