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


def mlFormat_V2(df):
    """reorganize the format"""
    # delete the 'flag' column - index=81
    del_cols = ['_c0', '_c1', '_c2', '_c81']
    feature_cols = [i for i in df.columns if i not in del_cols]
    vecAssembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    stringIndexer = StringIndexer(inputCol="_c81", outputCol="label")
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


def testJustify(lrModelPath, test_data):
    lrModel = LogisticRegressionModel.load(lrModelPath)

    predictions = lrModel.transform(test_data)

    evaluator = BinaryClassificationEvaluator(rawPredictionCol="prediction")  # attention：the inputted parameter
    accuracy = evaluator.evaluate(predictions)
    print('############ accuracy: {} ############'.format(accuracy))  # 正确率
    return predictions


def testResult(lrModelPath, test_data, threshold):
    lrModel = LogisticRegressionModel.load(lrModelPath)
    predictions = lrModel.transform(test_data)
    label = predictions.select('label').collect()
    label_list = [label[i][0] for i in range(0, len(label))]
    probability = predictions.select('probability').collect()
    prob_list = [probability[i][0][1] for i in range(0, len(probability))]  # ！此处取出1的概率！！！

    # tag
    flag = []
    for prob in prob_list:
        if prob >= threshold:
            flag.append(float(1))
        else:
            flag.append(float(0))

    # 评测
    acc = 0
    for j in range(0, len(label_list)):
        if label_list[j] == flag[j]:
            acc += 1
    accuracy = acc / len(label_list)
    print('-------accuracy--------: {}'.format(accuracy))

    tp, fn, tn, fp = 0, 0, 0, 0
    length = len(label_list)
    for i in range(0, length):
        if label_list[i] == 0.0 and flag[i] == 0.0:
            tn += 1
        if label_list[i] == 1.0 and flag[i] == 1.0:
            tp += 1
        if label_list[i] == 1.0 and flag[i] == 0.0:
            fn += 1
        if label_list[i] == 0.0 and flag[i] == 1.0:
            fp += 1
    # precision
    total = tn + tp + fn + fp
    print('tn:', tn)
    print('tp:', tp)
    print('fn', fn)
    print('fp:', fp)
    print('total:', total)
    precision = tp / (tp + fp)
    print('-------precision--------: {}'.format(precision))
    # recall
    recall = tp / (tp + fn)
    print('-------recall--------: {}'.format(recall))
    f1_score = 2 * ((precision * recall) / (precision + recall))


def main():
    # 读取数据
    testPath = 'file:///opt/int_group/dis_3key_6_flag.csv'
    test_df = getData(testPath)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step1:getData  ##################$$$$$$')
    # 转换数据格式
    test_df = str2float(test_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step2:str2float  ##################$$$$$$')

    # 变量衍生
    new_test_df = deriveVar(test_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step3:deriveVar  ##################$$$$$$')

    # re-format
    new_test_df = mlFormat_V3(new_test_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ step4:mlFormat  ##################$$$$$$')

    lrModelPath = 'hdfs://bcg/opt/int_group/lrModel_500'
    # testJustify(lrModelPath, new_test_df)
    # testResult(lrModelPath, new_test_df, threshold=0.6)
    testResult(lrModelPath, new_test_df, threshold=0.5)
    print('######################### Successfully #########################')




if __name__ == '__main__':
    main()