#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
当前spark版本：1.6
python版本：2.7.5
"""

"""
训练模型，输出概率。针对6亿全库。
"""

import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import VectorSlicer


# The entry of spark
conf = SparkConf().setAppName("predict_prob_all").setMaster("spark://10.0.120.106:7077")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)


def colnameDict(filepath):
    with open(filepath, 'r') as f:
        fread = f.read()
        lines = fread.split('\n')
        col_name_dict = {}
        for line in lines:
            try:
                words = line.split(':')
                col_name_dict[int(words[0])] = words[1]
            except IndexError as e:
                pass
    return col_name_dict


def getDataDBTrain():
    """Get data from database"""
    hiveContext.sql("use tri_element")
    read_data = hiveContext.sql('select * from 3key_45_flag')
    return read_data


def getDataDBAll():
    """Get data from database"""
    hiveContext.sql("use tri_element")
    read_data = hiveContext.sql('select * from agg_3key_feature')
    return read_data


def str2float(df):
    cols = df.columns
    transfer_cols = [i for i in cols if i not in ['id', 'name', 'cell']]
    for col in transfer_cols:
        df = df.withColumn(col, df[col].cast('float'))
    return df


def deriveVar(df, col_name_dict):
    """derive new variables - cnt相乘"""
    index_list = range(3, 81, 3)
    for i in index_list:
        for j in index_list:
            col_1 = col_name_dict[i]
            col_2 = col_name_dict[j]
            new_col_name = col_1 + col_2
            col_1_column = df[col_1]
            col_2_column = df[col_2]
            new_col_val = col_1_column * col_2_column
            df = df.withColumn(new_col_name, new_col_val)
    return df


def mlFormat(df, del_cols):
    """reorganize the format"""
    feature_cols = [i for i in df.columns if i not in del_cols]
    vecAssembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    feature_df = vecAssembler.transform(df)
    if 'flag' in del_cols:
        feature_df = feature_df.withColumn('label', df['flag'])
        feature_df = feature_df.withColumn('label', feature_df['label'].cast('double'))  # 必选转换成double
    return feature_df


def buildModel(df):
    """build model"""
    lr = LogisticRegression(maxIter=500, elasticNetParam=0.8)
    lr_model = lr.fit(df)
    return lr_model


# def predict_prob(lrModelPath, test_data):
def predict_prob(lrModel, test_data):
    # lrModel = LogisticRegressionModel.load(lrModelPath)
    predictions = lrModel.transform(test_data)
    result = predictions.select(['id', 'name', 'cell', 'probability'])
    print('*************** result **************')
    print(result.show(5))

    vs = VectorSlicer(inputCol="probability", outputCol="prob_1", indices=[1])
    prob_1 = vs.transform(result)
    print('*************** prob_1 **************')
    print(prob_1.show(5))

    result_prob1 = prob_1.select(['id', 'name', 'cell', 'prob_1'])
    print('*************** result_prob1 **************')
    print(result_prob1.show(5))

    new_result_prob1 = result_prob1.select(['id', 'name', 'cell', result_prob1['prob_1'].cast('string').alias('prob_1_str')])
    print('*************** new_result_prob1 **************')
    print(new_result_prob1.show(5))
    print(new_result_prob1)
    return new_result_prob1


def write2db(df):
    hiveContext.sql("use tri_element")
    data.registerTempTable('temp_table')
    hiveContext.sql('create table if not exists tri_element.all_3key_prob as select * from temp_table')
    return


def mainBuildModel(col_name_dict):
    print('@@@@@@@@@@@@@@@@@$$$$$$$ 1.bulid model  ##################$$$$$$')
    # 读取数据
    train_df = getDataDBTrain()
    print('@@@@@@@@@@@@@@@@@$$$$$$$ 1.step1:getData  ##################$$$$$$')

    # 转换数据格式
    train_df = str2float(train_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ 1.step2:str2float  ##################$$$$$$')

    # 变量衍生
    new_train_df = deriveVar(train_df, col_name_dict)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ 1.step3:deriveVar  ##################$$$$$$')

    # re-format
    del_cols = ['id', 'name', 'cell', 'flag']
    new_train_df = mlFormat(new_train_df, del_cols)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ 1.step4:mlFormat  ##################$$$$$$')

    # 建立模型
    lr_model = buildModel(new_train_df)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ 1.step5:build  ##################$$$$$$')
    return lr_model
    


def main(lr_model, col_name_dict):
    print('@@@@@@@@@@@@@@@@@$$$$$$$ 2.predict_probability_all  ##################$$$$$$')
    # 读取数据
    read_data = getDataDBAll()
    print('@@@@@@@@@@@@@@@@@$$$$$$$ 2.step1:getData  ##################$$$$$$')
    # 转换数据格式
    read_data = str2float(read_data)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ 2.step2:str2float  ##################$$$$$$')
    # 变量衍生
    read_data = deriveVar(read_data, col_name_dict)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ 2.step3:deriveVar  ##################$$$$$$')
    # re-format
    del_cols = ['id', 'name', 'cell']
    new_read_data = mlFormat(read_data, del_cols)
    print('@@@@@@@@@@@@@@@@@$$$$$$$ 2.step4:mlFormat  ##################$$$$$$')

    # lrModelPath = 'hdfs://bcg/opt/int_group/lrModel_500'
    new_result_prob1 = predict_prob(lr_model, new_read_data)
    print('######################### 2.step5:predict #########################')

    write2db(new_result_prob1)
    print('######################### 2.step6:write to database #########################')
    print('######################### Successfully #########################')
    

if __name__ == '__main__':
    # 列名字典
    filepath = './col_name.txt'
    col_name_dict = colnameDict(filepath)

    lr_model = mainBuildModel(col_name_dict)
    main(lr_model, col_name_dict)
