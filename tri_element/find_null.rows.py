#!/usr/bin/env python
# -*- coding: utf-8 -*-

## predict_porb_v6.py
"""
建立外部表 external
hive> create external table 6yi_3key
    > (id  string,
    > name  string,
    > cell string,
    > prob  string)
    > ROW FORMAT DELIMITED 
    > FIELDS TERMINATED BY ','
    > location '/opt/int_group/hanmo.wang/3key_all_v3';


create table all_3key_prob as
select id, name, cell, regexp_replace(prob, '\\[|]', '') as prob
from 6yi_3key;


insert overwrite table all_3key_prob
select cell, cell_mask, regexp_replace(date, '/', '0') as date, target
from applyloan_grap.match_group_20180808

"""



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



spark = SparkSession.builder.master("spark://10.0.120.106:7077").appName("spark_tri_element").config("", "").getOrCreate()


def findNUllRow(dataPath):
    # train dataset & test dataset
    data_df = spark.read.csv(dataPath, sep='\001')
    # data_df = data_df.na.drop()
    null_rows = data_df.filter(data_df._c0.isNull() | data_df._c1.isNull() | data_df._c2.isNull())
    null_rows.show(100)
    return null_rows


def getData(dataPath):
    # train dataset & test dataset
    data_df = spark.read.csv(dataPath, sep='\001')
    # data_df = data_df.na.drop()
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

    new_result_prob1 = result_prob1.select(['_c0', '_c1', '_c2', result_prob1['prob_1'].cast('string').alias('prob_1_str')])
    print('*************** new_result_prob1 **************')
    print(new_result_prob1.show(10))
    print(new_result_prob1)
    # find null rows
    final_null_rows = new_result_prob1.filter(new_result_prob1._c0.isNull() | new_result_prob1._c1.isNull()\
        | new_result_prob1._c2.isNull() | new_result_prob1.prob_1_str.isNull())
    print('########### find null rows #############')
    final_null_rows.show(100)

    # new_result_prob1 = new_result_prob1.na.drop()
    #### write
    # new_result_prob1.write.csv('/opt/int_group/hanmo.wang/3key_all_v3', nullValue=None, mode='append')


def main():
    # 读取数据
    dirname = 'hdfs://bcg/user/hive/warehouse/tri_element.db/agg_3key_notnull/'
    # for i in range(8, 10):
    #     number = 1000000 + i 
    #     num_str = str(number)
    #     num_list = list(num_str)
    #     num_list.pop(0)
    #     num_str = ''.join(num_list)
    #     new_num_str = num_str + '_0'
    #     new_path = dirname + new_num_str
    # testPath = 'hdfs://bcg/user/hive/warehouse/tri_element.db/agg_3key_notnull/000001_0'

    test_df = getData(dirname)
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
    # print('######################### {} #########################'.format(new_path))
        


if __name__ == '__main__':
    ## 主程序 - 读-预测-写
    # main()
    
    ## 查询一开始读取的数据的空行
    # dirname = 'hdfs://bcg/user/hive/warehouse/tri_element.db/agg_3key_notnull/'
    # findNUllRow(dirname)

    ## 查询最终处理完的数据的空行 - 注意修改predict_prob函数最后
    main() ## 如果前面读取数据的时候df.na.drop了，则结果为空！！！
