# -*- coding=utf8 -*- 
from __future__ import print_function
import json
import re
import string
import numpy as np

from pyspark import SparkContext, SparkConf
from pyspark import SQLContext

#host = 'student25-x1,student25-x2,student26-x1'
host = 'student25-x1,student25-x2,student26-x1,student57-x1,student57-x2'

table = 'RawTweet'
keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
conf = {"hbase.zookeeper.quorum": host,"hbase.mapred.outputtable": table,"mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat","mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable","mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
'''
def to_String(accum):
    accum += 1
    return str(accum)
'''
sc = SparkContext('local', 'writeHbaseTable')
#rawData = ['first sentence','sencond sentence']
#sc.parallelize(rawData).map(lambda x: (x[0],x.split(','))).saveAsNewAPIHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)

# rawData = [['sparkrow1', 'cf', 'a', 'value1']]
#rawData = ['first sentence','sencond sentence']
rawData = sc.textFile("file:///home/hduser/presentation/writeHbase/demo.txt")

#accum = sc.accumulator(0)
#accum = 0
new_rdd = rawData.flatMap(lambda line: line.split("\n"))
#print(new_rdd.collect())

new_rdd.map(lambda x: (x.split('\t')[0],[x.split('\t')[0],'content','tweet',x.split('\t')[1]])).saveAsNewAPIHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)

