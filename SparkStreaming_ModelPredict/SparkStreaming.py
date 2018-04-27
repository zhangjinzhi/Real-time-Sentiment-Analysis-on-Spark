# -*- coding=utf8 -*- 
from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
import time
from collections import namedtuple
from textblob import TextBlob
#from pyspark.sql import HiveContext
import writeTXT

import json
import re
import string
import numpy as np

from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.tree import RandomForest,RandomForestModel
from pyspark.mllib.feature import Normalizer
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import Word2Vec
import json


conf = SparkConf().setAppName("sentiment_analysis")


conf.set('spark.executor.instances',4)
conf.set('spark.executor.memory', '2g')
conf.set('spark.executor.cores', '4')
conf.set('spark.cores.max', '8')
conf.set('spark.driver.maxResultSize', '16g')
conf.set('spark.kryoserializer.buffer.max', '2030')

sc = SparkContext('yarn', conf=conf)

# sc = SparkContext(conf=conf)

sc.setLogLevel("INFO")
sqlContext = SQLContext(sc)


#寻找推文的协调性
#符号化推文的文本
#删除停用词，标点符号，url等
remove_spl_char_regex = re.compile('[%s]' % re.escape(string.punctuation))  # regex to remove special characters
stopwords = [u'rt', u're', u'i', u'me', u'my', u'myself', u'we', u'our',u'ours', u'ourselves', u'you', u'your',
             u'yours', u'yourself', u'yourselves', u'he', u'him', u'his', u'himself', u'she', u'her', u'hers',
             u'herself', u'it', u'its', u'itself', u'they', u'them', u'their', u'theirs', u'themselves', u'what',
             u'which', u'who', u'whom', u'this', u'that', u'these', u'those', u'am', u'is', u'are', u'was', u'were',
             u'be', u'been', u'being', u'have', u'has', u'had', u'having', u'do', u'does', u'did', u'doing', u'a',
             u'an', u'the', u'and', u'but', u'if', u'or', u'because', u'as', u'until', u'while', u'of', u'at', u'by',
             u'for', u'with', u'about', u'against', u'between', u'into', u'through', u'during', u'before', u'after',
             u'above', u'below', u'to', u'from', u'up', u'down', u'in', u'out', u'on', u'off', u'over', u'under',
             u'again', u'further', u'then', u'once', u'here', u'there', u'when', u'where', u'why', u'how', u'all',
             u'any', u'both', u'each', u'few', u'more', u'most', u'other', u'some', u'such', u'no', u'nor', u'not',
             u'only', u'own', u'same', u'so', u'than', u'too', u'very', u's', u't', u'can', u'will', u'just', u'don',
             u'should', u'now']




# tokenize函数对tweets内容进行分词
def tokenize(text):
    tokens = []
    text = text.encode('ascii', 'ignore')  # to decode
    text = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*(),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '',
                  text)  # to replace url with ''
    text = remove_spl_char_regex.sub(" ", text)  # Remove special characters
    text = text.lower()

    for word in text.split():
        if word not in stopwords \
                and word not in string.punctuation \
                and len(word) > 1 \
                and word != '``':
            tokens.append(word)
    return tokens

def doc2vec(document):
    # 100维的向量
    doc_vec = np.zeros(100)
    tot_words = 0



    for word in document:
        try:
        # 查找该词在预训练的word2vec模型中的特征值
            vec = np.array(lookup_bd.value.get(word))
            #print(vec)
            # print(vec)
            # 若该特征词在预先训练好的模型中，则添加到向量中
            #print(vec == None)

            if vec.all() is None:
                continue
            else:
                #print(vec)
                #print(type(vec))
                #print(id(vec))     
                #print(vec)
                vec += 1
                doc_vec += vec
                tot_words += 1

        except Exception, e:
            print(e)
            #print("this is exception---------------------------------------------------------------------------")
            continue
    #print(tot_words)
    vec = doc_vec / float(tot_words)
    #print(vec)
    return vec



lookup = sqlContext.read.parquet("hdfs:///model_text8_c/data").alias("lookup")
#lookup = sqlContext.read.parquet("hdfs:///model_text8").alias("lookup")
lookup.printSchema()
lookup_bd = sc.broadcast(lookup.rdd.collectAsMap())




print("------------------------------------------------------")

def process_text(text):

    token_text = tokenize(text)
    tweet_text = doc2vec(token_text)

    return tweet_text

sameModel = RandomForestModel.load(sc,"hdfs:///myModelPath_2")



# 利用训练好的模型进行模型性能测试
#for text_100_list in [np.ones(100),np.ones(100)+1]:
#predictions = sameModel.predict(text_100_list)
def model_predict(text):
    
    text_100_list = process_text(text)
    predictions = sameModel.predict(text_100_list)

    return predictions






def showSentiment(record):
    #polarity = TextBlob(record).sentiment
    polarity = model_predict(record)
    print(polarity)
    if polarity > 0:
        polarity = 1
    if polarity < 0:
        polarity = 2
    if polarity == 0:
        polarity = 0

    return polarity

def takeAndPrint(time, rdd, num=1000):
    result = []
    taken = rdd.take(num + 1)
    #url = 'ws://localhost:8888/'

    print("-------------------------------------------")
    print("Time: %s" % time)
    print("-------------------------------------------")
    '''
    for i in xrange(len(taken[:num])):
        if len(taken[i]): 
            anaResult = showSentiment(taken[i])
            print(i, taken[i], anaResult)
            result.append(anaResult)
    '''
    writeTXT.writeTXT(taken)    
    #save to file??

    # ws = create_connection(url)
    # ws.send(json.dumps(result))
    # ws.close()
    print("-------------------------------------------")
    print(len(taken))
    if len(taken) > num:
        print("...")
    print("")




#sc = SparkContext()

#sc = SparkContext()
ssc = StreamingContext(sc, 10 )
#sqlContext = HiveContext(sc)
ssc.checkpoint( "file:///home/hduser/Tweet_SparkStreaming/checkpoint")

socket_stream = ssc.socketTextStream("10.42.0.90", 5555)

lines = socket_stream.window( 10 )

print("---------------------------------------------")
print(type(lines))



running_counts = lines.flatMap(lambda line: line.split("\n"))


running_counts.foreachRDD(takeAndPrint)


#running_counts = lines.flatMap(lambda line: line)


#running_counts.foreachRDD(takeAndPrint)


ssc.start()
ssc.awaitTermination()
