# -*- coding=utf8 -*- 
from __future__ import print_function
import json
import re
import string
import numpy as np

from pyspark import SparkContext, SparkConf
from pyspark import SQLContext
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.tree import RandomForest,RandomForestModel
from pyspark.mllib.feature import Normalizer
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import Word2Vec
import json

import get_trainset_json

conf = SparkConf().setAppName("sentiment_analysis")



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

lookup = sqlContext.read.parquet("hdfs:///word2vecM_simple/data").alias("lookup")
#lookup = sqlContext.read.parquet("hdfs:///model_text8").alias("lookup")
lookup.printSchema()
lookup_bd = sc.broadcast(lookup.rdd.collectAsMap())

'''
with open('tweets.json', 'r') as f:
    rawTrn_data = json.load(f)
    f.close()
'''
trainset_json_rdd = get_trainset_json.get_trainset_json(sc)
#trnData = trainset_json_rdd.map(lambda x: x[1]).map(lambda x: json.loads(x)).map(lambda x: LabeledPoint((x["value"])["polarity"], (x["value"])["text"]))
#print(trainset_json_rdd.collect())

print("--------begin          rnData = trainset_json_rdd.map(lambda x: x[1])--------")
trnData = trainset_json_rdd.map(lambda x: x[1])
#print(trnData.collect())
print("--------begin         trnData.map(lambda x: json.loads(x))-------")
trnData = trnData.map(lambda x: json.loads(x))
#print(trnData.collect())
print("--------begin trnData.map(lambda x: json.loads(x[value]))---------")
trnData = trnData.map(lambda x: json.loads(x["value"]))
#trnData = trnData.map(lambda x: json.loads((x["value"]).replace('\\','').replace('x5C"','')))
#print(trnData.collect())
print("--------begin  trnData.map(lambda x: LabeledPoint(x[polarity], doc2vec(tokenize(x[text]))))")
trnData = trnData.map(lambda x: LabeledPoint(x["polarity"], doc2vec(tokenize(x["text"]))))
#print(trnData.collect())
'''
trainset_json = trainset_json_rdd
trn_data = []
#for obj in rawTrn_data['results']:
for obj in trainset_json:
    token_text = tokenize(obj['text']) # 规范化推特文本，进行分词
    tweet_text = doc2vec(token_text) # 将文本转换为向量
    # tweet_text = Word2Vec(token_text) # 将文本转换为向量
    # 使用LabeledPoint 将文本对应的情感属性polariy：该条训练数据的标记label，tweet_text：训练分类器的features特征，结合成可作为spark mllib分类训练的数据类型
    trn_data.append(LabeledPoint(obj['polarity'], tweet_text)) 

trnData = sc.parallelize(trn_data)
'''




#print(trnData)
print("------------------------------------------------------")

# 读入donald.json作为分类器测试数据集
with open('donald.json', 'r') as f:
    rawTst_data = json.load(f)
    f.close()

tst_data = []
for obj in rawTst_data['results']:
    token_text = tokenize(obj['text'])
    tweet_text = doc2vec(token_text)
    # tweet_text = Word2Vec(token_text) # 将文本转换为向量
    tst_data.append(LabeledPoint(obj['polarity'], tweet_text))

tst_dataRDD = sc.parallelize(tst_data)

# 训练随机森林分类器模型
model = RandomForest.trainClassifier(trnData, numClasses=3, categoricalFeaturesInfo={},
                                     numTrees=3, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth=4, maxBins=32)


model.save(sc,"hdfs:///myModelPath")

sameModel = RandomForestModel.load(sc,"hdfs:///myModelPath")



# 利用训练好的模型进行模型性能测试
#for text_100_list in [np.ones(100),np.ones(100)+1]:
#predictions = sameModel.predict(text_100_list)
predictions = sameModel.predict(tst_dataRDD.map(lambda x: x.features))

#predictions.collect()
print("----------------------------------")

#print(type(predictions.collect()))

labelsAndPredictions = tst_dataRDD.map(lambda lp: lp.label).zip(predictions)
# 计算分类错误率
testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(tst_dataRDD.count())
print('Right Error = ' + str(testErr))
print('Learned classification tree model:')
# 输出训练好的随机森林的分类决策模型
print(sameModel.toDebugString())


