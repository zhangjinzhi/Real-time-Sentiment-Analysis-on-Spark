from pyspark.mllib.feature import Word2Vec
from pyspark import SparkContext, SparkConf

sc = SparkContext('yarn', 'try_word2vector')
#sc = SparkContext(appName='try_word2vector')

sc_conf = SparkConf()

sc_conf.set('spark.yarn.executor.memoryOverhead', '4096')
sc_conf.set('spark.yarn.driver.memoryOverhead ', '8192')
sc_conf.set('spark.akka.frameSize', '700')
sc_conf.set('spark.driver.memory', '5g')
sc_conf.set('spark.executor.memory', '5g')
sc_conf.set('spark.executor.cores', '4')
sc_conf.set('spark.cores.max', '8')
sc_conf.set('spark.driver.maxResultSize', '8g')
sc_conf.set("spark.executor.heartbeatInterval","100s")


inp = sc.textFile("hdfs:///text/new_textac").map(lambda row: row.split(" "))
#inp = sc.textFile("file:///home/hduser/shiyanlou/word2vec/new_textaa").map(lambda row: row.split(" "))

word2vec = Word2Vec()
model = word2vec.setVectorSize(10).fit(inp)
#model = word2vec.setVectorSize(100).fit(inp)


model.save(sc, "hdfs:///model_text8_c")
#model.save(sc, "file:///home/hduser/shiyanlou/word2vec/model_text8")

#synonyms = model.findSynonyms('1', 5)

#for word, cosine_distance in synonyms:
#    print("{}: {}".format(word, cosine_distance))
