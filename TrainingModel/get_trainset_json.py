#  -*- coding: utf-8 -*- 
import json
from pyspark import SparkContext, SparkConf

def readHbase(sc, table, host, row_start=None, row_stop=None):

    # host = 'student25-x1,student25-x2,student26-x1'
    # table = 'RawTweet'
   
    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
    
    conf = dict()
    conf["hbase.zookeeper.quorum"] = host
    conf["hbase.mapreduce.inputtable"] = table

    if row_start:
        conf["hbase.mapreduce.scan.row.start"] = str(row_start)
    if row_stop:
        conf["hbase.mapreduce.scan.row.stop"] = str(row_stop)

    sc.setLogLevel("WARN")

    hbase_rdd = sc.newAPIHadoopRDD(
    "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
    "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
    "org.apache.hadoop.hbase.client.Result",
    keyConverter=keyConv,
    valueConverter=valueConv,
    conf=conf) #this conf is same conf created above

    # python 自动给出字典序的前一千个号码
    
    count = hbase_rdd.count()

    print "The count is " + str(count)
   
    ''' 
    hbase_rdd.cache()
    output = hbase_rdd.collect()

    json_list = []

    for (k, v) in output:

        #print (k, v)
        dic = json.loads(v)
        dic = dic['value']
        #dic.pop('id')
        dic = json.loads(dic)
        json_list.append(dic)

    print "Finished reading from HBase table {}".format(table)

    #print "Total number of rows is {}".format(len(kv_list))

    return json_list
    '''
    return hbase_rdd
def get_trainset_json(sc):

    host = 'student25-x1,student25-x2,student26-x1,student57-x1,student57-x2'
    '''
    spark_conf = SparkConf().setAppName("getTrainingSetJson")
    spark_conf.set('spark.executor.memory', '2g')
    spark_conf.set('spark.executor.cores', '4')
    spark_conf.set('spark.cores.max', '8')
    spark_conf.set('spark.driver.maxResultSize', '16g')
    spark_conf.set('spark.kryoserializer.buffer.max', '2000')
    
    sc = SparkContext('yarn', conf=spark_conf)
    '''
    read_table = 'RawTrainSet_Json_new'
    #write_table = 'RawTrainSet_Json'

    # total = 15004957
    # part = 30

    # 这两个参数为none时，代表取所有的
    row_start = 0
    row_stop =  101000

    return readHbase(sc, read_table, host, row_start, row_stop)



if __name__ == "__main__":

    # sc, table, host, row_start=None, row_stop=None

    host = 'student25-x1,student25-x2,student26-x1,student57-x1,student57-x2'

    spark_conf = SparkConf().setAppName("getTrainingSetJson")
    spark_conf.set('spark.executor.memory', '2g')
    spark_conf.set('spark.executor.cores', '4')
    spark_conf.set('spark.cores.max', '8')
    spark_conf.set('spark.driver.maxResultSize', '16g')
    spark_conf.set('spark.kryoserializer.buffer.max', '2000')
    
    sc = SparkContext('yarn', conf=spark_conf)

    read_table = 'RawTrainSet_Json'
    #write_table = 'RawTrainSet_Json'

    total = 15004957
    part = 30

    print readHbase(sc, read_table, host, 1, 1000)

