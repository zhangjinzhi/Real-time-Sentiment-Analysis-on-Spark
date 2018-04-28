# -*- coding: utf-8 -*-
import json
import re
from pyspark import SparkContext, SparkConf
from textblob import TextBlob

def readHbase(sc, table, host, row_start, row_stop):

    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
    
    conf = dict()
    conf["hbase.zookeeper.quorum"] = host
    conf["hbase.mapreduce.inputtable"] = table

    if row_start:
        conf["hbase.mapreduce.scan.row.start"] = row_start
    if row_stop:
        conf["hbase.mapreduce.scan.row.stop"] = row_stop

    sc.setLogLevel("WARN")

    hbase_rdd = sc.newAPIHadoopRDD(
    "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
    "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
    "org.apache.hadoop.hbase.client.Result",
    keyConverter=keyConv,
    valueConverter=valueConv,
    conf=conf) #this conf is same conf created above

    count = hbase_rdd.count()

    print "The count is " + str(count)
    hbase_rdd.cache()
    output = hbase_rdd.collect()

    kv_list = []
    for (k, v) in output:
        kv_list.append((k, v))

    print "Finished reading from HBase table {}".format(table)

    #print "Total number of rows is {}".format(len(kv_list))

    return kv_list

def writeHbase(sc, table, host, rawData):

    '''
    @ rawData: a list, each item should in the format like (rowkey, [row_key, CF, col, value])
    '''

    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
    conf = {"hbase.zookeeper.quorum": host,
    "hbase.mapred.outputtable": table,
    "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
    "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
    "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}

    sc.setLogLevel("WARN")
    new_rdd = sc.parallelize(rawData)   
    new_rdd = new_rdd.map(lambda x: x)

    new_rdd.saveAsNewAPIHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)  # 写入
    print "Finished writing"

def cleanData(text):

    regEx = re.compile(r'[^a-zA-Z]|\d')
    words = regEx.split(text)

    words = [word.lower() for word in words if len(word) > 0]
    
    new_words = []
    for c in words:
        if c[0] == 'x' and len(c)<3:
            continue
        new_words.append(c)

    return " ".join(new_words)

def get_RowKey(kv):
    '''
    An example for kv
    (u'1', u'{"qualifier" : "name", "timestamp" : "1524453142221", "columnFamily" : "info", "row" : "1", "type" : "Put", "value" : "Tom"}')
    '''
    return str(kv[0])

def get_Text(kv):
    '''
    An example for kv
    (u'1', u'{"qualifier" : "name", "timestamp" : "1524453142221", "columnFamily" : "info", "row" : "1", "type" : "Put", "value" : "Tom"}')
    '''
    res = json.loads(kv[1])

    if res['qualifier'] == 'tweet':
        text = res['value']
        text = cleanData(text)
        return text
    else:
        return None

def getSentimentScore(line):
    '''
    use textblob to get the Sentiment Score of a tweet.a
    @ return a value from -1 to 1
    '''
    polarity =  TextBlob(line).sentiment.polarity

    if polarity > 0:
        return 0
    elif polarity < 0:
        return 2
    else: 
        return 1


def get_Json(kv):
    
    rowkey = get_RowKey(kv)
    text = get_Text(kv)

    if not text:
        return ""

    raw_dict = {}

    raw_dict['id'] = rowkey
    raw_dict['polarity'] = getSentimentScore(text)
    raw_dict['text'] = text

    return json.dumps(raw_dict)


def get_RowKeyRange(total, part):
    '''
    @ total_no: table中总行数
    @ part_no: 对这些行进行几等分
    '''
    arr = range(total+1)
    arr = map(lambda x: str(x), arr)
    arr.sort() # 这里就得到了字典序的排列

    interval = int(total/part)

    index = 0

    range_list = []

    while index < total:
        start = index
        stop = index + interval

        if stop > total:
            #stop = total
            range_list.append((arr[start], ""))
        else:
            range_list.append((arr[start], arr[stop]))
        
        
        index = stop

    return range_list

if __name__ == "__main__":
    host = 'student25-x1,student25-x2,student26-x1'

    spark_conf = SparkConf().setAppName("getTrainingSetJson")
    spark_conf.set('spark.executor.memory', '2g')
    spark_conf.set('spark.executor.cores', '4')
    spark_conf.set('spark.cores.max', '8')
    spark_conf.set('spark.driver.maxResultSize', '16g')
    spark_conf.set('spark.kryoserializer.buffer.max', '2000')
    
    sc = SparkContext('yarn', conf=spark_conf)

    read_table = 'RawTweet'
    #write_table = 'RawTrainSet_Json_new'
    write_table = "test"    

    total = 15004957 
    part = 300 
    lines2write = 2000 

    range_list = get_RowKeyRange(total, part)

    len_ran = len(range_list)
    count = 0

    for ran in range_list:
        count += 1
        row_start = ran[0]
        row_stop = ran[1]
        kv_list = readHbase(sc, read_table, host, row_start, row_stop)

        inputData = []

        for i in range(len(kv_list)):

            single_json = get_Json(kv_list[0])
            print single_json

            rowkey = get_RowKey(kv_list[0])
            rawData = (rowkey, [rowkey, 'trainset','json', single_json])
            inputData.append(rawData)
            del kv_list[0]

            if len(inputData) > lines2write or (count >= len_ran and i == len(kv_list)-1):
                writeHbase(sc, write_table, host, inputData)
                inputData = []




