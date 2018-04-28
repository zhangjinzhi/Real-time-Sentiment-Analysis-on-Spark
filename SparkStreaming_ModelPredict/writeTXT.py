import time
from textblob import TextBlob

#txtName = 'test_write'+ str(time.time()) + '.txt'
def showSentiment(record):
    polarity = TextBlob(record).sentiment.polarity
    if polarity > 0:
        polarity = 1
    if polarity < 0:
        polarity = 2
    if polarity == 0:
        polarity = 0

    return polarity

def writeTXT(input_list):

    txtName = "../website/test_write.txt"
    f_1=file(txtName, 'w+')
    f_2=file("../website/statistic.txt", 'w+')
    #for i in xrange(len(input_list)):
    #    f.write(input_list[i])
    pos_count = 0
    neg_count = 0
    mid_count = 0
    for text in input_list:
        if len(text) > 5:
            anaResult = showSentiment(text)
            if anaResult == 0:
                pos_count += 1
            if anaResult == 1:
                mid_count += 1
            if anaResult == 2:
                neg_count += 1
            print(text, anaResult)
            f_1.write(text.encode('utf-8')+'\n'+str(anaResult)+'\n')
            #result.append(anaResult)
    f_2.write(str(pos_count)+"\n" + str(neg_count) +"\n"+ str(mid_count))
    f_1.close()
    f_2.close()


