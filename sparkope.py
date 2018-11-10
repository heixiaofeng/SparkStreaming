from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import imageio
from wordcloud import WordCloud, ImageColorGenerator


#绘制词云
I = 0
#定义List
allwords = []
def draw_wordcloud(wordlist):
    if wordlist != '':
        #读取背景图片
        color_mask = imageio.imread("beijing.png")
        cloud = WordCloud(
            #设置字体，不指定会出现乱码
            font_path = "FZSJ-XRWFTJW.TTF",
            #设置背景色
            background_color = 'white',
            #词云形状
            mask = color_mask,
            #允许最大词汇
            max_words = 2000,
            #最大号字体
            max_font_size = 80
        )
        # 将接收到的List与之前的合并
        allwords.extend(wordlist.collect())# 先从RDD转成list
        for i in range(0, len(allwords) - 1):
            cloud.generate(allwords.__str__()) #产生词云
        # 保存图片
        try:
            global I
            cloud.to_file("/home/wangfeng/cloudc/firstrealwork/resultpicActor/cloud"+str(I)+".jpg")
            print('------------------------------------------------------------保存图片')
            I += 1
        except Exception as e:
            print('----------------------图片保存异常:'+str(e))


#获取电影演员信息
def  getActors(s):
    print('------------------------s:', s)
    try:
        result = json.loads(s)
    except Exception as e:
        return []
    return result['actors']

# 定义文件的保存名字并保存文件
#N=0
#def saveFile(x):
#    global N
#    if x.collect():
#        x.saveAsTextFile('/wf/result' + str(N))
#        N += 1


#SparkStreaming程序主体
try:
    sc = SparkContext("local[2]", "Movie")
    #每隔10秒读取一次
    ssc = StreamingContext(sc, 10)
    #打开文件流
    data = ssc.socketTextStream("localhost", 9999)
    #data = ssc.textFileStream("hdfs://node-1/wf")
    #对电影类型进行map-reduce操作
    typelist = data.map(lambda s: getActors(s))
    wordslist = typelist.flatMap(lambda e: e)
    #wordCounts = wordslist.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)
    wordslist.pprint()
    #对DStream中的每个RDD进行保存
    wordslist.foreachRDD(
        #saveFile
        draw_wordcloud
    )
    ssc.start()
    ssc.awaitTermination()
except Exception as e:
    print('-----------Socket连接中断------------------')