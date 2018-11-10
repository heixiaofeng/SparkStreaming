from pyspark import SparkContext,SparkConf
import json


#建立电影评分和演员对应关系
def  getType(s):
    actors = []
    print('------------------------s:', s)
    try: 
        result = json.loads(s)
        for x in result['actors']:
            dict2 = {}
            dict2['name'] = x
            dict2['rate'] = float(result['star'])

            print('--------------key:', dict2['name'])
            print('--------------value:', dict2['rate'])

            actors.append(dict2)
       
    except Exception as e:
        return actors
    print('----------------actors:', actors)
    return actors
    


#SparkStreaming程序主体
try:
    conf = SparkConf().setAppName("ActorStar").setMaster("local[2]")
    sc = SparkContext(conf=conf)

    data = sc.textFile("file:///home/wangfeng/cloudc/firstrealwork/data3.json")
    #进行map-reduce操作
    actor_star_list = data.map(lambda s: getType(s)).flatMap(lambda e:e)
    wordslist = actor_star_list.map(lambda e: (e['name'], e['rate'])).reduceByKey(lambda x,y: x+y)
    words = wordslist.sortBy(lambda x:x[1],ascending=False).collect()
    for i in range(20):
        print(words[i])

except Exception as e:
    print('-----------------------------:', e)