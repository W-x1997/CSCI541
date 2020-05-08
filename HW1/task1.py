# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark import SparkConf
import json
import time
import pyspark
import sys
import os

#os.environ["PYSPARK_PYTHON"]="/usr/local/Cellar/python/3.6.3/Frameworks/Python.framework/Versions/3.6/bin/python3.6"

num_partitions=80

def opt_funtion(key):
    return hash(key)%num_partitions




sc = SparkContext(master='local[*]', appName='weixin_task1')

sc.setLogLevel("ERROR")

test_start_time=time.time()
result={}
file_path_input="../yelp_dataset/review.json"
#file_path_input=sys.argv[1]
#file_path_output=sys.argv[2]
file_path_output="../xx.txt"

#time1=time.time()
#rdd=sc.textFile(file_path).map(lambda x: json.loads(x)).map(lambda entry: (entry['review_id'], entry['user_id'])).collect()
reviews_rdd=sc.textFile(file_path_input).map(lambda line: json.loads(line))  #将文本的每一行调用map 解析json


#reviews_rdd.cache() # Preserve the actual items of this RDD in memory

user_id_persist_rdd=reviews_rdd.map(lambda entry:(entry["user_id"],1)).partitionBy(num_partitions,opt_funtion).persist()
n_review=user_id_persist_rdd.count()  #6685900
n_user=user_id_persist_rdd.distinct().count()
n_review_2018=reviews_rdd.filter(lambda entry:entry["date"][:4]=="2018").count()   # 1177662



top_10_user= user_id_persist_rdd.reduceByKey(lambda x1, x2: x1+x2).sortBy(lambda entry: (-entry[1],entry[0])).take(10)

#user_times = reviews_rdd.map(lambda entry: entry["user_id"]).countByValue()  #countByvalue 返回一个map，map的key是元素的值，value是出现的次数
#sort_user_review = sorted(user_times.items(),key=lambda x: x[1],reverse=True)
#top_10_user=sort_user_review[:10]
#print(top_10_user)

n_business_rdd=reviews_rdd.map(lambda entry: (entry['business_id'],1)).partitionBy(num_partitions,opt_funtion).reduceByKey(lambda x1, x2:x1+x2)
n_business=n_business_rdd.count()
#print(n_business)

top10_business=n_business_rdd.sortBy(lambda entry:(-entry[1], entry[0])).take(10)#collect()[:10]


result["n_review"]=n_review
result["n_review_2018"]=n_review_2018
result["n_user"]=n_user
result["top10_user"]=top_10_user
result["n_business"]=n_business
result["top10_business"]=top10_business

test_end_time=time.time()
print("Time:"+str(test_end_time-test_start_time))





with open(file_path_output, 'w') as outfile:
    json.dump(result, outfile)

#120s