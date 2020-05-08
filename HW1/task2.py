from pyspark import SparkContext
from pyspark import SparkConf
import json
import sys
import time


sparkConf = SparkConf().setMaster("local[*]").setAppName("weixin_task2") #SparkContext的初始化需要一个SparkConf对象，SparkConf包含了Spark集群配置的各种参数

sc = SparkContext(conf = sparkConf)


test_start_time=time.time()
res={}
file_path_input="../yelp_dataset/review.json"
file_path_output="../xx3.txt"
n_partition=80
#file_path_input=sys.argv[1]
#file_path_output=sys.argv[2]
#n_partition = int(sys.argv[3])

reviews_rdd=sc.textFile(file_path_input).map(lambda line: json.loads(line))
res['default'] = {}
res['customized'] = {}


#默认分割RDD
start_time_default= time.time()
res['default']['n_items'] = reviews_rdd.glom().map(len).collect() #glom 用来显示分区    接着len计算每个分区长度
top10_business_reviews=reviews_rdd.map(lambda entry: (entry['business_id'],1)).reduceByKey(lambda val1,val2,:val1+val2).sortBy(lambda entry:(-entry[1], entry[0])).take(10)
end_time_default=time.time()
default_time=end_time_default-start_time_default
#print(default_time)
res['default']['n_partition'] = reviews_rdd.getNumPartitions()
res['default']['exe_time'] =default_time


#自定义分割RDD
start_time_customized = time.time()
rdd_customized= reviews_rdd.map(lambda entry: (entry['business_id'], 1)).partitionBy(n_partition, lambda entry: hash(entry[0])% n_partition)#自定义
res['customized']['n_items'] = rdd_customized.glom().map(len).collect()
ans = rdd_customized.reduceByKey(lambda x, y: x+y).sortBy(lambda entry:(-entry[1], entry[0])).take(10)
end_time_customized =time.time()
customized_time=end_time_customized-start_time_customized
#print(customized_time)
res['customized']['n_partition'] = n_partition
res['customized']['exe_time'] = customized_time


#print(str(result))
with open(file_path_output, 'w') as output:
 	json.dump(res, output)


test_end_time=time.time()
print("Time:"+str(test_end_time-test_start_time))


