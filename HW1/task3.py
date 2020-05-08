from pyspark import SparkContext
from pyspark import SparkConf
import json
import sys
import time


# sparkConf = SparkConf().setMaster("local[*]").setAppName("weixin_task3") #SparkContext的初始化需要一个SparkConf对象，SparkConf包含了Spark集群配置的各种参数
#                                                   # 初始化后，就可以使用SparkContext对象所包含的各种方法来创建和操作RDD和共享变量。
# sc = SparkContext(conf = sparkConf)
sc = SparkContext(master='local[*]', appName='weixin_task3')
num_partitions=80
sc.setLogLevel("ERROR")

def opt_funtion(key):
    return hash(key)%num_partitions

test_start_time=time.time()

result={}
file_input1="../yelp_dataset/review.json"
file_input2="../yelp_dataset/business.json"
file_output1="../task3_1.txt"
file_output2="../task3_2.txt"

# file_input1=sys.argv[1]
# file_input2=sys.argv[2]
# file_output1=sys.argv[3]
# file_output2=sys.argv[4]

reviews_rdd=sc.textFile(file_input1).map(lambda line: json.loads(line))
businesses_rdd=sc.textFile(file_input2).map(lambda line:json.loads(line))


reviews_businessid_stars_rdd=reviews_rdd.map(lambda entry: (entry['business_id'], entry['stars'])).partitionBy(num_partitions,opt_funtion)
businesses_businessid_cities=businesses_rdd.map(lambda entry: (entry['business_id'], entry['city'])).partitionBy(num_partitions,opt_funtion)

businessid_cities=businesses_businessid_cities.join(reviews_businessid_stars_rdd).map(lambda entry:entry[1]).partitionBy(num_partitions,opt_funtion) # 【'city','stars'】

tmp=(0,0)
                                                        #这个操作根据key来进行区分操作 count表示value sum表示的是tmp 前一个entry[0]表示和   后一个entry[1]表示个数
businessid_cities=businessid_cities.aggregateByKey(tmp, lambda entry, value: (entry[0] + value,    entry[1] + 1),     #第一个操作是seqOp tmp表示元素集合的初始值
                                                        lambda entry, value: (entry[0] + value[0], entry[1] + value[1])  ) #第二个函数是combp 分区的结果相加
                                                                                                                    # 这里没有分区所以不用管


# print(businessid_cities.collect()) #得到的是 （key，tmp）类型的文件  ('Peoria', (209482.0, 57857))
average=businessid_cities.mapValues(lambda tmp: tmp[0]/tmp[1])




#taskB Method1
start_time_1=time.time()
data_python=average.collect()
method1=sorted(data_python,key=lambda entry:(-entry[1],entry[0]))
method1_res=method1[:10]
for entry in method1_res:
    print(entry[0])
end_time_1=time.time()
method1_time=end_time_1-start_time_1
#print("Method1:"+ str(method1_res))


#taskB Method2
start_time_2=time.time()
method2_res=average.sortBy(lambda entry: (-entry[1], entry[0])).take(10)
for entry in method2_res:
    print(entry[0])
end_time_2=time.time()
method2_time=end_time_2-start_time_2

res_time={}
res_time['m1']=method1_time
res_time['m2']=method2_time

sorted_average = average.sortBy(lambda entry: (-entry[1], entry[0]))   # - 表示降序 先根据评分降序 再根据名字升序
taskA_data=sorted_average.collect()   #这两部要放到下面 因为放上面会影响排序时间 Spark中 sortByKey被划分到transformation中，却有action操作原因



with open(file_output1, 'w') as out:
    out.write("city,stars"+'\n')
    for entry in taskA_data:
        out.write(entry[0]+','+str(entry[1])+'\n')



with open(file_output2, 'w') as out:
    json.dump(res_time, out)


test_end_time=time.time()
print("Time:"+str(test_end_time-test_start_time))


#65s