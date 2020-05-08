import itertools
import math
from random import sample
from operator import add
from pyspark import SparkContext
from pyspark import SparkConf
import json
import time
from itertools import combinations
import pyspark
import sys
import os


def process_data(input_file_train,input_file_test):
    rdd_train = sc.textFile(input_file_train,3)
    head_line = rdd_train.first()
    rdd_train = rdd_train.filter(lambda line: line != head_line).map(lambda line: line.split(','))
    rdd_train = rdd_train.map(lambda row: tuple([row[0], row[1], float(row[2])]))


    rdd_val=sc.textFile(input_file_test,2)
    head_line=rdd_val.first()
    rdd_val=rdd_val.filter(lambda line: line != head_line).map(lambda line: line.split(','))
    #rdd_val=rdd_val.map(lambda line: (line[0], line[1],float(line[2])))
    rdd_val=rdd_val.map(lambda line: (line[0], line[1]))
    return rdd_train,rdd_val

#
# def getSigMatrix():
#     params = dict()
#     bands_num = 22 #18
#     hash_num = 50  # 51 hash
#     # hash > f(x) = ((ax + b) % p) % m:number of minhash
#     b = sample(range(1, hash_num * 100), hash_num)
#     params['b'] = b
#     p = [getPrime(i) for i in sample(range(1, hash_num * 100), hash_num)]
#     params['p'] = p
#     a = sample(range(1, hash_num * 100), hash_num)  # random 随机抽取user_num个数 from (1,5100)
#     params['a'] = a
#     params['m'] = getPrime(user_num)
#     signatures = business_userindex.flatMap(lambda line: gethash(line, params)).reduceByKey(lambda x, y: x if x <= y else y)
#     #((business, i_index), hashvalue_min)
#     return signatures,bands_num



#
# def Jarccard(candidates, business_userindexgroup):   #  candidates==>business      business_userindexgroup[candidates[0]]==>{user1,user2,user3.....}
#     business1=candidates[0]
#     collection1=business_userindexgroup[business1]
#
#     business2=candidates[1]
#     collection2=business_userindexgroup[business2]
#     return (candidates, len(collection1&collection2)/len(collection1|collection2))


def calculate(value):
    yield (value[0][0], (value[0][1], value[1]))
    yield (value[0][1], (value[0][0], value[1]))

def getAverageDict(rdd_train):    # 计算平均分
    overall_average = rdd_train.map(lambda line: line[2]).mean()  # 所以的平均分
    # print(overall_average)
    average_rdd = rdd_train.map(lambda line: (line[0], line[2]))  # (user,stars)
    average_rdd = average_rdd.aggregateByKey((0, 0), lambda line, stars: (line[0] + stars, line[1] + 1),
                                                     lambda line, stars: (line[0] + stars[0], line[1] + stars[1]) )
    average = average_rdd.map(lambda line: (line[0], line[1][0] / line[1][1])).collect()
    average_dict = dict(average)
    return average_dict,overall_average
    # print(average_dict)

#
def prediction(rdd_val,overall_average,average_dict,sim_data_dict,user_dict):#prediction:
    test_data = rdd_val.map(lambda line: (line[0], line[1]))
    predict_data = test_data.map(lambda candidate: (candidate, calculate_prediction(overall_average,average_dict,sim_data_dict,user_dict,candidate)))
    predict_data_Output = predict_data.map(lambda line: line[0][0] + ',' + line[0][1] + ',' + str(line[1]))
    #print(predict_data_Output.collect())
    return predict_data_Output

def isPrime(n):
    if n == 2 or n == 3:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    for k in range(6, int(math.sqrt(n)) + 2, 6):
        if n % (k - 1) == 0 or n % (k + 1) == 0:
            return False
    return True
                        #overall_average,average_dict,sim_data_dict,user_dict,candidate
def calculate_prediction(overall_average,average_dict,sim_data_dict,user_dict,candidate):
    if not average_dict.get(candidate[0]):
        return overall_average
    #print(overall_average)
    if not sim_data_dict.get(candidate[1]):
        return average_dict[candidate[0]]
    if len(sim_data_dict[candidate[1]]) == 0:
        return average_dict[candidate[0]]

    user_target = user_dict[candidate[0]]
    userlist = dict(sim_data_dict[candidate[1]])
    items = [x for x, val in userlist.items()]

    most = [user_target[item] * userlist[item] for item in items if user_target.get(item)]
    least = [abs(userlist[item]) for item in items if user_target.get(item)]
    if len(least) == 0:
        return average_dict[candidate[0]]

    res= sum(most) / sum(least)
    if res > 5:
        return 5
    if res < 0:
        return 0
    return res


def getSimDict(user_rdd):
    partition_rdd = user_rdd.mapPartitions(lambda bucket: compare_User(n_user, bucket)) #iterator
    sim_flat_rdd = partition_rdd.flatMap(lambda item: item)
    # similarity_rdd=similarity_rdd.reduceByKey(add)
    similarity_rdd = sim_flat_rdd.reduceByKey(lambda x, y: x + y)
    pearson_rdd = similarity_rdd.mapValues(Pearson)
    similarity_rdd2 = pearson_rdd.filter(lambda val: abs(val[1]) > 0).flatMap(lambda item: calculate(item)).groupByKey()
    similarity_data_rdd = similarity_rdd2.map(lambda val: (val[0], findmostsim(2, val[1])))
    sim_data = similarity_data_rdd.collect()
    sim_data_dict = dict(sim_data)
    #print(sim_data)
    return sim_data_dict


def findmostsim(n, val):
    val = sorted(list(val), key = lambda x: -x[1])
    size=len(val)
    end_index=min(n,size)
    return val[:end_index]

def compare_User(n_users,iterator):
    count = {}
    threshold=7
    buckets= list(iterator)
    part_num = len(buckets)
    part_threshold = int(threshold * part_num / n_users)

    for bucket in buckets:
        collection = sorted(list(bucket), key=lambda item: item[0])
        for x, y in combinations(collection, 2):
            try:
                count[tuple([x[0], y[0]])] = count[tuple([x[0], y[0]])]+[(x[1], y[1])]
            except:
                count[tuple([x[0], y[0]])] = [(x[1], y[1])]

    yield [(entry, stars) for entry, stars in count.items() if len(stars) >= part_threshold]


def write_file(data_output):
    with open(output_file, 'w') as f:
        f.write("user_id, business_id, prediction" + "\n")
        for index in range(len(data_output)):
            f.write(data_output[index] + '\n')
        f.close()


def Pearson(arr):
    arr1 = list(arr)
    size = len(arr1)
    user1 = [a[0] for a in arr1]
    average_a=sum(user1)/len(user1)
    user2 = [a[1] for a in arr1]
    average_b=sum(user2)/len(user2)

    if size == 1:
        return 0
    num1 = sum([(user1[i]-average_a)*(user2[i]-average_b) for i in range(size)])
    num2 = sum([(user1[i]-average_a)**2 for i in range(size)])
    if num2 == 0:
        return 0
    num3 = sum([(user2[i]-average_b)**2 for i in range(size)])
    if num3 == 0:
        return 0

    return num1/num2**0.5/num3**0.5



sc = SparkContext(master='local[*]', appName='weixin_HW3_task2_1')
start_time=time.time()
input_file_train="./data/yelp_train.csv"
input_file_test="./data/yelp_val.csv"
output_file="./task2_1.csv"

# input_file_train=sys.argv[1]
# input_file_test=sys.argv[2]
# output_file=sys.argv[3]


rdd_train,rdd_val=process_data(input_file_train,input_file_test)
user_train_data = rdd_train.map(lambda line: (line[0], (line[1], line[2])))
user_train_data =user_train_data.groupByKey().mapValues(dict).collect()   #(userid,{'business1,star','business2,star',...})

#print(user_train_rdd)
#user_dict
user_dict = dict(user_train_data)

#计算平均分
average_dict,overall_average=getAverageDict(rdd_train)


#threshold=80
#getUserdict
user_rdd=rdd_train.map(lambda line:(line[0],(line[1],line[2]))).groupByKey().map(lambda line:line[1]) #(business,stars)
n_user=user_rdd.count()
#print(user_rdd.collect())

#get user candidates:
sim_data_dict=getSimDict(user_rdd)

#print(sim_data_dict)


#prediction:
#output_data=prediction(rdd_val)
predict_data_Output =prediction(rdd_val,overall_average,average_dict,sim_data_dict,user_dict)
output_data=list(predict_data_Output.collect())
write_file(output_data)


end_time = time.time()
duration=end_time-start_time
print(duration)