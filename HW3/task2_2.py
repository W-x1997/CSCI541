import itertools
import math
from random import sample
from operator import add
import xgboost as xgb
from pyspark import SparkContext
from pyspark import SparkConf
import json
import time
from itertools import combinations
import pyspark
import sys
import os
import numpy as np
import pandas as pd
from math import sqrt


def process_data(input_file_train,input_file_test):
    rdd_train = sc.textFile(input_file_train,3)
    head_line = rdd_train.first()
    rdd_train = rdd_train.filter(lambda line: line != head_line).map(lambda line: line.split(','))
    rdd_train = rdd_train.map(lambda row: [row[0], row[1], float(row[2])])


    rdd_val=sc.textFile(input_file_test,2)
    head_line=rdd_val.first()
    rdd_val=rdd_val.filter(lambda line: line != head_line).map(lambda line: line.split(','))
    #rdd_val=rdd_val.map(lambda line: (line[0], line[1],float(line[2])))
    rdd_val=rdd_val.map(lambda line:  [line[0], line[1]])
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
def get_avg_bus_stars(data, dic_business):
    business = data[1]
    b_a_g = dic_business[business]['stars']
    data.append(b_a_g)
    return data

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


def getfeatures(data,train_user_stars_dict,train_bus_stars_dict,dict_business,photo_business):
    user1 = data[0]
    user_stars = train_user_stars_dict[user1] if user1 in train_user_stars_dict else dict()
    n_size=len(user_stars)
    avg_user_stars = sum([x[1] for x in user_stars]) / n_size if len(
        user_stars) > 0 else 3.751
    business1 = data[1]
    avg_bus_stars = dict_business[business1]['stars']

    user2 = user1
    user2_all_ratings = train_user_stars_dict[user2] if user2 in train_user_stars_dict else dict()
    user_reivews_n = len(user2_all_ratings)

    business2 = business1
    bus_reviews_n = dict_business[business2]['review_count']

    user3=user2
    user_all_ratings = train_user_stars_dict[user3] if user3 in train_user_stars_dict else dict()
    sd_u = np.std([x[1] for x in user_all_ratings])


    business = data[1]
    photo_count = photo_business[business] if business in photo_business else 0

    business_all_ratings = train_bus_stars_dict[
        business] if business in train_bus_stars_dict else dict()
    sd_bus = np.std([x[1] for x in business_all_ratings])


    user_all_ratings = train_user_stars_dict[user3] if user3 in train_user_stars_dict else dict()
    val = 0
    for business, rating in user_all_ratings:
        business_avg = get_avg_bus_stars(['', business], dict_business)[-1]
        val += (rating - business_avg)
    avg_bias = val / len(user_all_ratings) if len(user_all_ratings) > 0 else 0

    data.append(avg_user_stars)
    data.append(avg_bus_stars)
    data.append(user_reivews_n)
    data.append(bus_reviews_n)
    data.append(sd_u)
    data.append(sd_bus)
    data.append(avg_bias)
    data.append(photo_count)
    return data



##wx


# def getSimDict(user_rdd):
#     partition_rdd = user_rdd.mapPartitions(lambda bucket: compare_User(n_user, bucket)) #iterator
#     sim_flat_rdd = partition_rdd.flatMap(lambda item: item)
#     # similarity_rdd=similarity_rdd.reduceByKey(add)
#     similarity_rdd = sim_flat_rdd.reduceByKey(lambda x, y: x + y)
#     pearson_rdd = similarity_rdd.mapValues(Pearson)
#     similarity_rdd2 = pearson_rdd.filter(lambda val: abs(val[1]) > 0).flatMap(lambda item: calculate(item)).groupByKey()
#     similarity_data_rdd = similarity_rdd2.map(lambda val: (val[0], findmostsim(2, val[1])))
#     sim_data = similarity_data_rdd.collect()
#     sim_data_dict = dict(sim_data)
#     #print(sim_data)
#     return sim_data_dict


def findmostsim(n, val):
    val = sorted(list(val), key = lambda x: -x[1])
    size=len(val)
    end_index=min(n,size)
    return val[:end_index]

def compare_User(n_users,iterator):
    count = {}
    threshold=80
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


def write_file(keys_test,predict_values):
    with open(output_file, 'w') as f:
        f.write('user_id, business_id, prediction\n')
        for i in range(len(keys_test)):
            prediction = predict_values[i]
            business = keys_test[i][1]
            user = keys_test[i][0]
            f.write(user + ',' + business + ',' + str(prediction) + '\n')


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



sc = SparkContext(master='local[*]', appName='weixin_HW3_task2_2')
start_time=time.time()
input_file_train="./data/yelp_train.csv"
input_file_test="./data/yelp_val.csv"
output_file="./task2_2.csv"
business_path="./data/business.json"
photo_path="./data/photo.json"

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
#print(average_dict)    #[{user:average}]

train_rdd_key=  rdd_train.map(lambda line:[line[0],line[1]])
test_rdd_key=   rdd_val.map(lambda line:[line[0],line[1]])

dict_business = sc.textFile(business_path).map(lambda x: json.loads(x)). map(lambda x: (x['business_id'], x)).collectAsMap()
photo_business = sc.textFile(photo_path).map(lambda x: (json.loads(x)['business_id'], 1)).reduceByKey(lambda x, y: x + y).collectAsMap()

#train的数据
rdd_train_bus_stars=rdd_train.map(lambda line:(line[1],[(line[0],line[2])]))
rdd_train_bus_stars=rdd_train_bus_stars.reduceByKey(lambda a,b:a+b)
train_bus_stars_dict=rdd_train_bus_stars.collectAsMap() # {business:[user,star]}

rdd_train_user_stars=rdd_train.map(lambda line:(line[0],[(line[1],line[2])]))
rdd_train_user_stars=rdd_train_user_stars.reduceByKey(lambda a,b:a+b)
train_user_stars_dict=rdd_train_user_stars.collectAsMap()

#test的数据
rdd_test_bus_stars=rdd_train.map(lambda line:(line[1],[(line[0],line[2])]))
rdd_test_bus_stars=rdd_test_bus_stars.reduceByKey(lambda a,b:a+b)
test_bus_stars_dict=rdd_test_bus_stars.collectAsMap()

rdd_test_user_stars=rdd_train.map(lambda line:(line[0],[(line[1],line[2])]))
rdd_test_user_stars= rdd_test_user_stars.reduceByKey(lambda a, b: a + b)
test_user_stars_dict=rdd_test_user_stars.collectAsMap()

Y_train =rdd_train.map(lambda x: x[2]).collect()
X_train =train_rdd_key.map(lambda data:getfeatures(data,train_user_stars_dict,train_bus_stars_dict,dict_business,photo_business)). map(lambda x: x[2:]). \
        collect()
X_test = test_rdd_key.map(lambda data:getfeatures(data,train_user_stars_dict,train_bus_stars_dict,dict_business,photo_business)). map(lambda x: x[2:]). \
        collect()



colsample_tree=0.3
learningrate=0.1
min_child_w=5
max_dep=8
alpha=1
gamma=0.1
subsample=0.8
n_est=130
xgb_bst = xgb.XGBRegressor(objective='reg:linear',colsample_bytree=colsample_tree, learning_rate=learningrate,min_child_weight=min_child_w,  max_depth=max_dep, alpha=alpha, gamma=gamma, subsample=subsample, \
                              n_estimators=n_est)

xgb_bst.fit(pd.DataFrame(X_train), pd.DataFrame(Y_train))
predict_y =xgb_bst.predict(pd.DataFrame(X_test))



keys_test=test_rdd_key.collect()

write_file(keys_test,predict_y)


end_time = time.time()
duration=end_time-start_time
print(duration)