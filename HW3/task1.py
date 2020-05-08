import itertools
import math
from random import sample
import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
import json
import time

import sys
import os

def process_data(input_file):
    raw_rdd = sc.textFile(input_file)
    head_line = raw_rdd.first()
    raw_rdd = raw_rdd.filter(lambda line: line != head_line).map(lambda line: line.split(','))
    user_business_rdd = raw_rdd.map(lambda line: (line[0], line[1]))
    return user_business_rdd

def getPrime(num):
    for n in range(num+1, num+1000):
        if isPrime(n): return n


def getSigMatrix():
    params = dict()
    bands_num = 22 #18
    hash_num = 50  # 51 hash
    # hash > f(x) = ((ax + b) % p) % m:number of minhash
    b = sample(range(1, hash_num * 100), hash_num)
    params['b'] = b
    p = [getPrime(i) for i in sample(range(1, hash_num * 100), hash_num)]
    params['p'] = p
    a = sample(range(1, hash_num * 100), hash_num)  # random 随机抽取user_num个数 from (1,5100)
    params['a'] = a
    params['m'] = getPrime(user_num)
    signatures = business_userindex.flatMap(lambda line: gethash(line, params)).reduceByKey(lambda x, y: x if x <= y else y)
    #((business, i_index), hashvalue_min)
    return signatures,bands_num


def isPrime(n):
    if n == 2 or n == 3:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    for k in range(6, int(math.sqrt(n)) + 2, 6):
        if n % (k - 1) == 0 or n % (k + 1) == 0:
            return False
    return True


def Jarccard(candidates, business_userindexgroup):   #  candidates==>business      business_userindexgroup[candidates[0]]==>{user1,user2,user3.....}
    business1=candidates[0]
    collection1=business_userindexgroup[business1]

    business2=candidates[1]
    collection2=business_userindexgroup[business2]
    return (candidates, len(collection1&collection2)/len(collection1|collection2))


def gethash(line, params): #line: [business, user_index]
    m = params['m']
    p = params['p']
    a = params['a']
    b = params['b']
    len_hash = len(b)
    #print(len_hash)
    #print(a)
    #print(m)
    for i in range(len_hash):  #((ax + b) % p) % m
        yield ((line[0], i),( (  (line[1]*a[i]+b[i]) %p[i])  %m)) #( (business, i_index), hash_value)

def write_file(data_output):
    with open(output_file, 'w') as f:
        f.write("business_id_1, business_id_2, similarity" + "\n")
        for index in range(len(data_output)):
            f.write(data_output[index] + '\n')
        f.close()


sc = SparkContext(master='local[*]', appName='weixin_HW3_task1')
start_time=time.time()
input_file="./data/yelp_train.csv"
output_file="./task1.txt"

user_business_rdd=process_data(input_file)  #(user,busines)

#user process     allusers
users_rdd=user_business_rdd.map(lambda line:line[0]).distinct().sortBy(lambda user:user)  #(user1,user2,user3...)
users=users_rdd.collect()
user_dict=dict()
for index, item in enumerate(users): # item==>user       list(enumerate(seasons))[(0, 'Spring'), (1, 'Summer'), (2, 'Fall'), (3, 'Winter')]
        user_dict[item] = index
user_num=users_rdd.count()

#(business_id, user_Index)
business_userindex=user_business_rdd.map(lambda entry:(entry[1],user_dict[entry[0]]))

#group by business   eg:   'business':{user1,user2,user3...}
business_userindexgroup=dict(business_userindex.groupByKey().mapValues(lambda item:set(item)).collect())
#print(business_userindexgroup)    #'FsbzGuFBMVV99T2otrfsmA': {10017, 3937, 742, 10055, 1448, 1577, 3143, 5405, 8268, 1964, 814, 6609, 5491, 3605, 8087, 858, 8733}

signatures,bands_num=getSigMatrix()

#line: [( (business, i_index), hash_value)]    line[0][1]%band==> the number of bucket
#(band_index, business), (i_Index(user), minhashvalue)
signatures_bands_1=signatures.map(lambda line: ((line[0][1]%bands_num, line[0][0]), (line[0][1], line[1]))).groupByKey()
#print(signatures_bands_1.collect())

#line: (band_index, business), (hash_Index(user), minhashvalue)
#( frozenset( hash_Index(user), minhashvalue ) , business_id)
signatures_bands_2=signatures_bands_1.map(lambda line: (frozenset(line[1]), line[0][1])).groupByKey()
#print(signatures_bands_2.collect())

#get business
signatures_bands_3=signatures_bands_2.map(lambda item: sorted(list(item[1]))).filter(lambda items: len(items)>1)
#print(signatures_bands_3.collect())

candidates=signatures_bands_3.flatMap(lambda bucket: [item for item in itertools.combinations(bucket, 2)])
candidates=candidates.distinct().sortBy(lambda item: (item[0], item[1]))
#print(candidates.collect())

# calculateLSH
similarity =candidates.map(lambda items: Jarccard(items, business_userindexgroup)).filter(lambda value: value[1]>=0.5)
similarity =similarity.sortBy(lambda item: (item[0][0], item[0][1]))

#print(similarity.collect())

data_output = similarity.map(lambda item: item[0][0]+','+item[0][1]+','+str(item[1]))
data_output=list(data_output.collect())
write_file(data_output)
#print(output)

end_time = time.time()
duration=end_time-start_time
print(duration)


