import itertools
import math
from random import sample

from pyspark import SparkContext
from blackbox import BlackBox
import time
import binascii
import datetime
from itertools import combinations
import pyspark
import sys
import os


SIZE=69997
HASH_FUNCTIONS= [[11, 49, 1543], [4, 801, 24593], [17, 81, 6151], [387, 509, 98317], [3, 36, 193] , [41, 196, 393242] ,  \
                [19, 144, 24593], [59, 757, 7873], [5, 16, 389], [443, 119, 196613], [11, 311, 6299], [37, 225, 786433],\
                [31, 121, 12289], [127, 487, 7691],[1, 25, 769], [39, 53, 4363],[37, 53, 5651],[19, 100, 12289],\
                [41, 193, 786437],[43, 227, 196611],[71, 111,1229],[5, 25, 193]]

index=0
true_negative = 0
false_positive = 0
user_set=set()
filter_array=[0 for _ in range(SIZE)]




def bloom_filter(users):
    global index
    global true_negative
    global false_positive

    for item in users:
        tag = True
        result = myhashs(item)

        #print(myhashs(item))
        id = int(binascii.hexlify(item.encode('utf8')), 16)

        for i in result:
            if filter_array[i]==0 : #
                tag = False
                filter_array[i] = 1

        if(tag):
            if id not in user_set:
                user_set.add(id)
                false_positive=false_positive+1
                continue
        # else:
        user_set.add(id)
        true_negative=true_negative+1

    FPR=0
    sum=true_negative+false_positive
    if sum>0:
       ack_sum=float(sum)
       FPR=false_positive/ack_sum

    f = open(output_file, "a")
    # f.write(str(index))
    # #print(index)
    # f.write(",")
    # f.write(str(FPR))
    # f.write("\n")
    f.write(f"\n{index},{FPR}")


    #print(str(index) + "," + str(FPR) + "\n")

    index=index+1
    f.close()




# def myhashs(s):
#     result=[]
#     for f in hash_function_list:
#         result.append(f(s))
#
#     return result

def myhashs(s):
    result=[]
    userid=int(binascii.hexlify(s.encode('utf8')), 16)
    #f(x)= (ax + b) % m or f(x) = ((ax + b) % p) % m
    for parameters in HASH_FUNCTIONS:#
        value=((parameters[0]*userid+parameters[1])%parameters[2])
        tmp=value%SIZE
        result.append(tmp)
   # print(result)
    return result


sc = SparkContext(master='local[*]', appName='weixin_HW5_task1')
start_time=time.time()

input_file_path="./data/users.txt"
stream_size=100
ask_num=60
output_file="./HW5_task1.csv"
f = open(output_file, "w+")
f.write("Time,FPR")
f.close()

bx=BlackBox()
#stream_users=bx.ask(input_file_path,stream_size)

for _ in range(ask_num):
    stream_users=bx.ask(input_file_path,stream_size)
    bloom_filter(stream_users)



end_time=time.time()
print("Duration:"+str(end_time-start_time))

#import itertools
# import math
# from random import sample
#
# from pyspark import SparkContext
# from blackbox import BlackBox
# import time
# import binascii
# import datetime
# from itertools import combinations
# import pyspark
# import sys
# import os
#
#
# SIZE=69997
# HASH_FUNCTIONS= [[11, 49, 1543], [4, 801, 24593], [17, 81, 6151], [387, 509, 98317], [3, 36, 193] , [41, 196, 393242] ,  \
#                 [19, 144, 24593], [59, 757, 7873], [5, 16, 389], [443, 119, 196613], [11, 311, 6299], [37, 225, 786433],\
#                 [31, 121, 12289], [127, 487, 7691],[1, 25, 769], [39, 53, 4363],[37, 53, 5651],[19, 100, 12289],\
#                 [41, 193, 786437],[43, 227, 196611],[71, 111,1229],[5, 25, 193]]
#
# index=0
# true_negative = 0
# false_positive = 0
# user_set=set()
# filter_array=[0 for _ in range(SIZE)]
#
#
#
#
# # def myhashs(s):
# #     result=[]
# #     for f in hash_function_list:
# #         result.append(f(s))
# #
# #     return result
#
# def myhashs(s):
#     result=[]
#     userid=int(binascii.hexlify(s.encode('utf8')), 16)
#     #f(x)= (ax + b) % m or f(x) = ((ax + b) % p) % m
#     for parameters in HASH_FUNCTIONS:#
#         value=((parameters[0]*userid+parameters[1])%parameters[2])
#         tmp=value%SIZE
#         result.append(tmp)
#    # print(result)
#     return result
#
#
# sc = SparkContext(master='local[*]', appName='weixin_HW5_task1')
# start_time=time.time()
#
# input_file_path="./data/users.txt"
# stream_size=100
# ask_num=30
# output_file="./HW5_task1.csv"
# #input_file_path=sys.argv[1]
# #stream_size=int(sys.argv[2])
# #ask_num=int(sys.argv[3])
# #output_file=sys.argv[4]
#
# f = open(output_file, "w+")
# f.write("Time,FPR")
# f.close()
#
# bx=BlackBox()
# stream_users=bx.ask(input_file_path,stream_size)
#
# for _ in range(ask_num):
#     stream_users=bx.ask(input_file_path,stream_size)
# #    global index
#   #  global true_negative
#   #  global false_positive
#     users=stream_users
#
#     for item in users:
#         tag = True
#         result = myhashs(item)
#
#         # print(myhashs(item))
#         id = int(binascii.hexlify(item.encode('utf8')), 16)
#
#         for i in result:
#             if filter_array[i] == 0:  #
#                 tag = False
#                 filter_array[i] = 1
#
#         if (tag):
#             if id not in user_set:
#                 user_set.add(id)
#                 false_positive = false_positive + 1
#                 continue
#         # else:
#         user_set.add(id)
#         true_negative = true_negative + 1
#
#     FPR = 0
#     sum = true_negative + false_positive
#     if sum > 0:
#         ack_sum = float(sum)
#         FPR = false_positive / ack_sum
#
#     f = open(output_file, "a")
#     # f.write(str(index))
#     # #print(index)
#     # f.write(",")
#     # f.write(str(FPR))
#     # f.write("\n")
#     f.write(f"\n{index},{FPR}")
#
#     # print(str(index) + "," + str(FPR) + "\n")
#
#     index = index + 1
#     f.close()
#
#
#
# end_time=time.time()
# print("Duration:"+str(end_time-start_time))
