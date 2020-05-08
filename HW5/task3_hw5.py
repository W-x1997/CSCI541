import itertools
import math
from random import sample

from blackbox import BlackBox
import time
import binascii

from itertools import combinations

import sys
import os
import random


random.seed(553)
# index = 0
# true_negative = 0
# false_positive = 0
list_user = list()
num_of_users = 0




def writeItem(output, data, numofusers):
    with open(output, "a") as f:
        f.write(str(numofusers) + ",")
        f.write(str(data[0]) + ",")
        f.write(str(data[20]) + ",")
        f.write(str(data[40]) + ",")
        f.write(str(data[60]) + ",")
        f.write(str(data[80]))
        f.write("\n")


# sc = SparkContext(master='local[*]', appName='weixin_HW5_task3')
start_time = time.time()

input_file_path="./data/users.txt"
stream_size=100
ask_num=30
output_file="./HW5_task3_test.csv"


# input_file_path = sys.argv[1]
# stream_size = int(sys.argv[2])
# ask_num = int(sys.argv[3])
# output_file = sys.argv[4]

f = open(output_file, "w+")
f.write("seqnum,0_id,20_id,40_id,60_id,80_id")
f.write("\n")
f.close()

bx = BlackBox()
#stream_users = bx.ask(input_file_path, stream_size)

for _ in range(ask_num):
    stream_users = bx.ask(input_file_path, stream_size)
    users = stream_users
    for user in users:
        num_of_users=num_of_users+1
        if num_of_users <= 100:
            list_user.append(user)
            if num_of_users%100==0:
                writeItem(output_file,list_user,num_of_users)
            continue

        random_val = (random.randint(0, 100000))%num_of_users
        if random_val < 100:
            random_position=(random.randint(0, 100000))%100
            list_user[random_position] = user

        if num_of_users % 100 == 0:
            writeItem(output_file, list_user, num_of_users)
        #
        # num_of_users = num_of_users + 1
        #
        # if 100 >= num_of_users:  # >=
        #     list_user.append(user)
        #     if num_of_users % 100 == 0:
        #         writeItem(output_file, list_user, num_of_users)
        #
        #     continue
        #
        # random_val = random.randint(0, num_of_users)
        # if random_val < 100:
        #     n_random_position = (random.randint(0, 100000)) % 100
        #     list_user[n_random_position] = user
        #
        #
        # if num_of_users % 100 == 0:
        #     writeItem(output_file, list_user, num_of_users)

end_time = time.time()
print("Duration:" + str(end_time - start_time))


