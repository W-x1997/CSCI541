import itertools

from pyspark import SparkContext
from pyspark import SparkConf
import json
import time
import pyspark
import sys
import os



def generate_baskets(input):
    opt_num_of_partition=1
    if support//8 >1:
        opt_num_of_partition=support//8
    rdd=sc.textFile(input)
    first_line = rdd.first()
    rdd = rdd.filter(lambda line: line != first_line)
    rdd = rdd.map(lambda entry: entry.split(",")).map(lambda x: (x[0] + "-" + x[1]).replace("\"", "") + "," + str(int(x[5].replace("\"", "")))) #'2/28/2001-01719888,4713080610231'
    #print(table.collect())
    baskets=rdd.map(lambda line: [(line.split(',')[0]), (line.split(',')[1])]).groupByKey().mapValues(list).values().filter(lambda x: len(x)>filter_threshold)#map(list).persist()
    num_partitions = opt_num_of_partition
    #print(num_partitions)
    #print(baskets.collect())
    return baskets,num_partitions


def count_in_pass(block,baskets):
    res_frequent_items=[]
    Item_counts={}
    for candidate in block:
        for basket in baskets:
            if candidate.issubset(basket):
                if candidate in Item_counts:
                    Item_counts[candidate]=Item_counts[candidate]+1
                else:
                    Item_counts[candidate]=1

    for key,val in Item_counts.items():
        if val >= support:
            res_frequent_items.append(set(key))
    return res_frequent_items


def judge(Item):
    if Item[1]>=support:
        return True
    return False



def count_map(basket):
    freq_res =[]
    for candidate in candidates_keys:
        if set(candidate).issubset(basket):
            freq_res.append((candidate, 1))
    return freq_res




def A_priorty(block):       #此方法一定要在generate baskets之后调用
    block_support=support/num_partitions #当前chunk中的support
    frequent_items=[]     #frequent candidates 总集合
    prev_frequent=[]   #用来计算下一次的不同集合
    baskets_block=[]  #所有的baskets
    Item_counts={}
    loop = 2  # 循环  也是表示candidate的个数
    for candidate in block:
       baskets_block.append(candidate)
       for item in candidate:
            if item not in Item_counts:
                Item_counts[item] = 1
            else:
                Item_counts[item] = Item_counts[item] + 1

    for item , times in Item_counts.items():
        if block_support <= times:
            prev_frequent.append(frozenset([item]))
            frequent_items.append(frozenset([item]))
    while True:
        if len(prev_frequent)<=0:
            break
        #计算出 loop个元素的frequent items
        Item_counts2 = {} #每次都要重新初始化
        candidates = set()
        #print(prev_frequent)
        for tmp in itertools.combinations(prev_frequent, 2):
            #print("tmp:")
            #print(tmp)
            #print("Loop"+str(loop))
            #print("tmp[0]"+str(tmp[0]))
            #print("tmp[1]" + str(tmp[1]))
            total = tmp[0].union(tmp[1])  # 合并
            #print("total" + str(total))
            if loop == len(total):
                candidates.add(frozenset(total))  # 这里一定要用frozenset  因为frozenset才有哈希值 后面才可以用union
                # set无序排序且不重复，是可变的，有add（），remove（）等方法。既然是可变的，所以它不存在哈希值


        for candidate in candidates:
            for basket_Item in baskets_block:
                if candidate.issubset(basket_Item):
                    if candidate not in Item_counts2:
                        Item_counts2[candidate] = 1
                    else:
                        Item_counts2[candidate] = Item_counts2[candidate] + 1

        frequent_set = []     #frequent_set 即为每一次  长度为loop的frequent items
        for item, times in Item_counts2.items():
            if times >= block_support:
                frequent_set.append(item)

        prev_frequent=frequent_set   #prev_frequen
        #
        frequent_items=frequent_items+prev_frequent #bug!!! found here  frequent_sets_local      !!frequent_items=frequent_items.append(prev_frequent) is wrong!
        loop=loop+1

    return frequent_items



sc = SparkContext(master='local[*]', appName='weixin_HW2_task1')
filter_threshold=20
support=50
input_file="./ta_feng_all_months_merged.csv"
output_file='./task2_output.csv'
#filter_threshold=int(sys.argv[1])
#support=int(sys.argv[2])
#input_file=sys.argv[3]
#output_file=sys.argv[4]

start_time=time.time()

# 第一阶段 第一次pass the whole data
baskets,num_partitions=generate_baskets(input_file)

#print(baskets.collect())
candidates_rdd = baskets.repartition(num_partitions).mapPartitions(A_priorty)
#candidates_rdd = baskets.repartition(numPartitions=num_partitions).mapPartitions(A_priorty) #每一个分区都是一个block
candidates_rdd=candidates_rdd.map(lambda item: (tuple(sorted(list(item))), 1))
candidates_keys=candidates_rdd.reduceByKey(lambda a, b: 1).keys().collect()
candidates_keys=sorted(candidates_keys,key=lambda x:(len(x),x))
#print(candidates_keys)
with open(output_file, "w") as f:
    line=""
    f.write("Candidates:\n")
    cur_row= 1
    for candidate in candidates_keys:
        if len(candidate)!=cur_row:
            cur_row = cur_row + 1
            f.write(line[0:-1] + '\n\n')  # 最后一个， delete
            line = str(candidate) + ','
        else:
            if (cur_row != 1):
                line = line + str(candidate) + ','
            else:
                line = line + '(\'' + candidate[0] + '\'),'   # '  需要加\'

    if len(line)>0:
            f.write(line[0:-1]+'\n\n')

#第二阶段 第二次pass the whole data
res_frequent_Items_rdd= baskets.flatMap(count_map).reduceByKey(lambda count1, count2: count1+count2) ##flatMap
res_frequent_Items=res_frequent_Items_rdd.filter(lambda candidate:candidate[1]>=support).keys().collect()
res_frequent_Items=sorted(res_frequent_Items,key=lambda x:(len(x),x))
#print(res_frequent_Items)
with open(output_file, "a") as f:  #追加
    line=""
    f.write("Frequent Itemsets:\n")
    cur_row= 1
    for candidate in res_frequent_Items:
        if len(candidate)!=cur_row:
            cur_row = cur_row + 1
            f.write(line[0:-1] + '\n\n')  # 最后一个， delete
            line = str(candidate) + ','
        else:
            if (cur_row != 1):
                line = line + str(candidate) + ','
            else:
                line = line + '(\'' + candidate[0] + '\'),'   # '  需要加\'

    if len(line)>0:
            f.write(line[0:-1]+'\n\n')

end_time=time.time()
Duration=end_time-start_time

#print("=============================================")
print("Duration: "+str(Duration))
