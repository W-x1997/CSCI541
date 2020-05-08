
import itertools
import math
from random import sample
from operator import add

import numpy as np
from pyspark import SparkContext
from pyspark import SparkConf
import json
import time
from sklearn.cluster import KMeans
from itertools import combinations
import pyspark
import sys
import os

def data_process(input_file_path): # 第一步 process 20% data  随机去百分之20的数据
    p = 0.2
    aug_cluster=CLUSTER_NUM*10
    rdd=sc.textFile(input_file,None, False)
    rdd=rdd.map(lambda item: item.decode().split(',')).map(lambda item: list(map(eval,item[2:])))

    all_data=rdd.collect()
    all_data_size = len(all_data)

    data = np.array(all_data[:int(p*all_data_size)])

    data_index= np.arange(int(p*all_data_size))
    return data,data_index,aug_cluster,all_data_size,all_data

def K_means(num_cluster,data):
    res = KMeans(n_clusters=num_cluster, random_state=0).fit(data)
    return res


def writeFile(output_file,data,ds_set):
    f= open(output_file, 'w')
    line= "The intermediate results:\n"
    size=len(data)
    for x in range(size):
        item=data[x]
        line=line+"Round "+str(x+1)+": "
        line=line+str(item[0]) + ","
        line=line+str(item[1]) + ","
        line=line+str(item[2]) + ","
        line=line+str(item[3]) + "\n"
    ss="\nThe clustering results:\n"
    line=line+ss
    size=len(ds_set)

    for k in range(size):
        tmp=int(ds_set[k])
        item = str(k) + "," + str(tmp)
        item=item+"\n"
        line=line+item
    f.write(line)
    f.close()

input_file = "./data/hw6_clustering.txt"
CLUSTER_NUM = 10
output_file ="./HW6_wx.txt"


# The intermediate results:
# Round 1:64444,6,12,6
# Round 2:125650,17,3240,34
# Round 3:186494,16,6844,48
# Round 4:247161,16,10620,67
# Round 5:307686,24,14553,71
# Round 6:322212,30,69,31
#
# The clustering results:
# 0,8
# 1,0
# 2,1
# 3,4

sc = SparkContext(master='local[*]', appName='weixin_HW6_task')
sc.setLogLevel("ERROR")
start_time=time.time()

p = 0.2
ds_n=0
cs_n = 0

N_ds=[]
N_cs=[]

cs_centre=[]

rs_set=[]

SSQ_ds=[]
cs_SSQ=[]

cs_set_index=[]
rs_set_index=[]


tmp_mid_data=[]

#第一步
data,data_index,aug_cluster,all_data_size,all_data=data_process(input_file)


kmeans_res = K_means(aug_cluster,data) # 第二步 选largeK 运行kmeans
index_list=[]


keys = np.unique(kmeans_res.labels_)

for x in keys:
    fg= (x== kmeans_res.labels_ )
    tmp_arr=kmeans_res.labels_[fg]
    size= tmp_arr.size
    if 30>size:
        index= np.argwhere(x==kmeans_res.labels_ )
        for u in index:
            rs_set_index.append(u[0])
            rs_set.append(data[u][0])


data = np.delete(data, rs_set_index, axis=0)   #delete the data we used      第三部移除rs
rs_set = np.array(rs_set)

data_index = np.delete(data_index, rs_set_index)
rs_set_index = np.array([i for i in rs_set_index])

#第4步 对其他点运行Kmeans
kmeans_res=K_means(CLUSTER_NUM,data)



#第5步生成ds
keys = np.unique(kmeans_res.labels_)
ds_set=-np.ones(all_data_size)

for x in keys:
    index = np.argwhere(x==kmeans_res.labels_ ).flatten()
    N_ds.append([index.size])
    ds_n=ds_n+index.size

    value = np.power(data[index], 2)  ##value transfer
    value2 = np.sum(value, axis=0)
    SSQ_ds.append(value2)

    base = data_index[index] - 1
    ds_set[base] = x


SSQ_ds=np.array(SSQ_ds)
N_ds=np.array(N_ds)
ds_centre=kmeans_res.cluster_centers_
num1=SSQ_ds/N_ds
ds_s=ds_centre*N_ds

num2=np.power(ds_centre,2)
num=num1-num2
ds_sv=np.sqrt(num)


###有问题！！
# for k in keys_Kmeans3:
#     inx = np.argwhere(Kmeans_res_3.labels_ == k).flatten().tolist()
#     if len(inx) > 1:
#         cs_n += len(inx)
#         cs_set_index.append(rs_set_index[inx].tolist())
#         N_cs.append([len(inx)])
#         cs_SSQ.append(np.sum(np.power(rs_set_index[inx], 2), axis=0))
#         cs_centre.append(Kmeans_res_3.cluster_centers_[k])
#         index_list =index_list+inx
#

temp_cluster = int(rs_set_index.size * 0.7)#第六步 RS中运行大的kmeans
kmeans_res = K_means(temp_cluster,rs_set)

keys = np.unique(kmeans_res.labels_)
for item in keys:
    index = np.argwhere(item==kmeans_res.labels_).flatten()
    index=index.tolist()
    val=len(index)
    if 1<val:  #index change
        num1 = np.power(rs_set[index], 2)
        num2 = np.sum(num1, axis=0)
        index_list = index_list + index
        cs_SSQ.append(num2)
        cs_n=cs_n+val
        N_cs.append([val])
        tt=rs_set_index[index].tolist() ##
        cs_centre.append(kmeans_res.cluster_centers_[item])
        cs_set_index.append(tt)


N_cs=np.array(N_cs)
cs_centre=np.array(cs_centre)
cs_SSQ=np.array(cs_SSQ)

cs_s=cs_centre * N_cs  #sum

num1=cs_SSQ / N_cs
num2=np.power(cs_centre, 2)
#fftp=num1-num2 #
num3=num1-num2
tmp_sv=np.array(num3)
tmp_sv=np.where(0 == tmp_sv, 0.00000001, tmp_sv)
cs_sv=np.sqrt(tmp_sv)

rs_set=np.delete(rs_set, index_list, axis=0)
rs_set_index=np.delete(rs_set_index, index_list)


val1=len(cs_set_index)
val2=rs_set_index.size
tmp_mid_data.append((ds_n, val1, cs_n, val2))



#第七步 随机加载另外百分之20的data
p=0.2 # or 0.01
begin=int(all_data_size*p)
tmp_2=all_data_size*p
end=begin+int(tmp_2)

while all_data_size>begin:
    len_cs_set_index = len(cs_set_index)
    tmp_cs = [[] for i in range(len_cs_set_index)]
    tmp_ds = [[] for i in range(10)]

    if all_data_size<end:
        end=all_data_size
    #data2_len = end - begin
    arr=all_data[begin:end]
    #data2=np.array(arr)
    data2=np.array(all_data[begin:end])

    for k in range(end-begin):
        num1 = ds_centre - data2[k]   # 第8-10步 compare
        num1 = np.abs(num1)
        ds_t = num1 / ds_sv
        ds_m_l = np.argmax(np.bincount(ds_t.argmin(axis=0)))
        ds_t = ds_t[ds_m_l]
        flag = np.argwhere(2.8 < ds_t).size
        if flag != 0:
            cs_t = np.abs(cs_centre - data2[k]) / cs_sv
            cs_m_l = np.argmax(np.bincount(cs_t.argmin(axis=0)))
            cs_t = cs_t[cs_m_l]

            flag2 = np.argwhere(cs_t > 2).size
            if flag2!= 0:   #
                rs_set_index = np.append(rs_set_index, k + begin)  # change recover
                rs_set = np.r_[rs_set, np.array([data2[k]])]
            else:
                cs_n = cs_n + 1
                tmp_cs[cs_m_l].append(data2[k])
                cs_set_index[cs_m_l].append(k + begin)
        else:
            ds_set[k + begin] = ds_m_l
            ds_n = ds_n + 1
            tmp_ds[ds_m_l].append(data2[k])


    for item in range(10):
        tp = np.array(tmp_ds[item])
        num11 = np.power(tp, 2)
        # 一轮完成之后更新
        SSQ_ds[item] = SSQ_ds[item] + np.sum(num11, axis=0)
        ds_s[item] = ds_s[item] + np.sum(tp, axis=0)



    ds_ttt=np.array([[len(x)] for x in tmp_ds])
    ds_centre=ds_s/N_ds
    N_ds=N_ds+ds_ttt


    ds_centre = ds_s/N_ds
    ds_sv = np.sqrt((SSQ_ds / N_ds) - np.power(ds_centre, 2))


    len_cs_set_index2 = len(cs_set_index)
    for k in range(len_cs_set_index2):
        value_tmp = np.array(tmp_cs[k])
        value_tmp2 = np.power(value_tmp, 2)
        cs_SSQ[k] = cs_SSQ[k] + np.sum(value_tmp2, axis=0)
        cs_s[k] = cs_s[k] + np.sum(value_tmp, axis=0)


    cs_ttt=np.array([[len(x)] for x in tmp_cs])
    num1 = cs_SSQ / N_cs
    N_cs=N_cs+cs_ttt
    num2 = np.power(cs_centre, 2)
    cs_centre=cs_s/N_cs
    #num1=cs_SSQ/N_cs

    sizeofRS_index = rs_set_index.size

    tmp_sv=np.array(num1-num2)
    tmp_sv=np.where(0==tmp_sv ,0.00000001,tmp_sv)

    cs_sv = np.sqrt(cs_sv)


    #11 再运行大的Kmeans
    list_merge = list() #
    if sizeofRS_index != 0:
        list_index = []
        Kmeans_res=K_means(min(int(rs_set_index.size * 0.7),CLUSTER_NUM * 10),rs_set)
        keys_Kmeans4 = np.unique(Kmeans_res.labels_)
        for key in keys_Kmeans4:
            index = np.argwhere(key == Kmeans_res.labels_).flatten().tolist()
            lenofindex = len(index)
            if 1 < lenofindex:  ##
                cs_n = cs_n + lenofindex
                tmpp = rs_set_index[index].tolist()
                cs_set_index.append(tmpp)
                N_cs = np.r_[N_cs, np.array([[len(index)]])]
                num1 = np.power(rs_set[index], 2)
                tmp_sq = np.array([np.sum(num1, axis=0)])  #
                cs_SSQ = np.r_[cs_SSQ, tmp_sq]

                tmp_sum = np.array([np.sum(rs_set[index], axis=0)])
                cs_s = np.r_[cs_s, tmp_sum]

                tmp_centre = np.array(tmp_sum/len(index))
                cs_centre = np.r_[cs_centre, tmp_centre]

                num3 = tmp_sq/len(index)
                num4 = np.power(tmp_centre, 2)
                tmp_sv = num3 - num4
                tmp_sv = np.where(0 == tmp_sv, 0.00000001, tmp_sv)
                cs_sv = np.r_[cs_sv, np.sqrt(tmp_sv)]

                list_index = list_index + index

        rs_set_index = np.delete(rs_set_index, list_index)
        rs_set = np.delete(rs_set, list_index, axis=0)



    for x in range(len(cs_set_index)):  #12 merge
        for y in range(x + 1, len(cs_set_index)):
            d2 = np.abs(cs_centre[x] - cs_centre[y]) / cs_sv[y]
            d1 = np.abs(cs_centre[x] - cs_centre[y]) / cs_sv[x]

            val1 = np.argwhere(d1 > 3).size
            val2 = np.argwhere(d2 > 3).size
            if val2 == 0 or 0 == val1:
                list_merge.append([x, y])

    #merge_len = len(list_merge)
    len_of_mergelist = len(list_merge)
    if len(list_merge) != 0:
        for a in range(len_of_mergelist):
            for b in range(len_of_mergelist):

                num2 = len(list_merge[a]) + len(list_merge[b])
                set1 = set(list_merge[a] + list_merge[b])
                list1 = list(set(list_merge[a] + list_merge[b]))
                # list1 = list(set1)

                val = len(list1)
                if b == a:
                    break
                elif num2 > val:
                    list_merge[a] = list1
                    list_merge[b] = [-1]


        list_merge = [x for x in list_merge if x != [-1]]

        for l in list_merge:
            tmp_index = []
            for item in l:
                tmp_index = tmp_index + cs_set_index[item]
            cs_set_index.append(tmp_index)

            tc = len(tmp_index)
            N_cs = np.r_[N_cs, np.array([[tc]])]  ##
            tmp_sum = np.array([np.sum(cs_s[l], axis=0)])
            tmp_sq = np.array([np.sum(cs_SSQ[l], axis=0)])

            cs_s = np.r_[cs_s, tmp_sum]
            cs_SSQ = np.r_[cs_SSQ, tmp_sq]
            cs_centre = np.r_[cs_centre, tmp_sum / len(tmp_index)]
            num1 = tmp_sq / len(tmp_index)
            num2 = np.power(tmp_sum / len(tmp_index), 2)
            num3 = num1 - num2
            cs_sv = np.r_[cs_sv, np.sqrt(num3)]

        remove_item = sorted(sum(list_merge, []), key=int, reverse=True)
        for i in remove_item:
            del cs_set_index[i]

        cs_centre = np.delete(cs_centre, remove_item, axis=0)
        cs_s = np.delete(cs_s, remove_item, axis=0)
        N_cs = np.delete(N_cs, remove_item, axis=0)
        cs_sv = np.delete(cs_sv, remove_item, axis=0)
        cs_SSQ = np.delete(cs_SSQ, remove_item, axis=0)


    if all_data_size == end:
        remove_item = []
        r = len(cs_set_index)
        for x in range(r):
            cha=ds_centre- cs_centre[x]
            cha=np.abs(cha) / ds_sv
            ds_t = cha
            f = np.bincount(ds_t.argmin(axis=0))
            ds_m_l = np.argmax(f)
            ds_t = ds_t[ds_m_l]
            jugde = np.argwhere(3 < ds_t).size
            if 0 == jugde:
                remove_item.append(x)
                times = len(cs_set_index[x])
                cs_n = cs_n - times
                ds_n = ds_n + times
                ds_set[cs_set_index[x]] = ds_m_l
                # times = len(cs_set_index[x])

        remove_item = sorted(remove_item, key=int, reverse=True)

        for item in remove_item:
            del cs_set_index[item]

    length = len(cs_set_index)
    begin = end
    end = begin + int(all_data_size * p)
    length2 = rs_set_index.size
    res_data = (ds_n, length, cs_n, length2)

    tmp_mid_data.append(res_data)


#写入文件
writeFile(output_file,tmp_mid_data,ds_set)



end_time=time.time()
print("Duration"+str(end_time-start_time))

