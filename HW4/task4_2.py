import itertools
import math
import operator
from random import sample
import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
import json
import time

import sys
import os

from pyspark.sql import SparkSession


def data_prcocess(input_path):
    raw_rdd=sc.textFile(input_path).map(lambda line:line.split(" "))
    raw_rdd=raw_rdd.map(lambda line:(line[0],line[1]))
    #rdd=raw_rdd.flatMap(lambda item:((int(item[0]), int(item[1])), (int(item[1]), int(item[0]))))
    return raw_rdd


def getcommunities(vertices,G):
    candidates=list()
    tmp=vertices.copy()

    while len(tmp):
        candidate=getcommunity(tmp.pop(),G)
        data=list(candidate)
        data=sorted(data)       ##
        candidates.append(data)
        tmp = tmp.difference(candidate)
    return candidates


def write_file_1(output_file,data):
    with open(output_file, "w") as f:
        for item in data:
            f.write(f"{item[0]}, {item[1]}\n")




def graph_BFS(G,head_node):
    s_path = dict()
    visited = list()
    relation=dict() #  父子关系结点  x的父结点
    level = dict()
    queue=list()
    visited.append(head_node)
    queue.append(head_node)

    for node in G.keys():
        relation[node]=list()
        level[node]=-1
        s_path[node]=0
    level[head_node]=0
    s_path[head_node]=1

    while len(queue):  # queue
        node=queue.pop(0) #pop 1st element
        subnodes=G[node]
        visited.append(node)
        for x in subnodes:
            if level[x] == -1 :
                queue.append(x)
                level[x]=level[node]+1
            if level[x]==level[node]+1:
                s_path[x]=s_path[x]+s_path[node]
                relation[x].append(node)

    return s_path,relation,visited


def getEdges(data_edges):
    edges_list = list()
    for index in range(len(data_edges)):
        edges_list.append(data_edges[index])
        edges_list.append(data_edges[index][::-1])
    #edges_list1 = sqlContext.createDataFrame(edges_list, ["src", "dst"])
    return edges_list


def getVertices(data): #data-->edges
    vertices=set()
    for edge in data:
        vertices.add((edge[0], edge[0]))
        vertices.add((edge[1], edge[1]))
    vertices=list(vertices)
    #vertices=sqlContext.createDataFrame(vertices, ["id", "name"])
    return    vertices

def getcommunity(vertex,G):
    queue=list()
    queue.append(vertex)
    candidates=set()

    while len(queue):
        node=queue.pop(0)
        candidates.add(node)
        for item in G[node]:
            if item not in candidates:
                queue.append(item)
    return candidates

def write_file_2(output_file,data):
    with open(output_file, "w") as f:
        for item in data:
            f.write("'" + "', '".join(item) + "'\n")






#sc = SparkContext(master='local[*]', appName='weixin_HW4_task2')
spark = SparkSession.builder.master("local[*]").appName("weixin_HW4_task2").getOrCreate()
sc = spark.sparkContext
start_time=time.time()
input_file="./data/power_input.txt"
output_file_1="./task4_11.txt"
output_file_2="./task4_22.txt"

# input_file=sys.argv[1]   #<input_file_path>
# output_file_1=sys.argv[2] #<betweenness_output_file_path>
# output_file_2=sys.argv[3]  #<community_output_file_path>
#


rdd=data_prcocess(input_file)
edges=rdd.collect()

v1=rdd.map(lambda node:node[0]).collect()
v2=rdd.map(lambda node:node[1]).collect()
vertices=set(v1+v2)    #顶点集合



G=dict() #graph


for edge in edges:
    if edge[0] in G.keys():
        G[edge[0]].add(edge[1])
    else:
        G[edge[0]] = set()
        G[edge[0]].add(edge[1])
    if edge[1] in G.keys():
        G[edge[1]].add(edge[0])
    else:
        G[edge[1]] = set()
        G[edge[1]].add(edge[0])



betweenness=dict()
for node in vertices: # run BFS from each node
    s_path, relation, visited=graph_BFS(G,node)
    weight_edge=dict()
    weight_vertice={node:1 for node in visited}
    for node2 in visited[::-1]:
        for root in relation[node2]:
            items=min(node2,root),max(node2,root)
            percent=s_path[root]/s_path[node2]
            value=percent*weight_vertice[node2]
            if items  in weight_edge.keys():
                weight_edge[items]=weight_edge[items]+value
            else:
                weight_edge[items]=0
                weight_edge[items] = weight_edge[items] + value
            weight_vertice[root]=weight_vertice[root]+value

    for e,val in weight_edge.items():
        if e in betweenness.keys():
            betweenness[e]=betweenness[e]+val/2
        else:
            betweenness[e] = 0
            betweenness[e] = betweenness[e] + val / 2

betweenness=sorted(betweenness.items(),key=operator.itemgetter(1),reverse=True)# 定义自定义函数，获取对象的第1个域的值 即betweenness
data1=betweenness


n_edges=len(betweenness)
sum_modular_max = -1
communities_max = list() # our final res list
G2=G.copy()
betweenness2=betweenness.copy()

while len(betweenness2):
    sum_modular=0.0
    pairs=getcommunities(vertices,G2) # community计算
    for candidate in pairs:
        for item in candidate:
            for item2 in candidate:
                k = 1.0 if item2 in G[item] else 0.0
                n2=len(G[item2])
                n1=len(G[item])
                sum_modular+=k-n1*n2/(n_edges*2)
    sum_modular=sum_modular/(n_edges*2)

    if sum_modular_max<sum_modular:
        communities_max=pairs
        sum_modular_max=sum_modular
        print(str(sum_modular_max)+"\n")
    betweenness_max_value = max([x for _, x in betweenness2])
    edges_delete=[k for k, v in betweenness2 if v == betweenness_max_value]
    for item in edges_delete:
        G2[item[0]] = {x for x in G2[item[0]] if x != item[1]}
        G2[item[1]] = {x for x in G2[item[1]] if x != item[0]}


    betweenness_tmp = dict()
    for node in vertices:  # run BFS from each node
        s_path, relation, visited = graph_BFS(G2, node)
        weight_edge = dict()
        weight_vertice = {node: 1 for node in visited}
        for node2 in visited[::-1]:
            for root in relation[node2]:
                items = min(node2, root), max(node2, root)
                percent = s_path[root] / s_path[node2]
                value = percent * weight_vertice[node2]
                if items in weight_edge.keys():
                    weight_edge[items] = weight_edge[items] + value
                else:
                    weight_edge[items] = 0
                    weight_edge[items] = weight_edge[items] + value
                weight_vertice[root] = weight_vertice[root] + value

        for e, val in weight_edge.items():
            if e in betweenness_tmp.keys():
                betweenness_tmp[e] = betweenness_tmp[e] + val / 2
            else:
                betweenness_tmp[e] = 0
                betweenness_tmp[e] = betweenness_tmp[e] + val / 2

    betweenness_tmp= sorted(betweenness_tmp.items(), key=operator.itemgetter(1),reverse=True)  # 定义自定义函数，获取对象的第1个域的值 即betweenness

    betweenness2 = betweenness_tmp




communities_max.sort(key=lambda item: (len(item), item[0]))

write_file_1(output_file_1,data1)
write_file_2(output_file_2,communities_max)


end_time=time.time()
print("Duration:"+str(end_time-start_time))