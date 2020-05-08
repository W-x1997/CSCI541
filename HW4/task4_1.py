import time
import os
import sys
from functools import reduce
from pyspark.sql.functions import col, lit, when
from pyspark import SparkContext
from pyspark.sql import SQLContext
import time
from collections import defaultdict
from pyspark.sql import SparkSession, Row
from graphframes import *



#os.environ["PYSPARK_SUBMIT_ARGS"] = ( "--packages graphframes:graphframes:0.6.0-spark2.3")



# test
# g = Graphs(sqlContext).friends()  # Get example graph
#
# # Run PageRank until convergence to tolerance "tol".
# results = g.pageRank(resetProbability=0.15, tol=0.01)
# # Display resulting pageranks and final edge weights
# # Note that the displayed pagerank may be truncated, e.g., missing the E notation.
# # In Spark 1.5+, you can use show(truncate=False) to avoid truncation.
# results.vertices.select("id", "pagerank").show()
# results.edges.select("src", "dst", "weight").show()
#
# # Run PageRank for a fixed number of iterations.
# results2 = g.pageRank(resetProbability=0.15, maxIter=10)
#
# # Run PageRank personalized for vertex "a"
# results3 = g.pageRank(resetProbability=0.15, maxIter=10, sourceId="a")
#
# # Run PageRank personalized for vertex ["a", "b", "c", "d"] in parallel
# results4 = g.parallelPersonalizedPageRank(resetProbability=0.15, sourceIds=["a", "b", "c", "d"], maxIter=10)


def data_prcocess(input_path):
    raw_rdd=sc.textFile(input_path).map(lambda line:line.split())
    raw_rdd=raw_rdd.map(lambda line:(line[0],line[1]))
    #rdd=raw_rdd.flatMap(lambda item:((int(item[0]), int(item[1])), (int(item[1]), int(item[0]))))
    return raw_rdd

def write_file(res,output_path):
    with open(output_path, 'w') as f:
        for item in res:
            f.write('\'' + '\', \''.join(item) + '\'' )
            f.write('\n')

def getEdges(data_edges):
    edges_list = list()
    for index in range(len(data_edges)):
        edges_list.append(data_edges[index])
        edges_list.append(data_edges[index][::-1])
    edges_list1 = sqlContext.createDataFrame(edges_list, ["src", "dst"])
    return edges_list1,edges_list


def getVertices(data): #data-->edges
    vertices=set()
    for edge in data:
        vertices.add((edge[0], edge[0]))
        vertices.add((edge[1], edge[1]))
    vertices=list(vertices)
    vertices=sqlContext.createDataFrame(vertices, ["id", "name"])
    return    vertices

start_time=time.time()
#conf = SparkConf().setAppName('weixin_HW_4_task1').setMaster("local[*]")
#sc = SparkContext(conf=conf)

sc = SparkContext('local[*]', 'HW4_task1_weixin')

input_file_path="./data/power_input.txt"
output_file_path="./task4_3.txt"
sc.setLogLevel("WARN")
sqlContext = SQLContext(sc)

rdd_edges=data_prcocess(input_file_path)

data_edges=rdd_edges.collect()
edges,tmp=getEdges(data_edges)
vertices=getVertices(tmp)


#
#data_rdd=data_prcocess(input_file_path)
#all_vertices=data_rdd.map(lambda item:item[0]).distinct()
# vertices=all_vertices.map(lambda key:Row(id=key))
# vertices=vertices.toDF()
# edges=data_rdd.toDF(["src","dst"])
#
#
graph=GraphFrame(vertices,edges)
tmp= graph.labelPropagation(maxIter=5)
# result=result.collect()
tmp = tmp.toLocalIterator()
dict_result=defaultdict(list)

for line in tmp:
    label=line[2]
    name=line[1]
    dict_result[label].append(name)


res=list()
for key in dict_result.keys():
    res.append(sorted(dict_result[key]))
    res.sort(key=lambda item: (len(item), item[0]))

#
# for line in result:
#     key=line["label"]
#     if key not in dict_result.keys():
#         dict_result[key]=[]
#     item=str(line["id"])
#     dict_result[key].append(item)


#res_sort=sorted(dict_result,key=lambda x:(len(dict_result[x]),min(dict_result[x])))

write_file(res,output_file_path)
end_time=time.time()
print("Duration:"+str(end_time-start_time))
