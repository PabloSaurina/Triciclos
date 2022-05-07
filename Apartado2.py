from pyspark import SparkContext
import sys

def mapper(line):
    edge = line.split(',')
    n1 = edge[0]
    n2 = edge[1]
    return [(n1,n2), (n2,n1)]

SAMPLE = 15

sc = SparkContext()

rdd1 = sc.textFile(sys.argv[1])
print('textFile1', rdd1.take(SAMPLE))

rdd2 = sc.textFile(sys.argv[2])
print('textFile2', rdd2.take(SAMPLE))

rdd = rdd2.union(rdd1)
print('mergedTextFiles',rdd.take(SAMPLE))

rdd = rdd.flatMap(mapper)
print('flatMap', rdd.take(SAMPLE))

rdd = rdd.filter(lambda x: x[0]!=x[1])
print('filter', rdd.take(SAMPLE))

rdd = rdd.distinct()
print('distinct', rdd.take(SAMPLE))

rdd = rdd.groupByKey()
print('groupByKey', rdd.take(SAMPLE))

rdd = rdd.map(lambda x: (x[0],tuple(sorted(x[1]))))
print('map', rdd.take(SAMPLE))
lista = []
for i,j in rdd.collect():
    for k,l in rdd.collect():
        if k in j and i in l:
            for m in j:
                if m in l:
                    lista.append(tuple(sorted([i,k,m])))
print(lista)
res = sc.parallelize(lista)
res = res.distinct()
print('res',res.take(SAMPLE))
print("Result:")
for i in res.collect():
    print("Los vertices",i,"forman un 3-ciclo.")
