from pyspark import SparkContext
import sys
from multiprocessing import Process

def mapper(line):
    edge = line.split(',')
    n1 = edge[0][1:-1]
    n2 = edge[1][1:-1]
    return [(n1,n2), (n2,n1)]

SAMPLE = 15

sc = SparkContext()


def task(file):
    rdd = sc.textFile(file)
    print('textFile', rdd.take(SAMPLE))
    rdd = rdd.flatMap(mapper)
    print('flatMap', rdd.take(SAMPLE))
    rdd = rdd.filter(lambda x: x[0]!=x[1])
    print('filter', rdd.take(SAMPLE))
    rdd = rdd.distinct()
    print('distinct', rdd.take(SAMPLE))
    rdd = rdd.groupByKey()
    print('groupByKey', rdd.take(SAMPLE))
    rdd = rdd.map(lambda x: (tuple(sorted(x[1])), x[0]))
    print('map', rdd.take(SAMPLE))
    rdd = rdd.groupByKey()
    print('Result:')
    resultados = False
    for adyacentes, nodos in rdd.collect():
        vertices = list(nodos)
        if len(vertices)>1:
            resultados = True
            print("vertices", list(nodos), "has common adjacents", adyacentes)
    if not resultados:
        print ('No vertices with common adjacents, try another graph!')


def main():
    lp = []
    for i in range(len(sys.argv) - 1):
       	file = sys.argv[i+1]
        lp.append(Process(target=task,args = (file)))
    for p in lp:
        p.start()
    for p in lp:
        p.join()

if __name__ == '__main__':
    main()
