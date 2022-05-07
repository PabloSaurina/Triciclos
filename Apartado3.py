from pyspark import SparkContext
import sys
from multiprocessing import Process

def mapper(line):
    edge = line.split(',')
    n1 = edge[0]
    n2 = edge[1]
    return [(n1,n2), (n2,n1)]

SAMPLE = 15

sc = SparkContext()


def task(ide,rdd,sc):
    rdd = rdd.flatMap(mapper)
#    print('flatMap', rdd.take(SAMPLE))
    rdd = rdd.filter(lambda x: x[0]!=x[1])
#    print('filter', rdd.take(SAMPLE))
    rdd = rdd.distinct()
#    print('distinct', rdd.take(SAMPLE))
    rdd = rdd.groupByKey()
#    print('groupByKey', rdd.take(SAMPLE))
    rdd = rdd.map(lambda x: (x[0],tuple(sorted(x[1]))))
#    print('map', rdd.take(SAMPLE))
    lista = []
    for i,j in rdd.collect():
        for k,l in rdd.collect():
            if k in j and i in l:
                for m in j:
                    if m in l:
                        lista.append(tuple(sorted([i,k,m])))
#    print(lista)
    res = sc.parallelize(lista)
    res = res.distinct()
#    print('res',res.take(SAMPLE))
    for i in res.collect():
        print("Los vertices",i,f"forman un 3-ciclo en el archivo {ide}.")


def main():
    lp = []
    n = len(sys.argv)
    for i in range( n- 1):
        rdd = sc.textFile(sys.argv[i+1])
        print(f'textFile {i+1}', rdd.take(SAMPLE))
        lp.append(Process(target=task,args = (i+1,rdd,sc)))
    for p in lp:
        p.start()
    for p in lp:
        p.join()

if __name__ == '__main__':
    main()
