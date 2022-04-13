import findspark
findspark.init()
import pyspark
import kshingle as ks

from datasketch import MinHash
sc=pyspark.SparkContext("local[*]")

def inputs(paths):
    rdd_by_path = {}
    list_of_shingles = []
    for path in paths:
        rdd=sc.textFile(path)
        shingle_rdd = rdd.flatMap(lambda row: ks.shingleset_k(row,k=10)).filter(lambda shingle: len(shingle) == 10)
        shingle_hash_rdd = shingle_rdd.map(lambda s: (s, hash(s)))
        rdd_by_path[path] = (shingle_rdd,shingle_hash_rdd)
    return rdd_by_path

def mh(rdd_by_path):
    mh_dict = {}     
    l=list(rdd_by_path.keys())
    for pathx in range(0,len(l)):
        for pathy in range(pathx +1,len(l)):
            m1 = MinHash()
            m2 = MinHash()
            print('pathx: ', l[pathx])
            print('pathy: ', l[pathy])
            tupx=rdd_by_path[l[pathx]]
            tupy=rdd_by_path[l[pathy]]
            tupx[0].map(lambda s: m1.update(s.encode('utf8'))).count()
            tupy[0].map(lambda s: m2.update(s.encode('utf8'))).count()
            mh_dict[pathx]=m1
            mh_dict[pathy]=m2               
    keys=list(mh_dict.keys())
    for i in range (0,len(keys)):
        for j in range (i+1,len(keys)):
            print("Estimated Jaccard for " , keys[i] , "and " , keys[j] , "is:" , mh_dict[keys[i]].jaccard(mh_dict[keys[j]]))
         #   print("Actu#al Jaccard for data1 and data2 is", float(len(set_of_shingles_of_y.intersection(set_of_shingles_of_x))) / float(
                     #   len(set_of_shingles_of_x.union(set_of_shingles_of_y))))
                                  
    
if __name__=="__main__":
    
    paths=["joyce.txt", "joyce2.txt","joyce3.txt", "joyce4.txt"]
    rdd_by_path=inputs(paths)
    print(rdd_by_path)
    mh_dict=mh(rdd_by_path)

