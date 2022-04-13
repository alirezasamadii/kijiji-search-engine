import kshingle as ks
from datasketch import MinHash
from  datasketch import MinHashLSH
#########################################################################################
#################### takes list of paths to documents that wants to evaluate them #######
#########################################################################################
def inputs(paths):
    s = Shingler()
    list_of_shingles = []  # list of set shingles for each path
    for path in paths:
        with open(path) as file:
            shingle_and_hash = s.compute_shingles(file)  # computes shingles and hashes
            #print(shingle_and_hash[0])  # prints shingles of file
            #print(shingle_and_hash[1])  # prints hashes of shingles of file
            list_of_shingles.append((shingle_and_hash, path))
    return list_of_shingles # returns a list with "tuples for each path/document". second iindex of tuple is the path of the document
                            # and first element of tuple is a list made of lists of shingles and hashes of that document
                            # ex. list of shingles=[  (  [[shingles of doc],[hashes of shingles]],path  )  ]



class Shingler:
    def compute_shingles(self, file):
        lines = file.readlines()
        doc_shingle=[]
        hash_of_shingles=[]
        for line in lines:
            shingles = ks.shingleset_k(line,k=10)
            for shingle in shingles:
                if len(shingle) == 10:
                    doc_shingle.append(shingle)
                    hash_of_shingles.append(hash(shingle))
        return (doc_shingle,hash_of_shingles)

class Minhashing:
    def mh(self, list_of_shingles):
        mh_dict = {}
        for x in list_of_shingles:
            for y in list_of_shingles:
                m1, m2 = MinHash(), MinHash() # uses SHA1 as default hash function
                shingles_and_hashes_of_y = y[0]
                path_of_y = y[1]
                shingles_of_y = shingles_and_hashes_of_y[0]
                set_of_shingles_of_y = set(shingles_of_y)

                shingles_and_hashes_of_x = x[0]
                path_of_x = x[1]
                shingles_of_x = shingles_and_hashes_of_x[0]
                set_of_shingles_of_x = set(shingles_of_x)

                for d in set_of_shingles_of_x:
                    m1.update(d.encode('utf8'))
                mh_dict[path_of_x]=m1
                for d in set_of_shingles_of_y:
                    m2.update(d.encode('utf8'))
                mh_dict[path_of_y]=m2
                if path_of_x != path_of_y:
                    print("Estimated Jaccard for " , path_of_x , "and " , path_of_y , "is:" , m1.jaccard(m2))
                    print("Actual Jaccard for data1 and data2 is", float(len(set_of_shingles_of_y.intersection(set_of_shingles_of_x))) / float(
                        len(set_of_shingles_of_x.union(set_of_shingles_of_y))))
        return mh_dict


class LSH: #locality sensitive hashing
    def compute_LSH(self, mh_dict):
        lsh=MinHashLSH(threshold=0.8, num_perm=128)
        for key in mh_dict:
            lsh.insert(key,mh_dict[key])
        for key in mh_dict:
            print("Approximate neighbours with Jaccard similarity larger than 80%  to document ",key ," is ",lsh.query(mh_dict[key]))

if __name__ == "__main__":
    paths = ['joyce.txt', 'joyce2.txt','joyce3.txt','joyce4.txt']
    list_of_shingles = inputs(paths)
    print("list of shingles created")
    mwh = Minhashing()

    mh_dict = mwh.mh(list_of_shingles)
    print(mh_dict)
    lsh=LSH()
    lsh.compute_LSH(mh_dict)
