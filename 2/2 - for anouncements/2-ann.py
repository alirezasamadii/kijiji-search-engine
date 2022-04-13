import kshingle as ks
from datasketch import MinHash
from  datasketch import MinHashLSH
import csv
from tqdm import tqdm


####################################################################################################
########################## 1. computing shingles with length k=10 ##################################
####################################################################################################
class Shingler:
    length_of_shingle=10
    def compute_shingles(self, adv):
        doc_shingle=[]
        hash_of_shingles=[]
        shingles = ks.shingleset_k(adv,k=self.length_of_shingle)
        for shingle in shingles:
            if len(shingle) == self.length_of_shingle:
                doc_shingle.append(shingle)
                hash_of_shingles.append(hash(shingle))
        return (doc_shingle,hash_of_shingles)    ### it not only returns shingles, but also hash of shignles of each doc



########################################################################################################
########################################################################################################
class Minhashing:
    def mh(self, list_of_shingles):
        mh_dict = {}
        for xi in tqdm(range(len(list_of_shingles)-1),"loading..........."):
            x=list_of_shingles[xi]
            for yi in range(xi+1,len(list_of_shingles)-1):
                y=list_of_shingles[yi]
                m1, m2 = MinHash(), MinHash() # uses SHA1 as default hash function
                shingles_and_hashes_of_y = y[0]
                title_of_y = y[1]
                shingles_of_y = shingles_and_hashes_of_y[0]
                set_of_shingles_of_y = set(shingles_of_y)

                shingles_and_hashes_of_x = x[0]
                title_of_x = x[1]
                shingles_of_x = shingles_and_hashes_of_x[0]
                set_of_shingles_of_x = set(shingles_of_x)

                for d in set_of_shingles_of_x:
                    m1.update(d.encode('utf8'))
                mh_dict[title_of_x]=m1
                for d in set_of_shingles_of_y:
                    m2.update(d.encode('utf8'))
                mh_dict[title_of_y]=m2
               # if path_of_x != path_of_y:
                  #  print("Estimated Jaccard for " , path_of_x , "and " , path_of_y , "is:" , m1.jaccard(m2))
                  #  print("Actual Jaccard for data1 and data2 is", float(len(set_of_shingles_of_y.intersection(set_of_shingles_of_x))) / float(
                     #   len(set_of_shingles_of_x.union(set_of_shingles_of_y))))
        return mh_dict

##############################################################################################
##############################################################################################
##############################################################################################
class LSH:
    def compute_LSH(self, mh_dict):
        lsh=MinHashLSH(threshold=0.8, num_perm=128)
        for key in mh_dict:
            lsh.insert(key,mh_dict[key])
        print("LSH min hashes computed... now doing query... please wait")    
        print("Approximate neighbours with Jaccard similarity larger than 80%  to documents: ")
        for key in mh_dict:
            final_array=(lsh.query(mh_dict[key]))
            final_array.remove(key)
            if len(final_array)>0:
                print(key ," ----> ",final_array,"# duplicates ----> ", len(final_array))
################################################################################################
################################################################################################
################################################################################################

def inputs(path):
    s = Shingler()
    list_of_shingles = []  # list of set shingles for each path
    file = open(path)
    advs = csv. reader(file, delimiter="\t")
    for adv in advs:
        if len(adv)>0:
            shingle_and_hash = s.compute_shingles(adv[1])  # computes shingles and hashes
                #print(shingle_and_hash[0])  # prints shingles of file
                #print(shingle_and_hash[1])  # prints hashes of shingles of file
            list_of_shingles.append((shingle_and_hash, adv[0]))

    return list_of_shingles # returns a list with "tuples for each path/document". second iindex of tuple is the path of the document
                            # and first element of tuple is a list made of lists of shingles and hashes of that document
                            # ex. list of shingles=[  (  [[shingles of doc],[hashes of shingles]],path  )  ]
################################################################################################
################################################################################################
################################################################################################

if __name__ == "__main__":
    path = 'records.tsv'    
    list_of_shingles = inputs(path)
    print("list of shingles completed..... Min wisehashing in progress... please wait...")
    mwh = Minhashing()
    mh_dict = mwh.mh(list_of_shingles)
    print(" minwise hashing completed")
    lsh=LSH()
    print("LSH object created please wait......")
    lsh.compute_LSH(mh_dict)
    print("completed")
    
