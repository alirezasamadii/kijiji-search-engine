import csv
import json
from itertools import count
from os import write
import nltk
from nltk.corpus import stopwords
from nltk.util import pr
from numpy import cos, vectorize
stop_words = set(stopwords.words('italian'))

from nltk.tokenize import word_tokenize
import string
nltk.download('stopwords')
nltk.download('punkt')

from nltk.stem.snowball import SnowballStemmer
snow_stemmer = SnowballStemmer(language='italian')

import simplemma
langdata = simplemma.load_data('it')

from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer

###############################################################################################
################ STOPWORD FILTER ##############################################################
###############################################################################################

def stopword_filter(sentence):
    word_tokens = word_tokenize(sentence)
    filtered_sentence = [w for w in word_tokens if w not in string.punctuation and w not in stop_words]
    filtered_sentence_text=" ".join(filtered_sentence).lower()
    return filtered_sentence

###############################################################################################
################  lemmatizer  ###################################################################
###############################################################################################
def lemmatizer(filtered_sentence):
    lemmatized=[]
    for word in filtered_sentence:
       lemmatized.append(simplemma.lemmatize(word.lower(), langdata))
    return " ".join(lemmatized)
#########################################################################################
############# query on inverted index ############################################################
#################################################################################
def query(inverted_index):
    query_dict={}
    print("################################################## ")
    print("################################################## ")
    query=input("insert you query and press ENTER: ")
    print("################################################## ")
    print("################################################## ")
    query=stopword_filter(query)
    query=lemmatizer(query)
    for word in query.split():
        if word in inverted_index:
            query_dict.update({word:inverted_index[word]})
    return (query_dict,query)
#########################################################################################
############## comparing similarity of input and inverted index ##################
##########################################################################
def input_index_similarity(space,query_value):
    vectorizer =  CountVectorizer()
    space.append(query_value)
    vector=vectorizer.fit_transform(space)
    vector_array=vector.toarray()
    co_sim=cosine_similarity(vector_array)
    space.pop()      
    return co_sim[-1]
###############################################################################################
################  MAIN  ###################################################################
###############################################################################################

if __name__=='__main__':
    
    tsv_file = open("records.tsv")
    read_tsv = csv. reader(tsv_file, delimiter="\t")
    inverted_index={}
    space=[]
    links=[]
    for row in read_tsv:
        if len(row)>1 :
            filtered_sentence = stopword_filter(row[0])
            output_title=lemmatizer(filtered_sentence)

            filtered_sentence = stopword_filter(row[1])
            output_description=lemmatizer(filtered_sentence)
            links.append(row[4])
            space.append(output_title+" "+output_description)

            for word in (output_title+" "+ output_description).split():           ### creating inverted index based on
                if word not in inverted_index:                 
                    inverted_index.update({word:[]})
                # add link of this title to this key
                if row[4] not in inverted_index[word]:
                    inverted_index[word].append(row[4])
    while(True):
        returned=query(inverted_index)
        query_dict=returned[0]
        query_value=returned[1]

        if not bool(query_dict):
            print(" NO RESULTS FOUND ")
        else:
            #print(query_dict)
            final_list=[] # the smiliarity values
            results_links_and_similarity=[]
            final_list=input_index_similarity(space,query_value).tolist()  
            for  i in range(0,len(final_list)-1):
                if final_list[i]>0:   
                    results_links_and_similarity.append((links[i],final_list[i]))   
            sortedresults = sorted(results_links_and_similarity, key=lambda tup: tup[1], reverse=True)
            for ad in sortedresults:
                print(ad)
            results_links_and_similarity=[]
                        # take the title of it and link of it and show them by order

        # f = open("inverted_index.json", 'w')
        #  index_json = json.dump(inverted_index,f)