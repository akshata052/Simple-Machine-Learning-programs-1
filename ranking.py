from elasticsearch import Elasticsearch
import math
INDEX_NAME = "aquaint"
DOC_TYPE = "doc"
QUERY_FILE = "data/queries.txt"  # make sure the query file exists on this location
OUTPUT_FILE = "data/baseline.txt"  # output the ranking

def load_queries(query_file):
    queries = {}
    with open(query_file, "r") as fin:
        for line in fin.readlines():
            qid, query = line.strip().split(" ", 1)
            queries[qid] = query
    return queries

queries = load_queries(QUERY_FILE

def analyze_query(es, query):
    tokens = es.indices.analyze(index=INDEX_NAME, body={"text": query})["tokens"]
    query_terms = []
    for t in sorted(tokens, key=lambda x: x["position"]):
        query_terms.append(t["token"])
    return query_terms



def score_bm25(es, qterms, doc_id):
    # Total number of documents in the index\n",
    N = es.count(index=INDEX_NAME, doc_type=DOC_TYPE)["count"]
    # Getting term frequency statistics for the given document field from Elasticsearch
    tv = es.termvectors(index=INDEX_NAME, doc_type=DOC_TYPE, id=doc_id, fields=["content"], 
                        term_statistics=True).get("term_vectors", {}).get("content", {})
    dl = sum([s["term_freq"] for t, s in tv["terms"].items()])  # length of the document\n",
    cl = tv["field_statistics"]["sum_ttf"]  # collection length (total number of terms in a given field in all documents)\n",
    avg_dl = cl / N
    k1 = 1.2
    b = 0.75
    
    s = 0  # holds the retrieval score
    for t in qterms:
        if t in tv["terms"]:
            df_t = tv["terms"][t]["doc_freq"]  # number of docs in the collection that contain that #term
            f_td = tv["terms"][t]["term_freq"]  # raw frequenct of t in d (number of times term t #appears in doc d)\n",
            t_tC = tv["terms"][t]["ttf"]  # frequency of t in the entire collection
    
            w_tq = 1
            idf_t = math.log(N / df_t, 2)
            B = 1 - b + b * dl / avg_dl
            w_td = (f_td * (k1+1)) / (f_td + k1 * B) * idf_t
            s += w_tq * w_td
        print(s)
    return s



def main():


es = Elasticsearch()
queries = {}
with open(QUERY_FILE, "r") as fin:
    for line in fin.readlines():
        qid, query = line.strip().split(" ", 1)
        queries[qid] = query


with open(OUTPUT_FILE, "w") as fout:
    # write header
    fout.write("QueryId,DocumentId\n")
    for qid, query in queries.items():
        # get top 100 docs using BM25
        print("Get baseline ranking for [%s] '%s'" % (qid, query))
        res = es.search(index=INDEX_NAME, q=query, df="content", _source=False, size=	100).get('hits', {})
        
        print("Scoring documents and writing top 100 to file: ")
        # get analyzed query
        qterms=analyze_query(es,query)       
        scores = {}
        for doc in res.get("hits", {}):
            doc_id = doc.get("_id")
            scores[doc_id] = score_bm25(es, qterms, doc_id)

        # write top 100 results to file
        for doc_id, score in sorted(scores.items(), key=lambda x: x[1], reverse=True)[:100]:            
            fout.write(qid + "," + doc_id + "\n")
            
 if __name__ == "__main__":
    main()
    
