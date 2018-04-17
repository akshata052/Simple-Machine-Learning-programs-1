from elasticsearch import Elasticsearch
import math
INDEX_NAME = "aquaint"
DOC_TYPE = "doc"
QUERY_FILE = "data/queries.txt"
OUTPUT_FILE = "data/mlm_default.txt"  # output the ranking
FIELDS = ["title", "content"]
FIELD_WEIGHTS = [0.2, 0.8]
LAMBDA = 0.1
def load_queries(query_file):
    queries = {}
    with open(query_file, "r") as fin:
        for line in fin.readlines():
            qid, query = line.strip().split(" ", 1)
            queries[qid] = query
    return queries

def analyze_query(es, query):
    tokens = es.indices.analyze(index=INDEX_NAME, body={"text": query})["tokens"]
    query_terms = []
    for t in sorted(tokens, key=lambda x: x["position"]):
        query_terms.append(t["token"])
    return query_terms

def score_mlm(es, clm, qterms, doc_id):
    score = 0  # log P(q|d)
 
    tv = es.termvectors(index=INDEX_NAME, doc_type=DOC_TYPE, id=doc_id, fields=FIELDS,
                              term_statistics=False).get("term_vectors", {})

    
    # scoring the query
    for t in qterms:
        Pt_theta_d = 0  # P(t|\theta_d)
        for i, field in enumerate(FIELDS):
            Pt_theta_di = 0
            F_q = 0
            
      
            hits = clm._es.search(index=INDEX_NAME, body={"query": {"match": {field: t}}},
                               _source=False, size=1).get("hits", {}).get("hits", {})
            #henter id fra matcher
            doc_id = hits[0]["_id"] if len(hits) > 0 else None
            print(doc_id)
            Pt_di=0
            if doc_id is not None:
                # ask for global term statistics when requesting the term vector of that doc (`term_statistics=True`)
                tv = clm._es.termvectors(index=INDEX_NAME, doc_type=DOC_TYPE, id=doc_id, fields=field,
                                      term_statistics=False)["term_vectors"][field]
                #frequency of term t in field f in a document
                tf = tv["terms"].get(t, {}).get("term_freq", 0)  # total term count in the collection (in that field)
                #number of terms in field f in a document.
                print(tf)
                ttf = sum([s["term_freq"] for t, s in tv["terms"].items()]) 
                print(ttf)
                #print(ttf)
                Pt_di=tf/ttf
                #print(Pt_di)
     
            Pt_Ci=clm.prob(field,t)
            print(Pt_Ci)
            Pt_theta_di = (1-LAMBDA)*Pt_di+LAMBDA*Pt_Ci
            #print(Pt_theta_di)


            
            Pt_theta_d += FIELD_WEIGHTS[i] * Pt_theta_di
        score += math.log(Pt_theta_d) #*F_q 
        print(score)
    
    return score


def main():


es = Elasticsearch()
queries = load_queries(QUERY_FILE)

with open(OUTPUT_FILE, "w") as fout:
    # write header
    fout.write("QueryId,DocumentId\n")
    for qid, query in queries.items():
        # get top 200 docs using BM25
        print("Get baseline ranking for [%s] '%s'" % (qid, query))
        res = es.search(index=INDEX_NAME, q=query, df="content", _source=False, size=200).get('hits', {})

        # re-score docs using MLM
        print("Re-scoring documents using MLM")
        # get analyzed query
        qterms = analyze_query(es, query)
        # get collection LM 
        # (this needs to be instantiated only once per query and can be used for scoring all documents)
        clm = CollectionLM(es, qterms)        
        scores = {}
        for doc in res.get("hits", {}):
            doc_id = doc.get("_id")
            scores[doc_id] = score_mlm(es, clm, qterms, doc_id)

    # write top 100 results to file
        for doc_id, score in sorted(scores.items(), key=lambda x: x[1], reverse=True)[:100]:            
            fout.write(qid + "," + doc_id + "\n")
            
         
 if __name__ == "__main__":
    main()
