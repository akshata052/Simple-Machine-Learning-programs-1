import re
import gzip
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from elasticsearch import helpers

INDEX_NAME = "aquaint"
DOC_TYPE = "doc"
INDEX_SETTINGS = {
    "settings" : {
        "index" : {
            "number_of_shards" : 1,
            "number_of_replicas" : 1
        }
    }
}

def add_docs_bulk(es, docs):
    actions = []
    for doc_id, doc in docs.items():
        action = {
            "_index": INDEX_NAME,
            "_type": DOC_TYPE,
            "_id": doc_id,
            "_source": doc
        }
        actions.append(action)

    if len(actions) > 0:
        helpers.bulk(es, actions)

def index(es, file_name):
    print("Processing", file_name)
    with gzip.open(file_name, "rt") as fin:
        is_body = False
        docs = {}
        doc_id, body = None, None
        for line in fin:
            line = line.strip()
            if line.startswith("<DOCNO>"):  # get doc id
                doc_id = re.sub("<DOCNO> | </DOCNO>", "", line)
            elif line.startswith("<BODY>"):  # start to parse body
                is_body = True
                body = []
            elif line.startswith("</BODY>"):  # finished reading body
                soup = BeautifulSoup("\n".join(body), "lxml")
                headline = soup.find("headline")
                text = soup.find("text")
                docs[doc_id] = {
                    "title": headline.text if headline is not None else "",  # use an empty string if no <HEADLINE> found
                    "content": text.text if text is not None else ""  # everything inside <TEXT> is indexed as content
                }
                # get ready for next document
                doc_id = None
                is_body = False
            elif is_body:  # accumulate body content
                body.append(line)

        # bulk index the collected documents
        print("Bulk indexing", len(docs), "documents")
        add_docs_bulk(es, docs)

def main():


es = Elasticsearch()
if not es.indices.exists(INDEX_NAME):
    es.indices.create(index=INDEX_NAME, body=INDEX_SETTINGS)

#INDEXING ALL FILES:
compath = "C:/Users/ap/aquaint/"
endpath=["apw/1998","apw/1999","apw/2000"]
for ep in endpath:
    mypath = [compath+ep]
#repeat the following for all the directories by setting mypath
    from os import listdir
    from os.path import isfile, join
    for m in mypath:
        onlyfiles = [f for f in listdir(m) if isfile(join(m, f))]
        for f in onlyfiles:
            if (f!=".DS_Store"):
                path=m+'/'
                path+=f
                index(es, path)
                path=''
    mypath=[compath-ep]
    
    
if __name__ == "__main__":
    main()
