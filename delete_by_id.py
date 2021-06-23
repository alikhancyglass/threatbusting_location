from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk
import json

# 1. Ping ElasticSearch Instance


def get_escl(url):
    escl = Elasticsearch(hosts=url, timeout=300)
    if escl.ping():
        return escl
    return False


url = "https://cyglass:cyglass@devfonex1.cyglass.com:9200/"
escl = get_escl(url)

index = 'user_location_check_test2'
doc_type = 'cyglass'

search_param = {
                "query": {
                    "bool": {
                    "must": [
                        {
                        "match": {
                            "site_name": 'devstratjmo365'
                        }
                        },
                        {
                        "match": {
                            'location_type': 'normal'
                        }
                        }
                    ]
                    }
                }
                }

delete_ids = []
for doc in scan(client=escl, index=index, query=search_param):
    delete_id = doc['_id']
    delete_ids.append(delete_id)
print(len(delete_ids))   

for id in delete_ids:
    escl.delete(index=index, doc_type=doc_type, id=id)

check_ids = []
for doc in scan(client=escl, index=index, query=search_param):
    delete_id = doc['_id']
    check_ids.append(delete_id)

print(len(check_ids), ': should be less than 10')