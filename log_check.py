from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk
import json

# 1. Ping ElasticSearch Instance


def get_escl(url):
    escl = Elasticsearch(hosts=url, timeout=300)
    if escl.ping():
        return escl
    return False


url = "https://cyglass:cyglass@esb.cyglass.com:9200/"
escl = get_escl(url)

index = 'rawappidaccmgt_v1'
search_param = {
                "query": {
                    "bool": {
                    "must": [
                        {
                        "match": {
                            "user_id": 'sstrauss@performanceproc.com'
                        }
                        },
                        {
                        "match": {
                            'operation': 'UserLoggedIn'
                        }
                        }
                    ]
                    }
                }
                }

for doc in scan(client=escl, index=index, query=search_param):
    print(doc)
    print('#####')
    break
