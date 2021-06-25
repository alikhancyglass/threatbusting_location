from __future__ import division
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk, parallel_bulk
from user_location_event import get_escl, get_user_location_schema, create_index, generator_docs
import json
import numpy as np
from collections import deque
from datetime import datetime

USER_ID = "smadhushree@corelogic.com"
REM_LOCALITY = "Quincy"
SRC_SITE = 'esb'
RAW_LOGIN_INDEX = "rawappidaccmgt_v1"
DOC_TYPE = "cyglass"
GMM_INDEX = "saas_gmm_table_v1"

TEST_USER_ID = 'jaubin@bankhometown.com'

def get_user_docs(escl, user_id, rem_locality):
    user_docs = []
    search_param = {
                "query": {
                    "bool": {
                    "must": [
                        {
                        "match": {
                            "user_id": user_id
                        }
                        },
                        {
                        "match": {
                            'rem_locality': rem_locality
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
    for doc in scan(client=escl, index=RAW_LOGIN_INDEX, query=search_param):
        user_doc = {
                    'user_id': USER_ID,
                    'rem_locality': REM_LOCALITY,
                    'rem_latitude': doc['_source']['rem_latitude'],
                    'rem_longitude': doc['_source']['rem_longitude'],
                    'timestamp': doc['_source']['timestamp'],
                    'datetime': datetime.fromtimestamp(doc['_source']['timestamp']/1000),
                    'operation': str(doc['_source']['operation'])
                   }
        user_docs.append(user_doc)
    return user_docs

def get_models(escl, event_timestamp):
    models = []
    search_param = {
        'query': {'match': {'doc_type': 'models'}}}
    for doc in scan(client=escl, index=GMM_INDEX, query=search_param):
        if (doc['_source']['end_time'] >= event_timestamp) and (not doc['_source']['state'] == 'EXPIRED'):
            model = {
                    "curr_page_id": str(doc['_source']["curr_page_id"]),
                    'model_id': str(doc['_source']['model_id']),
                    'doc_type': str(doc['_source']['doc_type']),
                    'state': str(doc['_source']['state']),
                    'start_time': doc['_source']['start_time'],
                    'end_time': doc['_source']['end_time']
                    }
            models.append(model)
    return models

def get_features(escl, feature_data_id):
    feature_docs = []
    search_param = {
        'query': {'match': {'doc_type': 'feature_data'}}}
    for doc in scan(client=escl, index=GMM_INDEX, query=search_param):
        if doc['_id'] == feature_data_id:
            feature_doc = {
                            'doc_type': 'feature_data',
                            '_id': doc['_id'],
                            'user_id': doc['_source']['user_id']
                          }
            feature_docs.append(feature_doc)
    return feature_docs

def analyze_user(escl, USER_ID, models):
    res = "NOT FOUND IN MODELS"
    all_results = []
    for model in models:
        feature_data_id = model['curr_page_id']
        feature_docs = get_features(escl, feature_data_id)
        for feature_doc in feature_docs:
            for user_id in feature_doc['user_id']:
                if str(user_id) == USER_ID:
                    all_results.append((model['state'], str(user_id)))
    return all_results


def main():
    src_url = "https://cyglass:cyglass@"+SRC_SITE+".cyglass.com:9200/"
    src_escl = get_escl(src_url)
    user_docs = get_user_docs(src_escl, USER_ID, REM_LOCALITY)
    print(user_docs)
    print('#################')
    models = get_models(src_escl, user_docs[0]['timestamp'])
    print(models)
    print(len(models))
    print('#################')
    feature_docs = get_features(src_escl, models[0]['curr_page_id'])
    print(feature_docs)
    print('#################')
    user_results = analyze_user(src_escl, USER_ID, models)
    print(user_results)

if __name__ == "__main__":
    main()

    