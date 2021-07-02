from __future__ import division
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk, parallel_bulk
from user_location_event import get_escl, get_user_location_schema, create_index, generator_docs
import json
import numpy as np
from collections import deque
from datetime import datetime
import math


USER_ID = "vbendzuweit@fonex.com"
REM_LOCALITY = "Szczecin"
REM_COUNTRY = "Poland"
SRC_SITE = 'devfonex1'
RAW_LOGIN_INDEX = "rawappidaccmgt"
DOC_TYPE = "cyglass"
GMM_INDEX = "saas_gmm_table"

def get_user_docs(escl, user_id, rem_locality, rem_country):
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
                            'rem_country': rem_country
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
                    'rem_country': REM_COUNTRY,
                    'rem_latitude': doc['_source']['rem_latitude'],
                    'rem_longitude': doc['_source']['rem_longitude'],
                    'timestamp': doc['_source']['timestamp'],
                    'datetime': datetime.fromtimestamp(doc['_source']['timestamp']/1000),
                    'operation': str(doc['_source']['operation'])
                   }
        user_docs.append(user_doc)
    print('Length of user_docs: ',len(user_docs))
    return user_docs

def get_model_docs(escl):
    models = []
    gmm_docs = []
    search_param = {
        'query': {'match': {'doc_type': 'models'}}}
    
    for doc in scan(client=escl, index=GMM_INDEX, query=search_param):
        if not doc['_source']['state'] == 'COLLECTING':
            gmm_doc = {
                        'model_id': str(doc['_source']['model_id']),
                        'start_time': doc['_source']['start_time'],
                        'end_time': doc['_source']['end_time'],
                        'state': str(doc['_source']['state']),
                        'means': doc['_source']['GMM_params']['means'],
                        'feat_scales':doc['_source']['GMM_params']['feat_scales'],
                        'feat_mins': doc['_source']['GMM_params']['feat_mins'],
                        'covariances': doc['_source']['GMM_params']['covariances']
                    }
            gmm_docs.append(gmm_doc)
    print('Detected: ', len(gmm_docs), 'gmm models for ', SRC_SITE)
    return gmm_docs

def unscale_gmm_docs(gmm_docs):
    unscaled_gmm_docs = []
    for idx, doc in enumerate(gmm_docs):
        unscaled_doc = {
            'model_id': doc['model_id'],
            'start_time': doc['start_time'],
            'end_time': doc['end_time'],
            'state': doc['state']
        }
        unscaled_means = (np.array(doc['means'])-np.array(doc['feat_mins']))/np.array(doc['feat_scales'])
        unscaled_means_lat_lon = [[i[2], i[3]] for i in list(unscaled_means)]
        unscaled_doc['unscaled_means_lat_lon'] = unscaled_means_lat_lon
        # unscaled_clust_feat_vars_K = GMM_model.covariances_[:, f_idx, f_idx] / math.pow(feat_scales[f_idx], 2)
        unscaled_stds_all = []
        for centroid in doc['covariances']:
            unscaled_var = np.array(centroid).diagonal() / (np.array(doc['feat_scales'])**2)
            unscaled_std = unscaled_var**0.5
            unscaled_stds_all.append(unscaled_std)
        unscaled_stds_lat_lon = [[i[2], i[3]] for i in unscaled_stds_all]
        unscaled_doc['unscaled_stds_lat_lon'] = unscaled_stds_lat_lon
        unscaled_gmm_docs.append(unscaled_doc)
    return unscaled_gmm_docs

def get_cluster_centroids(unscaled_gmm_docs):
    cluster_centroids = []
    for doc in unscaled_gmm_docs:
        for i in range(len(doc['unscaled_means_lat_lon'])):
            centroid = {
                'model_id': doc['model_id'],
                'start_time': doc['start_time'],
                'end_time': doc['end_time'],
                'state': doc['state']
            }
            centroid['lat_mean'] = doc['unscaled_means_lat_lon'][i][0]
            centroid['lon_mean'] = doc['unscaled_means_lat_lon'][i][1]
            centroid['lat_std'] = doc['unscaled_stds_lat_lon'][i][0]
            centroid['lon_std'] = doc['unscaled_stds_lat_lon'][i][1]
            cluster_centroids.append(centroid)
    return cluster_centroids

def get_z_score(user_doc, cluster_centroids):
    user_z_scores = []
    for cluster in cluster_centroids:
        lat_z_score = abs(user_doc['rem_latitude'] - cluster['lat_mean']) / cluster['lat_std']
        lon_z_score = abs(user_doc['rem_longitude'] - cluster['lon_mean']) / cluster['lon_std']
        temp = [cluster['model_id'],
                cluster['start_time'],
                cluster['end_time'],
                cluster['state'],
                lat_z_score + lon_z_score
            ]
        user_z_scores.append(temp)
    return sorted(user_z_scores, key=lambda x: x[-1])

def main():
    src_url = "https://cyglass:cyglass@"+SRC_SITE+".cyglass.com:9200/"
    src_escl = get_escl(src_url)
    user_docs = get_user_docs(src_escl, USER_ID, REM_LOCALITY, REM_COUNTRY)
    print(user_docs)
    print('##########################')
    gmm_docs = get_model_docs(src_escl)
    # print(gmm_docs[0])
    print('##########################')
    unscaled_docs = unscale_gmm_docs(gmm_docs)
    # print(unscaled_docs[0])
    print('##########################')
    cluster_centroids = get_cluster_centroids(unscaled_docs)
    # print(len(cluster_centroids))
    print(cluster_centroids[0]) 
    print('##########################')
    user_z_scores = get_z_score(user_docs[0], cluster_centroids)
    print(user_z_scores[:5])

if __name__ == "__main__":
    main()