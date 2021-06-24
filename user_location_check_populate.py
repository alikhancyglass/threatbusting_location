from __future__ import division
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk
import json


# 1. Ping ElasticSearch Instance


def get_escl(url):
    escl = Elasticsearch(hosts=url, timeout=300)
    if escl.ping():
        return escl
    return False


def create_index(escl, set_index, set_schema):
    """
    Setup  Elastic Search indices for test data
    :param set_index:
    :param set_schema:
    :return:
    """
    # create index in ES if indices do not exist
    if not escl.indices.exists(index=set_index):
        print "Detected no index by the name %s in Elasticsearch" % set_index
        try:
            escl.indices.create(index=set_index, body=set_schema, ignore=400)
            print "Created ES index %s ... " % set_index
        except Exception:
            print "Failed to create ES index %s ... " % set_index
            traceback.print_exc()
        print "Created schema for %s in Elasticsearch" % set_index


def run():
    # abnormal locations
    search_param = {
        'query': {'match': {'anomtype': 'Unusual Access Location For a User'}}}
    index_name = 'ml_event_v1'
    # + devfonex1
    # + stagingredteam2
    # + esb
    # + devstratjmo365
    # + hydro3
    # + schemmer  # empty set
    # + stratejm
    # + superior

    site_name = 'superior'
    new_index = 'user_location_check_test2'
    doc_type = 'cyglass'
    towrite_docs = []
    user_ids = set()
    # url = "https://cyglass:cyglass@devfonex1.cyglass.com:9200/"
    url = "https://cyglass:cyglass@"+site_name+".cyglass.com:9200/"
    escl = get_escl(url)

    for doc in scan(client=escl, index=index_name, query=search_param):
        target_doc = {'user_id': str(doc['_source']['endpoints'][0]['value']),
                      'location_type': 'anomaly',
                      'location': {'lat': doc['_source']['triggering_features_by_model']['main_triggering_feature']['value_location']['lat'],
                                   'lon': doc['_source']['triggering_features_by_model']['main_triggering_feature']['value_location']['lon']},
                      'lat_z_score': abs(doc['_source']['triggering_features_by_model']['main_triggering_feature']['value_location']['lat'] -
                                                   doc['_source']['triggering_features_by_model']['main_triggering_feature']['baseline_location']['lat']) / doc['_source']['triggering_features_by_model']['main_triggering_feature']['standard_deviation'][0],
                      'lon_z_score': abs(doc['_source']['triggering_features_by_model']['main_triggering_feature']['value_location']['lon'] -
                                                   doc['_source']['triggering_features_by_model']['main_triggering_feature']['baseline_location']['lon']) / doc['_source']['triggering_features_by_model']['main_triggering_feature']['standard_deviation'][1],
                      'site_name': site_name}

        target_doc_baseline = {'user_id': str(doc['_source']['endpoints'][0]['value']),
                               'location_type': 'baseline',
                               'location': {'lat': doc['_source']['triggering_features_by_model']['main_triggering_feature']['baseline_location']['lat'],
                                            'lon': doc['_source']['triggering_features_by_model']['main_triggering_feature']['baseline_location']['lon']
                                            },
                               'lat_diff': 0.0, 
                               'lon_diff': 0.0,
                               'site_name': site_name}

        towrite_docs.append(target_doc)
        towrite_docs.append(target_doc_baseline)
        user_ids.add(target_doc['user_id'])

    # print(len(towrite_docs))
    # print(towrite_docs)

    dst_escl = get_escl(url)
    # schema = {"mappings": {
    #     "cyglass": {
    #         "properties": {
    #             "area": {"type": "geo_shape"},
    #             "user_id": {"type": "keyword"},
    #             "location": {"type": "geo_point"},
    #             "location_type": {"type": "keyword"},
    #             "lat_z_score": {"type":"double"},
    #             "lon_z_score": {"type":"double"},
    #             "site_name": {"type": "keyword"}
    #         }
    #     }
    # }
    # }

    # create_index(dst_escl, new_index, schema)

    
    # # dst_escl.bulk(new_index, doc_type, towrite_docs, 10000)
    dst_site = 'devfonex1'
    dst_url = "https://cyglass:cyglass@"+dst_site+".cyglass.com:9200/"
    dst_escl = get_escl(dst_url)

    for doc in towrite_docs:
        dst_escl.index(index=new_index, doc_type=doc_type, body=doc)

    # Normal locations
    print(user_ids)
    print(len(user_ids))
    normal_loc_index = 'rawappidaccmgt_v1'
    normal_loc_docs = []
    for user_id in user_ids:
        # normal_loc_search_param = {
        #     'queries': {'match': {'user_id': user_id}},
        #                {'match': {'operation': 'UserLoggedIn'}}}

        normal_loc_search_param = search_param = {
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
                            'operation': 'UserLoggedIn'
                        }
                        }
                    ]
                    }
                }
                }
        for doc in scan(client=escl, index=normal_loc_index, query=normal_loc_search_param):
            normal_loc_doc = {'user_id': user_id,
                                   'location_type': 'normal',
                                   'location': {'lat': doc['_source']["rem_latitude"],
                                                'lon': doc['_source']["rem_longitude"]
                                                },
                                   'lat_diff': 0.0, 
                                   'lon_diff': 0.0,
                                   'site_name': site_name}
            normal_loc_docs.append(normal_loc_doc)

    print('---------------------------')
    print(len(normal_loc_docs))
    print(normal_loc_docs[0])


    for count, doc in enumerate(normal_loc_docs):
        dst_escl.index(index=new_index, doc_type=doc_type, body=doc)
        if count % 1000 == 0:
            print("Percentage completed: ", count/len(normal_loc_docs) * 100)

if __name__ == "__main__":
    run()
