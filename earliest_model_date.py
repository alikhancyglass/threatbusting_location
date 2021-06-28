from __future__ import division
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk, parallel_bulk
from user_location_event import get_escl, get_user_location_schema, create_index, generator_docs
import json
import numpy as np
from collections import deque
from datetime import datetime

SRC_SITE = "devfonex1"
DOC_TYPE = "cyglass"
GMM_INDEX = "saas_gmm_table"

def get_earliest_fitted_model_date(escl, index=GMM_INDEX):
    fitted_model_dates = []
    search_param = {'query': {'match': {'doc_type': 'models'}}}

    for doc in scan(client=escl, index=index, query=search_param):
        if not doc['_source']['state'] == 'COLLECTING':
            fitted_model_dates.append(doc['_source']['end_time'])

    earliest_model = min(fitted_model_dates)
    earliest_model_datetime = datetime.fromtimestamp(earliest_model/1000)
    return earliest_model_datetime


def main():
    src_url = "https://cyglass:cyglass@"+SRC_SITE+".cyglass.com:9200/"
    src_escl = get_escl(src_url)
    print(get_earliest_fitted_model_date(src_escl))


if __name__ == "__main__":
    main()