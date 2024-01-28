# -*- coding: utf-8 -*-
# PJ: interesting_fact_code
# Author: Junjie Xing Github: @GavinXing

import pickle
from typing import Any

from sklearn.cluster import KMeans
import numpy as np


QUERY_PICKLE_FILE = "PATH_TO_THE_PICKLE_FILE"
N_CLUSTERS = 100  # number of clusters


def normalize_list(two_column: Any) -> list[tuple]:
    """
    Normalize the list of tuples
    """
    filter_none = []
    for row in two_column:
        if row[1] is not None:
            row = (row[0], float(row[1]))
            filter_none.append(row)
    filter_none.sort(key=lambda x: x[1], reverse=True)
    sum_ = sum([float(row[1]) for row in filter_none])
    return [(x, y / sum_) for (x, y) in filter_none]


def get_normalized_value(query_result, k=10):
    """
    Get the normalized value of the query result
    """
    normalized = normalize_list(query_result)
    values = [x[1] for x in normalized][:k]
    values = values + [0] * max(0, k - len(values))
    return values


with open(QUERY_PICKLE_FILE, "rb") as infile:
    queries = pickle.load(infile)

queries_with_feature = []

special = []
for _id, query in enumerate(queries):
    try:
        queries_with_feature.append({
            "id": _id,
            "features": get_normalized_value(query.result, k=10),
            "query": query
        })
    except ZeroDivisionError:
        continue

features = [x['features'] for x in queries_with_feature]
features = np.array(features)

kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=0).fit(features)

for _id, query in enumerate(queries_with_feature):
    queries_with_feature[_id]["cluster_id"] = int(kmeans.labels_[id])

clusters = {}
for query in queries_with_feature:
    if query["cluster_id"] not in clusters:
        clusters[query["cluster_id"]] = []
    clusters[query["cluster_id"]].append(query)

with open("PATH_TO_SAVE_CLUSTERS.pickle", "wb") as outfile:
    pickle.dump(clusters, outfile)
