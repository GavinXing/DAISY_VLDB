# -*- coding: utf-8 -*-
# PJ: interesting_fact_code
# Author: Junjie Xing Github: @GavinXing

import pickle
import psycopg2

from objects import Query, GropedQuery

conn = psycopg2.connect(database='postgres', user="postgres")

cur = conn.cursor()

TABLE = "austin_crime"
cats = ["census_tract", "clearance_date", "clearance_status",
        "district", "primary_type", "year", "zipcode"]
nums = ["x_coordinate", "y_coordinate", "*"]

values = {}
for col in cats:
    get_distinct_col = f"""
    SELECT DISTINCT {col} from {TABLE};
    """

    cur.execute(get_distinct_col)
    rst = cur.fetchall()
    value_set = [a[0] for a in rst]
    values[col] = value_set

AGGs = ["COUNT", "SUM", "AVG"]

queries_no_where = []
# no where
for dimension in cats:
    for numerical in nums:
        for agg in AGGs:
            if numerical == "*" and agg != "COUNT":  # escape other aggs for *
                continue
            query_info = {
                "table": TABLE,
                "dc": dimension,
                "agg": agg,
                "mc": numerical,
                "where": None
            }
            query = Query(query_info)
            queries_no_where.append(query)


grouped_queries_dict = {}
queries_where = []
# with where
for dimension in cats:
    for numerical in nums:
        for agg in AGGs:
            where_dcs = [x for x in cats if dimension != x]
            for where_col in where_dcs:
                for where_value in values[where_col]:
                    if numerical == "*" and agg != "COUNT":  # escape other aggs for *
                        continue
                    query_info = {
                        "table": TABLE,
                        "dc": dimension,
                        "agg": agg,
                        "mc": numerical,
                        "where": {
                            "dc": where_col,
                            "value": where_value
                        }
                    }
                    query = Query(query_info)
                    # get grouped query
                    grouped_query_info = {
                        "table": TABLE,
                        "dc": dimension,
                        "agg": agg,
                        "mc": numerical,
                        "where_dc": where_col
                    }
                    if GropedQuery.hash(grouped_query_info) in grouped_queries_dict:
                        pass
                    else:
                        grouped_queries_dict[GropedQuery.hash(
                            grouped_query_info)] = GropedQuery(grouped_query_info)
                    query.grouped_query = grouped_queries_dict[GropedQuery.hash(
                        grouped_query_info)]
                    queries_where.append(query)


RUN = True
if RUN:
    for query in queries_no_where:
        query.execute(disableGroupedQuery=False)

if RUN:
    for grouped_query in grouped_queries_dict.values():
        grouped_query.execute()


if RUN:
    for it, query in enumerate(queries_where):
        query.execute(disableGroupedQuery=False)


all_queries = queries_no_where + queries_where
if RUN:
    with open(f"{TABLE}_all_query_rst.pickle", "wb") as outfile:
        pickle.dump(all_queries, outfile)
