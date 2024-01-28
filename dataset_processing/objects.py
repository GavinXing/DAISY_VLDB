# -*- coding: utf-8 -*-
# PJ: interesting_fact_code
# Author: Junjie Xing Github: @GavinXing

import json
import hashlib
import psycopg2


class GropedQuery:
    """
    Grouped Query
    """

    def __init__(self, info):
        self.table = info["table"]
        self.dimension = info["dc"]
        self.agg = info["agg"]
        self.measure = info["mc"]
        self.where_dimension = info["where_dc"]

        self.info = info

        self.result = None

    @staticmethod
    def hash(info):
        """
        Hash function
        """
        return hashlib.sha224(json.dumps(info, sort_keys=True).encode('utf-8')).hexdigest()

    def query_string(self):
        """
        Query string
        """
        rst = f"SELECT {self.dimension}, {self.where_dimension}, {self.agg}({self.measure})\nFROM {self.table}\nGROUP BY {self.dimension}, {self.where_dimension}\nORDER BY 3 DESC"
        return rst

    def execute(self, conn=psycopg2.connect(database='postgres', user="junjiexing")):
        # print("psql execute")
        cur = conn.cursor()
        cur.execute(self.query_string())
        self.result = cur.fetchall()
        cur.close()
        # print(len(self.result))

    def sub_query_result(self, value):
        return [(row[0], row[2]) for row in self.result if row[1] == value]


class Query:
    def __init__(self, info):
        self.table = info["table"]
        self.dimension = info["dc"]
        self.agg = info["agg"]
        self.measure = info["mc"]
        if info["where"]:
            self.where = {
                "dimension": info["where"]["dc"],
                "value": info["where"]["value"]
            }
        else:
            self.where = None
        self.result = None
        self.grouped_query = None

    def query_string(self):
        if self.where:
            where_dc = self.where["dimension"]
            rst = "SELECT {}, {}({})\nFROM {}\nWHERE {}={}\nGROUP BY {}\nORDER BY 2 DESC".format(self.dimension, self.agg, self.measure,
                                                                                                 self.table, where_dc,
                                                                                                 str(
                                                                                                     self.where["value"]),
                                                                                                 self.dimension)
        else:
            rst = "SELECT {}, {}({})\nFROM {}\nGROUP BY {}\nORDER BY 2 DESC".format(self.dimension, self.agg, self.measure, self.table,
                                                                                    self.dimension)
        return rst

    def query_to_execute(self):
        if self.where:
            where_dc = self.where["dimension"]
            rst = "SELECT {}, {}({})\nFROM {}\nWHERE {}=%s\nGROUP BY {}\nORDER BY 2 DESC".format(self.dimension, self.agg, self.measure,
                                                                                                 self.table, where_dc,
                                                                                                 self.dimension)
        else:
            rst = "SELECT {}, {}({})\nFROM {}\nGROUP BY {}\nORDER BY 2 DESC".format(self.dimension, self.agg, self.measure, self.table,
                                                                                    self.dimension)
        return rst

    def execute(self, conn=psycopg2.connect(database='postgres', user="junjiexing"), disableGroupedQuery=False):
        if self.grouped_query and not disableGroupedQuery:
            self.result = self.grouped_query.sub_query_result(
                self.where["value"])
            print("get result from grouped query")
        else:
            cur = conn.cursor()
            if self.where:
                cur.execute(self.query_to_execute(), (self.where["value"],))
            else:
                cur.execute(self.query_to_execute())
            self.result = cur.fetchall()
            cur.close()
