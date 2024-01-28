# -*- coding: utf-8 -*-
# PJ: interesting_fact_code
# Created on: 7/21/20
# Author: Junjie Xing Github: @GavinXing


class FactNotExistError(Exception):
    def __init__(self, fact_id):
        self.fact_id = fact_id
        self.message = "Fact: {} doesn't exist in database.".format(fact_id)
        super().__init__(self.message)


class ClusterNotExistError(Exception):
    def __init__(self, cluster_id):
        self.cluster_id = cluster_id
        self.message = "Cluster: {} doesn't exist in database.".format(
            cluster_id)
        super().__init__(self.message)


class ComparisonNotExistError(Exception):
    def __init__(self, comparison_id):
        self.comparison_id = comparison_id
        self.message = "Comparison: {} doesn't exist in database.".format(
            comparison_id)
        super().__init__(self.message)
