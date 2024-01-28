# -*- coding: utf-8 -*-
# PJ: interesting_fact_code
# Created on: 2020-07-18
# Author: Junjie Xing Github: @GavinXing

import atexit
import logging
import pickle
import random
import time
from os.path import isfile, join, exists
from os import listdir, mkdir

from tqdm import tqdm

from config import *
from db_api import DB_API
from db_info import db_info
from mturk_api import create_hit_with_type, get_hit_ans

random.seed(1)
logging.basicConfig(level=logging.DEBUG)


class Fact:
    def __init__(self, db_name="", group_by="", agg_func="", num_col="", where_col="", where_value="", sql_result=[],
                 viz_link="", db_api: DB_API = None, cluster_id="", fact_id="", reload=False):
        if fact_id:
            if reload:
                fact_info = db_api.get_fact_from_psql(fact_id=fact_id)
                self.load_with_psql(fact_info)
                self.loaded = True
            else:
                self.id = fact_id
                self.loaded = False
        else:
            self.id = db_api.insert_uuid()
            self.cluster_id = cluster_id
            self.place_holders = {
                "db_name": db_name,
                "group_by": group_by,
                "agg_func": agg_func,
                "numerical_col": num_col,
                "where_condition": {}
            }
            if where_col and where_value:
                self.place_holders["where_condition"] = {
                    "col": where_col,
                    "value": where_value
                }
            self.sql_result = sql_result
            self.viz_link = viz_link

            db_api.insert_fact_to_psql(self.__dict__)
            self.loaded = True
            logging.debug("Fact: {} created with data in Cluster: {} .".format(
                self.id, self.cluster_id))

    @staticmethod
    def psql_helper(sql_result):
        rst = []
        for x in sql_result:
            rst.append(tuple(x[1:-1].split(",")))
        return rst

    def __eq__(self, other):
        if self.id == other.id:
            return True
        else:
            return False

    def load_data(self, db_api: DB_API):
        if self.loaded:
            return
        fact_info = db_api.get_fact_from_psql(fact_id=self.id)
        self.load_with_psql(fact_info)
        self.loaded = True

    def load_with_psql(self, fact_info):
        try:
            self.id = fact_info[0]
            self.cluster_id = fact_info[1]
            self.place_holders = {
                "db_name": fact_info[2],
                "group_by": fact_info[3],
                "agg_func": fact_info[4],
                "numerical_col": fact_info[5],
                "where_condition": {}
            }
            if fact_info[6] and fact_info[7]:
                self.place_holders["where_condition"] = {
                    "col": fact_info[6],
                    "value": fact_info[7]
                }
            self.sql_result = Fact.psql_helper((fact_info[8]))
            self.viz_link = fact_info[9]
        except Exception as e:
            logging.debug(
                "This should not happen. Error in Fact->load_with_psql")
            raise e


class Cluster:
    def __init__(self, db_name, data=[], cluster_id="", db_api: DB_API = DB_API()):
        self.db_name = db_name
        self.facts = []
        self.log_folder = join(RUNTIME_LOG_DIR, db_name)
        if data:
            self.id = db_api.insert_uuid()
            self.create_cluster_from_data(data, db_api)
        else:
            assert cluster_id != ""
            self.id = cluster_id
            self.load_cluster_with_log_file()

    def __store_status(self):
        # store Cluster information to RUNTIME_LOG_DIR
        data = {self.id: [fact.id for fact in self.facts]}

        with open(join(self.log_folder, "{}.pickle".format(self.id)), "wb") as outfile:
            pickle.dump(data, outfile)
            logging.info("Cluster: {} info stored.".format(self.id))

    def create_cluster_from_data(self, data, db_api):
        for fact in data:
            fact_obj = Fact(fact[0], fact[1], fact[2], fact[3],
                            fact[4], fact[5], fact[6], fact[7], db_api, self.id)
            self.facts.append(fact_obj)
        logging.info("Cluster: {} loaded with data.".format(self.id))
        self.__store_status()

    def load_cluster_with_log_file(self):
        cluster_log = "{}.pickle".format(self.id)
        assert isfile(join(self.log_folder, cluster_log))
        with open(join(self.log_folder, cluster_log), "rb") as infile:
            data = pickle.load(infile)
        for fact_id in data[self.id]:
            self.facts.append(Fact(fact_id=fact_id, reload=False))
        logging.info("Cluster: {} loaded with log file. Contains {} facts.".format(
            self.id, len(self.facts)))


class Hit:
    def __init__(self, hitid, ans=None):
        self.id = hitid
        self.ans = ans

    # def __repr__(self):
    #     return json.dumps(self.__dict__)

    def check(self, db_api=DB_API(), live=False):
        if self.ans in [0, 1]:
            return True
        else:
            # should check for database if manully
            ans_db = db_api.get_hit_answer(hitid=self.id)
            if ans_db[0][1] is not None:
                logging.info(
                    "HIT: {} retrieve answer from psql.".format(self.id))
                self.ans = ans_db[0][1]
                return True

            # finally check MTurk
            ans = get_hit_ans(HitID=self.id, live=LIVE)
            if ans:
                hit_ans = 0 if ans["interesting"] == 'A' else 1
                sql_a_ms = 0 if ans["sql_a_not_ms"]["on"] else 1
                sql_b_ms = 0 if ans["sql_b_not_ms"]["on"] else 1
                success = db_api.insert_hit_answer(
                    sql_a_ms, sql_b_ms, hit_ans, self.id)
                if success:
                    self.ans = hit_ans
                    return True
            else:
                return False


class Comparison:
    def __init__(self, cluster_a: Cluster = None, cluster_b: Cluster = None, db_api: DB_API = None, comparison_id=""):
        self.hits = []
        self.db_api = db_api
        if comparison_id:
            self.id = comparison_id
            assert db_api is not None
            self.load_comparison_with_psql(db_api)
        else:
            id = db_api.if_comparison_exists(cluster_a.id, cluster_b.id)
            if id:
                self.id = id
                assert db_api is not None
                self.load_comparison_with_psql(db_api)
            else:
                self.id = db_api.insert_uuid()
                self.cluster_a_id = cluster_a.id
                self.cluster_b_id = cluster_b.id

                self.facts_pairs = Comparison.generate_fact_pairs(
                    cluster_a.facts, cluster_b.facts)
                self.done = 0
                self.posted = 0
                self.ans = None
                db_api.insert_comparison(
                    self.id, self.cluster_a_id, self.cluster_b_id, self.facts_pairs)
                logging.debug(
                    "Comparison: {} inserted into psql.".format(self.id))

    # def __repr__(self):
    #     data = self.__dict__.copy()
    #     data["db_api"] = "db_api"
    #     for index in range(len(data["hits"])):
    #         data["hits"][index] = data["hits"][index].__dict__
    #     return json.dumps(data)

    def load_comparison_with_psql(self, db_api: DB_API):
        data = db_api.get_comparison_with_id(self.id)
        hits = db_api.get_hits_for_comparison(self.id)
        self.cluster_a_id = data[1]
        self.cluster_b_id = data[2]
        self.facts_pairs = data[3]
        self.done = data[4]
        self.posted = data[5]
        self.ans = data[6]
        if hits:
            hit_objs = []
            for (hit_id, hit_ans) in hits:
                h = Hit(hit_id, hit_ans)
                hit_objs.append(h)
            self.hits = hit_objs
        else:
            self.post_questions_to_mturk(
                db_name=DB_NAME, db_api=db_api, live=LIVE)

    @staticmethod
    def generate_fact_pairs(facts_a: [Fact], facts_b: [Fact]):
        if len(facts_a) * len(facts_b) < FACT_PAIRS_PER_COMPARISON:
            pairs = []
            while len(pairs) < FACT_PAIRS_PER_COMPARISON:
                a = random.choice(facts_a)
                b = random.choice(facts_b)
                pairs.append((a.id, b.id))
            return pairs
        pairs = set()
        while len(pairs) < FACT_PAIRS_PER_COMPARISON:
            a = random.choice(facts_a)
            b = random.choice(facts_b)
            pairs.add((a.id, b.id))
        return [list(pair) for pair in pairs]

    def post_questions_to_mturk(self, db_name, db_api: DB_API, live=False):
        assert self.facts_pairs is not None
        for [fact_a, fact_b] in self.facts_pairs:
            self.post_question_to_mturk(
                db_name, fact_a, fact_b, db_api, live=LIVE)
        if len(self.hits) == len(self.facts_pairs):
            self.posted = 1
            self.db_api.update_comparison_posted(id=self.id, posted=1)

    def post_question_to_mturk(self, db_name, fact_a_id, fact_b_id, db_api: DB_API, live=False):
        hit_info = db_api.if_mturk_task_posted(self.id, fact_a_id, fact_b_id)
        if hit_info:
            self.hits.append(Hit(hit_info[3], hit_info[7]))
            logging.info("task already posted")
            return
        fact_a = Fact(fact_id=fact_a_id, reload=True, db_api=db_api)
        fact_b = Fact(fact_id=fact_b_id, reload=True, db_api=db_api)
        data = {
            "db_info": db_info[db_name],
            "fact_A": fact_a.__dict__,
            "fact_B": fact_b.__dict__
        }
        try:
            hit_id, group_id = create_hit_with_type(data, live=LIVE)
            db_api.insert_mturk_task(
                self.id, fact_a_id, fact_b_id, hit_id, group_id)
            self.hits.append(Hit(hit_id, None))
        except Exception as e:
            logging.error(e)
            logging.error("Create HIT failed.")
            raise e

    def check(self, live=False):
        if self.done:
            return True
        assert len(self.hits) == FACT_PAIRS_PER_COMPARISON
        for hit in self.hits:
            hit.check(db_api=self.db_api, live=LIVE)
        all_ansed = True
        zero_s = 0
        one_s = 0
        done_cnt = 0
        for hit in self.hits:
            if hit.ans == 0:
                zero_s += 1
                done_cnt += 1
            elif hit.ans == 1:
                one_s += 1
                done_cnt += 1
            else:
                all_ansed = False
                break
        if all_ansed:
            if zero_s > one_s:
                ans = 0
            else:
                ans = 1
            success = self.db_api.insert_comparison_answer(self.id, ans)
            if success:
                self.done = 1
                self.ans = ans
                return True
            else:
                return False


class QuickSelectIteration:
    """
    Perform an iteration of quick select
    :param List A[...] and pivot x is the last element of A
    :return [..X..]
    """

    def __init__(self, db_name, clusters: [Cluster], offset: int = 0, iteration_id="", db_api: DB_API = DB_API(),
                 live: bool = False):
        self.db_name = db_name
        self.db_api = db_api
        self.ans = -1
        self.offset = offset
        if iteration_id:
            logging.info(
                "Initiating QuickSelectIteration {} from psql".format(iteration_id))
            self.id = iteration_id
            self.load_iteration_from_psql()
            logging.info("QuickSelectIteration {} status: {}.".format(
                self.id, self.status))
            self.action_after_reload(live=LIVE)
        else:
            assert len(clusters) >= 2
            self.id = db_api.insert_uuid()
            logging.info(
                "Initiating QuickSelectIteration {} from scratch.".format(self.id))
            self.clusters = clusters[:-1]  # all clusters except for the pivot
            self.pivot = clusters[-1]
            self.comparisons = []  # should be a list of Comparison objs
            self.status = "Initiated"

            self.create_comparisons()
            self.post_all_comparisons()
        logging.info("QuickSelectIteration {} initiated.".format(self.id))
        logging.info("QuickSelectIteration {} status: {}.".format(
            self.id, self.status))

    def create_comparisons(self):
        for cluster in self.clusters:
            comparison = Comparison(
                cluster_a=cluster, cluster_b=self.pivot, db_api=self.db_api)
            # comparison.post_questions_to_mturk(db_name=db_name, db_api=self.db_api, live=LIVE)
            self.comparisons.append(comparison)
        self.status = "Comparison Created"
        self.store_info_into_psql()
        logging.debug("Iteration: {} created with {} comparisons.".format(
            self.id, len(self.comparisons)))

    def post_all_comparisons(self, live=False):
        logging.info("Post all comparisons for iteration: {}".format(self.id))
        for comparison in self.comparisons:
            if not comparison.posted:
                comparison.post_questions_to_mturk(
                    db_name=self.db_name, db_api=self.db_api, live=LIVE)
        all_posted = True
        for comparison in self.comparisons:
            if not comparison.posted:
                all_posted = False
        if all_posted:
            self.status = "Posted"
            self.db_api.update_iteration_status(self.id, self.status)
            logging.info(
                "All comparisons for iteration: {} posted.".format(self.id))

    def store_info_into_psql(self):
        assert len(self.comparisons) == len(self.clusters)
        clusters = [cluster.id for cluster in self.clusters]
        comparisons = [comparison.id for comparison in self.comparisons]
        self.db_api.insert_iteration(id=self.id, cluster_ids=clusters, pivot_id=self.pivot.id,
                                     comparison_ids=comparisons, status=self.status, offset=self.offset)

    def load_iteration_from_psql(self):
        (cluster_ids, pivot_id, comparison_ids, status,
         offset, ans) = self.db_api.get_iteration(id=self.id)
        self.offset = offset
        if status == "Done":
            self.ans = ans
            return
        elif status == "Initiated":
            self.clusters = [Cluster(self.db_name, cluster_id=cluster_id, db_api=self.db_api) for cluster_id in
                             cluster_ids]
            self.pivot = Cluster(
                self.db_name, cluster_id=pivot_id, db_api=self.db_api)
            self.status = status
            return
        elif status in ["Comparison Created", "Posted"]:
            self.comparisons = []
            for comparison_id in comparison_ids:
                comparison = Comparison(
                    comparison_id=comparison_id, db_api=self.db_api)
                self.comparisons.append(comparison)
            self.pivot = Cluster(
                self.db_name, cluster_id=pivot_id, db_api=self.db_api)
            self.status = status
            return
        else:
            raise NotImplementedError

    def action_after_reload(self, live=False):
        if self.status == "Done":
            return
        elif self.status == "Initiated":
            self.create_comparisons()
            self.post_all_comparisons()
        elif self.status == "Comparison Created":
            self.post_all_comparisons()
        elif self.status == "Posted":
            self.check(live=LIVE)
        elif self.status == "Done":
            return
        else:
            return

    def check(self, live=False):
        logging.info(
            "Checking comparisons for QuickSelectIteration: {}".format(self.id))
        logging.info("QuickSelectIteration {} status: {}.".format(
            self.id, self.status))
        if self.status == "Done":
            logging.info("QuickSelectIteration: {} DONE.".format(self.id))
            return True
        assert len(self.comparisons) > 0
        all_done = True
        not_done_cnt = 0
        not_done_ids = []
        with tqdm(total=len(self.comparisons)) as pbar:
            for comparison in self.comparisons:
                if not comparison.check(live=LIVE):
                    all_done = False
                    not_done_cnt += 1
                    not_done_ids.append(comparison.id)
                pbar.update(1)
        if all_done:
            self.status = "Done"
            logging.info("QuickSelectIteration: {} DONE.".format(self.id))
            return True
        else:
            logging.info(
                "QuickSelectIteration: {} {} out of {} comparisons not completed.".format(self.id, not_done_cnt,
                                                                                          len(self.comparisons)))
        return False

    def get_answer(self):
        try:
            right = []
            left = []
            assert len(self.comparisons) > 0
            for comparison in self.comparisons:
                assert comparison.done == 1
                if comparison.ans == 0:
                    right.append(
                        Cluster(self.db_name, cluster_id=comparison.cluster_a_id, db_api=self.db_api))
                else:
                    left.append(
                        Cluster(self.db_name, cluster_id=comparison.cluster_a_id, db_api=self.db_api))
                pos = len(left)
            return pos, self.pivot, left, right, self.offset
        except Exception as e:
            logging.error(e)
            raise e


class QuickSelect:
    def __init__(self, db_name, clusters: [Cluster], info={}, k=100, live=False):
        self.db_name = db_name
        self.db_api = DB_API()
        if info:
            logging.info("Initiating QuickSelect with backup.")
            self.k = info["k"]
            self.jobs = [QuickSelectIteration(db_name, clusters, iteration_id=job_id, db_api=self.db_api, live=LIVE) for
                         job_id in
                         info["jobs"]]
            self.buckets = info["buckets"]
            self.live = live
            self.status = info["status"]
            logging.info("QuickSelect top {} from {} clusters.".format(
                self.k + 1, len(clusters)))
        else:
            logging.info(
                "Initiating QuickSelect from scratch. Select top {}".format(k))
            self.k = k
            self.jobs = []  # should be a queue of Iteration
            self.buckets = dict()
            self.buckets[len(clusters) - 1] = clusters
            self.live = live
            self.status = 'SELECT'  # three status, SELECT, SORT, DONE

            logging.info("QuickSelect top {} from {} clusters.".format(
                self.k + 1, len(clusters)))
            self.post_first_iteration(clusters)

    def post_first_iteration(self, clusters):
        logging.info("QuickSelect: preparing for posting the first iteration.")
        iter = QuickSelectIteration(
            self.db_name, clusters, db_api=self.db_api, live=LIVE)
        logging.info(
            "Iteration Initiated, with {} clusters".format(len(clusters)))
        self.jobs.append(iter)
        iter.post_all_comparisons(live=LIVE)

    def print_status(self):
        logging.info("========================================")
        logging.info("QuickSelect status: {}".format(self.status))
        keys = list(self.buckets.keys())
        keys.sort()
        for key in keys:
            bucket = self.buckets[key]
            logging.info("{} : ".format(key))
            logging.info(", ".join([cluster.id for cluster in bucket]))
        logging.info("========================================")

    def proceed(self):
        logging.info("Check QuickSelect ...")
        logging.info("Current jobs: {}".format(
            "\t".join([iter.id for iter in self.jobs])))
        self.print_status()
        if self.status == 'SELECT':
            logging.info("In SELECT.")
            assert len(self.jobs) == 1
            check = self.jobs[0].check()
            if check:
                job = self.jobs.pop()
                pos, pivot, left, right, offset = job.get_answer()
                logging.info("pivot : {} at {}".format(pivot.id, offset + pos))
                assert offset + len(left) + len(right) in self.buckets.keys()
                del self.buckets[offset + len(left) + len(right)]
                self.buckets[offset + pos] = [pivot]
                if len(left) > 0:
                    self.buckets[offset + pos - 1] = left
                if len(right) > 0:
                    self.buckets[offset + len(left) + len(right)] = right
                if offset + pos < self.k:
                    if len(right) > 1:
                        iter = QuickSelectIteration(db_name=self.db_name, clusters=right, offset=offset + pos + 1,
                                                    db_api=self.db_api, live=LIVE)
                        self.jobs.append(iter)
                        iter.post_all_comparisons(live=LIVE)
                elif offset + pos > self.k:
                    if len(left) > 1:
                        iter = QuickSelectIteration(db_name=self.db_name, clusters=left, offset=offset,
                                                    db_api=self.db_api, live=LIVE)
                        self.jobs.append(iter)
                        iter.post_all_comparisons(live=LIVE)

                if self.k in self.buckets.keys():
                    self.status = "SORT"
                    keys = list(self.buckets.keys())
                    keys.sort()
                    offset = 0
                    for key in keys:
                        if key > self.k:
                            pass
                        else:
                            if len(self.buckets[key]) >= 2:
                                iter = QuickSelectIteration(db_name=self.db_name, clusters=self.buckets[key],
                                                            offset=offset,
                                                            db_api=self.db_api, live=LIVE)
                                self.jobs.append(iter)
                                iter.post_all_comparisons(live=LIVE)
                            offset += len(self.buckets[key])
                self.print_status()
        elif self.status == "SORT":
            for index, job in enumerate(self.jobs):
                check = job.check()
                if check:
                    job = self.jobs.pop(index)
                    pos, pivot, left, right, offset = job.get_answer()
                    logging.info("pivot : {} at {}".format(
                        pivot.id, offset + pos))
                    assert offset + len(left) + \
                        len(right) in self.buckets.keys()
                    del self.buckets[offset + len(left) + len(right)]
                    self.buckets[offset + pos] = [pivot]
                    if len(left) > 0:
                        self.buckets[offset + pos - 1] = left
                    if len(right) > 0:
                        self.buckets[offset + len(left) + len(right)] = right
                    if len(right) > 1:
                        iter_left = QuickSelectIteration(db_name=self.db_name, clusters=right, offset=offset + pos + 1,
                                                         db_api=self.db_api, live=LIVE)
                        self.jobs.append(iter_left)
                        iter_left.post_all_comparisons(live=LIVE)

                    if len(left) > 1:
                        iter_right = QuickSelectIteration(db_name=self.db_name, clusters=left, offset=offset,
                                                          db_api=self.db_api, live=LIVE)
                        self.jobs.append(iter_right)
                        iter_right.post_all_comparisons(live=LIVE)
                    self.print_status()
            if len(self.jobs) == 0:
                self.status = "DONE"
                self.print_status()
                exit()


class Dataset:
    def __init__(self, db_name, schema=[], clusters_file="", reload=False, live=False):
        self.id = db_name
        self.name = db_name
        self.schema = schema
        self.clusters = []
        self.log_folder = join(RUNTIME_LOG_DIR, self.name)
        self.quick_select = None
        self.live = LIVE

        logging.info("Environment live: {}".format(LIVE))
        if LIVE:
            msg = 'Please conform that you want to work on production'
            shall = input("%s (y/N) " % msg).lower() == 'y'
            if not shall:
                logging.info("Do nothing. System Exit.")
                exit()
            else:
                logging.info("DB_NAME: {}".format(self.name))
                logging.info("CLUSTER_DUMP: {}".format(clusters_file))
                logging.info("FACT_PAIRS_PER_COMPARISON: {}".format(
                    FACT_PAIRS_PER_COMPARISON))
                logging.info(
                    "SLEEP_DURATION: {} seconds".format(SLEEP_DURATION))
                logging.info("TOP K: {}".format(K + 1))
                msg = 'Please conform that the following information is right'
                shall = input("%s (y/N) " % msg).lower() == 'y'
                if not shall:
                    logging.info("Do nothing. System Exit.")
                    exit()

        if not exists(self.log_folder):
            mkdir(self.log_folder)
            logging.info("Log folder: {} created.".format(self.log_folder))
        if clusters_file:
            logging.info(
                "Constructing Database {} with cluster file.".format(self.name))
            logging.info("Cluster file: {}".format(clusters_file))
            self.load_clusters_with_data(clusters_file)
        if reload:
            logging.info(
                "Reloading Database {} with log file.".format(self.name))
            self.load_clusters_with_log()
        self.db_api = DB_API()

        if not self.quick_select:
            assert len(self.clusters) > 0
            logging.info(
                "Initiating QuickSelect for Dataset {}".format(self.name))
            self.quick_select = QuickSelect(
                db_name, self.clusters, k=K, live=LIVE)

        atexit.register(self.__store_status)

    def __store_status(self):
        logging.info("__store_status is called.")
        if self.quick_select:
            del self.quick_select.db_api
        data = {"clusters": [cluster.id for cluster in self.clusters],
                "schema": self.schema, "quick_select_info": {
                "k": self.quick_select.k,
                "jobs": [iter.id for iter in self.quick_select.jobs],
                "buckets": self.quick_select.buckets,
                "status": self.quick_select.status
        }}

        i = 0
        files = listdir(self.log_folder)
        while "{}_{}.pickle".format(self.id, str(i)) in files:
            i += 1

        with open(join(self.log_folder, "{}_{}.pickle".format(self.id, str(i))), "wb") as outfile:
            pickle.dump(data, outfile)
            logging.info("Database: {} info stored.".format(self.name))

    def __exit__(self, exc_type, exc_val, exc_tb):
        # store Cluster information to RUNTIME_LOG_DIR
        self.__store_status()
        logging.debug("__exit__ in Database called.")

    # def __del__(self):
    #     logging.info("Store Database status on __del__.")
    #     self.__store_status()

    def load_clusters_with_data(self, f_path):
        with open(f_path, "rb") as infile:
            clusters = pickle.load(infile)

        logging.info("Loading {} clusters.".format(len(clusters)))
        for index, cluster in enumerate(clusters):
            logging.info("Start loading cluster: #{}".format(index + 1))
            cluster_obj = Cluster(db_name=self.name, data=cluster)
            self.clusters.append(cluster_obj)
            logging.info("Loading cluster: #{} succeeded.".format(index + 1))

    def load_clusters_with_log(self):
        logging.info("Start load clusters with log file")
        files = listdir(self.log_folder)
        dataset_files = [f for f in files if f.startswith(self.id)]
        assert len(dataset_files) > 0
        last_db_log = "{}_{}.pickle".format(self.id, str(
            max([int(f[:-7].split("_")[-1]) for f in dataset_files])))
        assert isfile(join(self.log_folder, last_db_log))
        logging.info("Reload with log file {}".format(
            join(self.log_folder, last_db_log)))
        with open(join(self.log_folder, last_db_log), "rb") as infile:
            data = pickle.load(infile)
        self.schema = data["schema"]
        clusters = []
        logging.info("Loading {} clusters.".format(len(data["clusters"])))
        for cluster_id in data["clusters"]:
            clusters.append(Cluster(db_name=self.name, cluster_id=cluster_id))
            logging.info(
                "Loading cluster: #{} succeeded.".format(len(clusters)))
        self.clusters = clusters
        self.quick_select = QuickSelect(
            self.name, clusters, info=data["quick_select_info"])

        logging.info("Load clusters with log file succeeded.")


def countdown(t):
    while t:
        mins, secs = divmod(t, 60)
        timeformat = 'Sleeping: {:02d}:{:02d}'.format(mins, secs)
        print(timeformat, end='\r')
        time.sleep(1)
        t -= 1
    logging.info("Sleep ended.")


def main(args):
    if args.function == "run":
        if args.reload:
            db = Dataset(DB_NAME, reload=True)
        else:
            db = Dataset(
                DB_NAME, schema=db_info[DB_NAME], clusters_file=CLUSTER_DUMP)
        while db.quick_select.status != "DONE":
            db.quick_select.proceed()
            logging.info("Start sleeping for {}s.".format(SLEEP_DURATION))
            countdown(SLEEP_DURATION)
    else:
        raise NotImplementedError


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Main MTurk Labeling')
    parser.add_argument('function', type=str,
                        choices=['run'])
    parser.add_argument('--reload', action="store_true")
    args = parser.parse_args()
    main(args)
