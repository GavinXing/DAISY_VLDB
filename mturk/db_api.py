# -*- coding: utf-8 -*-
# PJ: interesting_fact_code
# Created on: 2020-07-17
# Author: Junjie Xing Github: @GavinXing

import uuid
import logging
import psycopg2

from exceptions import FactNotExistError, ComparisonNotExistError
from db_config import db_config


class DB_API:
    def __init__(self, dbname=db_config["db_name"], user=db_config["username"], password=db_config["password"]):
        super(DB_API).__init__()
        self.dbname = dbname
        self.user = user
        self.password = password

        self.conn = self.get_conn()

    def __del__(self):
        self.conn.close()
        logging.debug("conn closed.")

    def get_conn(self):
        if self.user:
            conn = psycopg2.connect(
                dbname=self.dbname, user=self.user, password=self.password)
        else:
            conn = psycopg2.connect(dbname=self.dbname)

        return conn

    def close_conn(self):
        self.conn.close()

    def insert_uuid(self):
        # insert and return nealy-generated id
        cur = self.conn.cursor()
        success = False
        while not success:
            id = uuid.uuid4().hex
            try:
                cur.execute("INSERT INTO UUID (id) VALUES (%s)", (id,))
                self.conn.commit()
                success = True
            except Exception:
                pass
        cur.close()
        return id

    def insert_fact_to_psql(self, fact):
        cur = self.conn.cursor()
        try:
            if fact["place_holders"]["where_condition"]:
                cur.execute(
                    "INSERT INTO fact(id, cluster_id, db_name, group_by, agg_func, numerical_col, where_col, value_col, sql_result, viz_link) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        fact["id"], fact["cluster_id"], fact["place_holders"]["db_name"],
                        fact["place_holders"]["group_by"],
                        fact["place_holders"]["agg_func"], fact["place_holders"]["numerical_col"],
                        fact["place_holders"]["where_condition"]["col"],
                        fact["place_holders"]["where_condition"]["value"], fact["sql_result"], fact["viz_link"]))
            else:
                cur.execute(
                    "INSERT INTO fact(id, cluster_id, db_name, group_by, agg_func, numerical_col, where_col, value_col, sql_result, viz_link) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        fact["id"], fact["cluster_id"], fact["place_holders"]["db_name"],
                        fact["place_holders"]["group_by"],
                        fact["place_holders"]["agg_func"], fact["place_holders"]["numerical_col"],
                        None, None, fact["sql_result"], fact["viz_link"]))
            self.conn.commit()
        except Exception as e:
            logging.error(e)
        finally:
            cur.close()

    def get_fact_from_psql(self, fact_id):
        sql = "SELECT * from Fact WHERE id=%s"
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (fact_id,))
            info = cur.fetchone()
            if not info:
                raise FactNotExistError(fact_id)
            return info
        except Exception as e:
            logging.error(e)
            raise e
        finally:
            cur.close()

    def insert_comparison(self, id, a_id, b_id, fact_pairs):
        cur = self.conn.cursor()
        sql = "INSERT INTO Comparison(id, cluster_a, cluster_b, fact_pairs) VALUES(%s, %s, %s, %s)"
        try:
            cur.execute(sql, (id, a_id, b_id, fact_pairs))
            self.conn.commit()
        except Exception as e:
            logging.error(e)
            raise e
        finally:
            cur.close()
        return True

    def get_comparison_hits(self, id):
        cur = self.conn.cursor()
        sql = "SELECT mturk_hit_id from Comparison WHERE id=%s"
        try:
            cur.execute(sql, (id,))
            data = cur.fetchall()
            if data:
                return data
            else:
                raise Exception("Fail to get comparison hits {}".format(id))
        except Exception as e:
            logging.error(e)
            raise e

    def get_comparison_with_id(self, id):
        cur = self.conn.cursor()
        sql = "SELECT * from Comparison WHERE id=%s"
        try:
            cur.execute(sql, (id,))
            data = cur.fetchone()
            if data:
                return data
            else:
                raise ComparisonNotExistError(comparison_id=id)
        except Exception as e:
            logging.error(e)
            raise e

    def insert_mturk_task(self, comparison_id, fact_a_id, fact_b_id, mturk_hit_id, mturk_group_id):
        cur = self.conn.cursor()
        sql = "INSERT INTO mturk_task(comparison_id, fact_a_id, fact_b_id, mturk_hit_id, mturk_group_id) VALUES(%s, %s, %s, %s, %s)"
        try:
            cur.execute(sql, (comparison_id, fact_a_id,
                        fact_b_id, mturk_hit_id, mturk_group_id))
            self.conn.commit()
        except Exception as e:
            logging.error(e)
            raise e
        finally:
            cur.close()
        return True

    def if_mturk_task_posted(self, comparison_id, fact_a_id, fact_b_id):
        cur = self.conn.cursor()
        sql = "SELECT * from mturk_task WHERE comparison_id=%s and fact_a_id=%s and fact_b_id=%s"
        try:
            cur.execute(sql, (comparison_id, fact_a_id, fact_b_id))
            data = cur.fetchone()
            if data:
                return data
            else:
                return None
        except Exception as e:
            logging.error(e)
            logging.error("error in if_mturk_task_posted")
        finally:
            cur.close()
        return None

    def get_hits_for_comparison(self, comparison_id):
        cur = self.conn.cursor()
        sql = "SELECT mturk_hit_id, ans from mturk_task WHERE comparison_id=%s"
        try:
            cur.execute(sql, (comparison_id,))
            data = cur.fetchall()
            if data:
                return data
            else:
                return None
        except Exception as e:
            logging.error(e)
            logging.error("error in get_hits_for_comparison")
        finally:
            cur.close()
        return None

    def get_hit_answer(self, hitid):
        cur = self.conn.cursor()
        sql = "SELECT mturk_hit_id, ans from mturk_task WHERE mturk_hit_id=%s"
        try:
            cur.execute(sql, (hitid,))
            data = cur.fetchall()
            if data:
                return data
            else:
                return None
        except Exception as e:
            logging.error(e)
            logging.error("error in get_hit_answer")
        finally:
            cur.close()
        return None

    def insert_hit_answer(self, fact_a_ms, fact_b_ms, ans, hitid):
        cur = self.conn.cursor()
        sql = "UPDATE  mturk_task SET fact_a_ms=%s, fact_b_ms=%s, ans=%s WHERE mturk_hit_id=%s"
        try:
            cur.execute(sql, (fact_a_ms, fact_b_ms, ans, hitid,))
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(e)
            return False

    def insert_comparison_answer(self, comparison_id, ans):
        cur = self.conn.cursor()
        sql = "UPDATE  comparison SET done=%s, ans=%s WHERE id=%s"
        try:
            cur.execute(sql, (1, ans, comparison_id,))
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(e)
            return False

    def insert_iteration(self, id, cluster_ids, pivot_id, comparison_ids, status, offset):
        cur = self.conn.cursor()
        sql = "INSERT INTO iteration(id, cluster_ids, pivot_id, comparison_ids, status, offset_num) VALUES(%s, %s, %s, %s, %s, %s)"
        try:
            cur.execute(sql, (id, cluster_ids, pivot_id,
                        comparison_ids, status, offset,))
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(e)
            raise e

    def update_iteration_status(self, id, status):
        cur = self.conn.cursor()
        sql = "UPDATE iteration SET status=%s WHERE id=%s"
        try:
            cur.execute(sql, (status, id,))
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(e)
            return False

    def get_iteration(self, id):
        cur = self.conn.cursor()
        sql = "SELECT cluster_ids, pivot_id, comparison_ids, status, offset_num, ans from iteration WHERE id=%s"
        try:
            cur.execute(sql, (id,))
            data = cur.fetchone()
            if data:
                return data
            else:
                return None
        except Exception as e:
            logging.error(e)
            logging.error("error in get_iteration")
        finally:
            cur.close()
        return None

    def if_comparison_exists(self, cluster_a_id, cluster_b_id):
        cur = self.conn.cursor()
        sql = "SELECT id from comparison WHERE (cluster_a=%s and cluster_b=%s) OR (cluster_a=%s and cluster_b=%s)"
        try:
            cur.execute(sql, (cluster_a_id, cluster_b_id,
                        cluster_b_id, cluster_a_id))
            data = cur.fetchone()
            if data:
                return data
            else:
                return None
        except Exception as e:
            logging.error(e)
            logging.error("error in if_comparison_exists")
        finally:
            cur.close()
        return None

    def update_comparison_posted(self, id, posted):
        cur = self.conn.cursor()
        sql = "UPDATE comparison SET posted=%s WHERE id=%s"
        try:
            cur.execute(sql, (posted, id,))
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(e)
            logging.error("error in update_comparison_posted")
            return False
