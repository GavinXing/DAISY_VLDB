# -*- coding: utf-8 -*-
# PJ: interesting_fact_code
# Created on: 2020-07-10
# Author: Junjie Xing Github: @GavinXing

import uuid


def sql_generation(sql_info):
    sql = "<p>\n"
    sql += "SELECT " + sql_info["group_by"] + ", " + \
        sql_info["agg_func"] + "(" + sql_info["numerical_col"] + ")"
    sql += "\n<br>\nFROM " + sql_info["db_name"] + "\n<br>\n"
    if sql_info["where_condition"]:
        sql += "WHERE " + sql_info["where_condition"]["col"] + \
            "=" + str(sql_info["where_condition"]["value"])
        sql += "\n<br>\n"
    sql += "GROUP BY " + sql_info["group_by"] + "\n<br>\nORDER BY 2 DESC\n</p>"
    return sql


def sql_explanation(sql_info):
    exp = "<h5>"
    exp += "For each of the " + sql_info["group_by"] + ", "
    exp += "find the " + sql_info["agg_func"]
    if sql_info["numerical_col"] != "*":
        exp += " of the " + sql_info["numerical_col"]
    else:
        exp += " of the records"
    if sql_info["where_condition"]:
        exp += " for all records that have " + sql_info["where_condition"]["col"] + "=" + str(
            sql_info["where_condition"]["value"])
    exp += ".</h5>"
    return exp


def data_table_generation(data, sql_info, trim=False):
    data_html = """<table style="width:100%">\n"""
    data_html += """
    <tr>
        <th>{}</th>
        <th>{}</th>
    </tr>\n
    """.format(sql_info["group_by"], sql_info["agg_func"] + "(" + sql_info["numerical_col"] + ")")
    if trim and len(data) > 20:
        for (a, b) in data[:10]:
            data_html += """
            <tr>
                <td>{}</td>
                <td>{}</td>
            </tr>\n
            """.format(str(a), str(b))
        data_html += """
                    <tr>
                        <td>...</td>
                        <td>...</td>
                    </tr>\n
                    """
        for (a, b) in data[-10:]:
            data_html += """
            <tr>
                <td>{}</td>
                <td>{}</td>
            </tr>\n
            """.format(str(a), str(b))
    else:
        for (a, b) in data:
            data_html += """
            <tr>
                <td>{}</td>
                <td>{}</td>
            </tr>\n
            """.format(str(a), str(b))
    data_html += "</table>"
    return data_html


def db_schema_generation(db_info):
    rst = """
    <table style="width:100%">
        <tr>
            <th>Columns</th>
            <th>Type</th>
            <th>Description</th>
        </tr>\n
    """
    for (a, b, c) in db_info:
        rst += """
        <tr>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
        </tr>\n
        """.format(a, b, c)
    rst += "</table>"
    return rst


def xml_generation(hit_data, trim=False):
    if hit_data["fact_A"]["viz_link"][:5] != "http:":
        hit_data["fact_A"]["viz_link"] = hit_data["fact_A"]["viz_link"]
    if hit_data["fact_B"]["viz_link"][:5] != "http:":
        hit_data["fact_B"]["viz_link"] = hit_data["fact_B"]["viz_link"]

    seg_0 = """
    <HTMLQuestion xmlns="http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2011-11-11/HTMLQuestion.xsd">
        <HTMLContent><![CDATA[
        <!DOCTYPE html>
          <body>
            <script src="https://assets.crowd.aws/crowd-html-elements.js"></script>

    <style>
      h3 {
        margin-top: 0;
      }

      table, th, td {
      border: 1px solid black;
      border-collapse: collapse;
    }

      crowd-card {
        width: 100%;
      }

      .card {
        margin: 10px;
      }

      .left {
        width: 70%;
        margin-right: 10px;
        display: inline-block;
        height: 200px;
      }

      .right {
        width: 20%;
        height: 200px;
        display: inline-block;
      }

      th {
        witch: 20%
      }

      td {
        witch: 20%
      }

      tr {
        witch: 20%
      }
    </style>

    <crowd-form>
      

      <div class="left">
      <p>
      In this task, you will be ask to perform two sub-tasks.

        <br>

        For the first task, you will be asked which of the two SQL queries is <b>more interesting</b> than another. You will be given with the database schema.
        For each of two facts, you will be given SQL query, query explanation, query result and the visualization of the SQL query.
        <br>
        <b>Note:</b> Please judge the interestingness only by the query result, do not consider the semantic of the SQL itself.

        <br>
        For the second task, you will be asked whether the two SQL queries make sense by itself. For example, if a query try to calculate the SUM of y_coordinate, that may considered not make sense.
      </p>
        <h3>Database Schema</h3>

    """

    # add database schema

    seg_1 = db_schema_generation(hit_data["db_info"]["schema"])

    seg_2 = """
        <br><br>
        <h3>Which of the following fact is more interesting?</h3>
        <h4>A</h4>
        <crowd-card>
        <h5>SQL:</h5>
    """

    seg_3 = sql_generation(hit_data["fact_A"]["place_holders"])

    seg_4 = """
    <h5>SQL Explanation:</h5>
    """

    seg_5 = sql_explanation(hit_data["fact_A"]["place_holders"])

    seg_6 = "<h5>Data:</h5>"

    seg_7 = data_table_generation(
        hit_data["fact_A"]['sql_result'], hit_data["fact_A"]["place_holders"], trim=trim)

    seg_8 = """
    <h5>Visualization:</h5>
    <img
            src="{}"
            style="max-width: 60%"
          >
          <crowd-checkbox name="sql_a_not_ms">Check this if you find the SQL doesn't make sense</crowd-checkbox>
        </crowd-card>
      </div>

      <div class="right">
        <h3>Select an option</h3>

        <select name="interesting" style="font-size: large" required>
          <option value="">(Please select)</option>
          <option>A</option>
          <option>B</option>
        </select>
      </div>
    """.format(hit_data["fact_A"]["viz_link"])

    seg_9 = """
    <div class="left">
        <h4>B</h4>
        <crowd-card>
        <h5>SQL:</h5>
    """

    seg_10 = sql_generation(hit_data["fact_B"]["place_holders"])

    seg_11 = """
    <h5>SQL Explanation:</h5>
    """

    seg_12 = sql_explanation(hit_data["fact_B"]["place_holders"])

    seg_13 = "<h5>Data:</h5>"

    seg_14 = data_table_generation(
        hit_data["fact_B"]['sql_result'], hit_data["fact_B"]["place_holders"], trim=trim)

    seg_15 = """
    <h5>Visualization:</h5>
    <img
            src="{}"
            style="max-width: 60%"
          >
          <crowd-checkbox name="sql_b_not_ms">Check this if you find the SQL doesn't make sense</crowd-checkbox>
        </crowd-card>
      </div>

    </crowd-form>

          </body>
        </html>
      ]]></HTMLContent>
        <FrameHeight>0</FrameHeight>
    </HTMLQuestion>
    """.format(hit_data["fact_B"]["viz_link"])

    file_path = "tmp/" + uuid.uuid4().hex + ".xml"
    with open(file_path, "w+") as outfile:
        outfile.write(seg_0 + '\n')
        outfile.write(seg_1 + '\n')
        outfile.write(seg_2 + '\n')
        outfile.write(seg_3 + '\n')
        outfile.write(seg_4 + '\n')
        outfile.write(seg_5 + '\n')
        outfile.write(seg_6 + '\n')
        outfile.write(seg_7 + '\n')
        outfile.write(seg_8 + '\n')
        outfile.write(seg_9 + '\n')
        outfile.write(seg_10 + '\n')
        outfile.write(seg_11 + '\n')
        outfile.write(seg_12 + '\n')
        outfile.write(seg_13 + '\n')
        outfile.write(seg_14 + '\n')
        outfile.write(seg_15 + '\n')
    return file_path
