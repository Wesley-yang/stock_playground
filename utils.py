# -*- coding: utf-8 -*-
# @Author: youerning
# @Date:   2019-07-25 11:15:24
# @Last Modified by:   youerning
# @Last Modified time: 2019-07-27 00:22:35
import sqlite3
import logging
import os
import sys
import pandas as pd
from glob import glob
from os import path
from settings import config
from datetime import datetime


READALL_SQL = "SELECT * FROM DATA"
READ_ONE_SQL = "SELECT * FROM DATA ORDER BY trade_date DESC LIMIT 1"
data_path_name = "data"
curdir = path.dirname(path.abspath(__file__))
data_path = path.join(curdir, data_path_name)


def init_log(name, level=30, log_to_file=False):
    logger = logging.getLogger(name)
    formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_to_file:
        if sys.platform.startswith("linux"):
            fp = "/var/log/%s.log" % name
        else:
            fp = path.join(os.environ["HOMEPATH"], "%s.log" % name)

        file_handler = logging.FileHandler(filename=fp)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
    return logger


def load_hist():
    hist_data = {}
    db_glob_lst = glob(path.join(data_path, "*.csv"))
    for fp in db_glob_lst:
        hist = pd.read_csv(fp, parse_dates=["trade_date"])
        code = hist.ts_code[0]
        hist_data[code] = hist

    return hist_data


def convert():
    """convert data store with format sqlite to data with format csv"""
    db_glob_lst = glob(path.join(data_path, "*.db"))
    for db_path in db_glob_lst:
        fp = db_path[:-3] + ".csv"
        if path.exists(fp):
            continue

        with sqlite3.connect(db_path) as conn:
            data = pd.read_sql(READALL_SQL, conn, parse_dates=["trade_date"])
            data.sort_values("trade_date").to_csv(fp, index=False)

    db_glob_lst = glob(path.join(data_path, "*.csv"))
    for db_path in db_glob_lst:
        data = pd.read_csv(db_path, parse_dates=["trade_date"])
        data.sort_values("trade_date").to_csv(db_path, index=False)


def es_client():
    from elasticsearch import Elasticsearch
    es = Elasticsearch(config["es_host"])

    return es


def find_max_date(ts_code):
    es = es_client()
    body = {
        "aggs": {
            "trade_date": {
                "max": {
                    "field": "trade_date"
                }
            }
        },
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "match_phrase": {
                            "ts_code.keyword": {
                                "query": ts_code
                            }
                        }
                    }
                ]
            }
        }
    }

    ret = es.search(body=body)

    return ret["aggregations"]["trade_date"]["value"]


def dump():
    from elasticsearch.helpers import bulk
    date_format = "%Y-%m-%dT%H:%M:%S.%f+0800"
    es = es_client()
    # print(es.search())
    hist_data = load_hist()
    for code in hist_data:
        print("开始上传股票: %s" % code)
        data = hist_data[code]
        index_name = config["stock_index_name"]

        bulk_lst = []
        max_date = find_max_date(code)
        if max_date:
            max_date = datetime.fromtimestamp(max_date / 1000)
            print(max_date)
        else:
            max_date = datetime(1990, 1, 1)

        for idx, value in enumerate(data.values, start=1):
            doc = {}
            for col_name, v in zip(data.columns, value):
                doc[col_name] = v

            if doc["trade_date"] > max_date:
                continue
            doc["trade_date"] = doc["trade_date"].strftime(date_format)
            doc_id = "-".join([doc["ts_code"], doc["trade_date"]])
            bulk_lst.append({
                            "_index": index_name,
                            "_id": doc_id,
                            "_type": "doc",
                            "_source": doc,
                            })

            if idx % 500 == 0:
                bulk(es, bulk_lst, stats_only=True)
                bulk_lst = []

        if bulk_lst:
            bulk(es, bulk_lst, stats_only=True)


if __name__ == "__main__":
    print(find_max_date("000012.SZ"))
