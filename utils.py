# -*- coding: utf-8 -*-
# @Author: youerning
# @Date:   2019-07-25 11:15:24
# @Last Modified by:   youerning
# @Last Modified time: 2019-07-26 16:19:40
import sqlite3
import logging
import os
import sys
import pandas as pd
from glob import glob
from os import path


READALL_SQL = "SELECT * FROM DATA"
READ_ONE_SQL = "SELECT * FROM DATA ORDER BY trade_date DESC LIMIT 1"
data_path_name = "data"
curdir = path.dirname(path.abspath(__file__))
data_path = path.join(curdir, data_path_name)
es_host = "127.0.0.1:9200"


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
    data = {}
    db_glob_lst = glob(path.join(data_path, "*.csv"))
    for fp in db_glob_lst:
        code = fp.split(".")[0]
        hist = pd.read_csv(fp)
        data[code] = hist

    return data


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


def dump():
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk
    date_format = "%Y-%m-%dT%H:%M:%S.%f+0800"
    es = Elasticsearch([es_host])
    # print(es.search())
    db_glob_lst = glob(path.join(data_path, "*.csv"))
    for db_path in db_glob_lst:
        # index_name = "stock" + path.basename(db_path).lower()
        data = pd.read_csv(db_path, parse_dates=["trade_date"])
        index_name = "stock-" + data.ts_code[0].lower()
        print("从 %s 加载数据" % db_path)

        bulk_lst = []
        for idx, value in enumerate(data.values, start=1):
            doc = {}
            for col_name, v in zip(data.columns, value):
                doc[col_name] = v

            doc["trade_date"] = doc["trade_date"].strftime(date_format)
            doc_id = "-".join([doc["ts_code"], doc["trade_date"]])
            bulk_lst.append({
                            "_index": index_name,
                            "_id": doc_id,
                            "_type": "doc",
                            "_source": doc,
                            })

            if idx % 100 == 0:
                bulk(es, bulk_lst, stats_only=True)
                bulk_lst = []


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument('action', choices=["convert", "dump"])
    args = parser.parse_args()
    # convert()

    if args.action == "convert":
        convert()
    elif args.action == "dump":
        dump()
