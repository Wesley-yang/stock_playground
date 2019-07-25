# -*- coding: utf-8 -*-
# @Author: youerning
# @Date:   2019-07-25 11:15:24
# @Last Modified by:   youerning
# @Last Modified time: 2019-07-25 20:54:36
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


if __name__ == "__main__":
    convert()
