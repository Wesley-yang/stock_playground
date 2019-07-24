# -*- coding: utf-8 -*-
# @Author: youerning
# @Date:   2019-06-24 20:11:30
# @Last Modified by:   youerning
# @Last Modified time: 2019-06-24 22:28:31
# 下载日线数据
import tushare as ts
import sqlite3
import pandas as pd
from datetime import datetime
from os import path
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures as futures
import time
import os


TOKEN = "0c0bc7d7e15afa21a81c8da767227c9f5ffae9ab235d85bdd3473248"
START_DATE = "2012-01-01"
# END_DATE = ""
DATA_DIR = "data"
READALL_SQL = "SELECT * FROM DATA"
READ_ONE_SQL = "SELECT * FROM DATA ORDER BY trade_date DESC LIMIT 1"
DAY_FORMAT = "%Y%m%d"


def save_data(code):
    now = datetime.now()
    now_str = now.strftime(DAY_FORMAT)
    abs_path = path.dirname(path.abspath(__file__))

    print("下载股票(%s)日线数据" % code)
    fname = "%s.db" % code
    db_path = path.join(abs_path, DATA_DIR, fname)

    # TODO:
    # with open conn
    if path.exists(db_path) and os.stat(db_path).st_size > 0:
        print("db文件已存在")
        conn = sqlite3.connect(db_path)
        res = pd.read_sql(READ_ONE_SQL, conn)
        last_trade_date = res.iloc[0, 1]
        print("上次时间: %s" % last_trade_date)
        if now_str == last_trade_date:
            print("数据已经是最新的了")
            return
        start_date = pd.to_datetime(last_trade_date) + pd.Timedelta(1, unit="D")
        start_date = start_date.strftime(DAY_FORMAT)

        print("上次时间: %s" % last_trade_date)
    else:
        conn = sqlite3.connect(db_path)
        start_date = START_DATE
        print("全量更新股票")

    if start_date == now_str:
        print("数据已经是最新的了")
        return
    try:
        data = pro.daily(ts_code=code, start_date=start_date)
    except Exception as e:
        time.sleep(10)
        return

    data.sort_values("trade_date").to_sql("DATA", conn, index=False, if_exists="append")
    print("下载成功")


def main():
    pool = ThreadPoolExecutor(3)
    futures.wait(pool.map(save_data, code_lst))
    print("所有任务完成")


if __name__ == '__main__':
    pro = ts.pro_api()
    # 查询当前所有正常上市交易的股票列表
    data = pro.stock_basic(list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    code_lst = data.ts_code
    main()
