# -*- coding: utf-8 -*-
# @Author: youerning
# @Date:   2019-07-26 17:46:09
# @Last Modified by:   youerning
# @Last Modified time: 2019-07-26 23:54:13
import json
from os import path


config_file_name = "config.json"
curdir = path.dirname(path.abspath(__file__))
config_path = path.join(curdir, config_file_name)
config = json.load(open(config_path))

config["es_host"] = ["192.168.56.102:9200"]
# 股票索引名称
config["stock_index_name"] = 'stock'
# 指数索引名称
config["index_index_name"] = "index"
