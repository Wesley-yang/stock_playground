# -*- coding: utf-8 -*-
# @Author: youerning
# @Date:   2019-07-26 17:46:09
# @Last Modified by:   youerning
# @Last Modified time: 2019-07-26 17:54:15
import json
from os import path


config_file_name = "config.json"
curdir = path.dirname(path.abspath(__file__))
config_path = path.join(curdir, config_file_name)
config = json.load(open(config_path))

config["es_host"] = ["127.0.0.1:9200"]
