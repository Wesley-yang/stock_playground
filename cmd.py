# -*- coding: utf-8 -*-
# @Author: youerning
# @Date:   2019-07-26 17:39:07
# @Last Modified by:   youerning
# @Last Modified time: 2019-07-27 11:31:45
import argparse
from save_data import main as save_data
from utils import convert
from utils import dump
from utils import dump_index


if __name__ == "__main__":
    action_lst = ["save_data", "convert", "dump", "dump_index"]
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument('action', choices=action_lst)
    args = parser.parse_args()
    # convert()

    if args.action == "convert":
        convert()
    elif args.action == "dump":
        dump()
    elif args.action == "dump_index":
        dump_index()
    elif args.action == "save_data":
        save_data()

