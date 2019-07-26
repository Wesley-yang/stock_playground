# -*- coding: utf-8 -*-
# @Author: youerning
# @Date:   2019-07-26 17:39:07
# @Last Modified by:   youerning
# @Last Modified time: 2019-07-26 17:45:22
import argparse
from save_data import main as save_data
from utils import convert
from utils import dump


if __name__ == "__main__":
    action_lst = ["save_data", "convert", "dump"]
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument('action', choices=action_lst)
    args = parser.parse_args()
    # convert()

    if args.action == "convert":
        convert()
    elif args.action == "dump":
        dump()
    elif args.action == "save_data":
        save_data()
