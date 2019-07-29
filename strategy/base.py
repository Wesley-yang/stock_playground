# -*- coding: utf-8 -*-
# @Author: youerning
# @Date:   2019-07-29 11:13:07
# @Last Modified by:   youerning
# @Last Modified time: 2019-07-29 17:26:46
import abc


class BaseStrategy(object):
    __class__ == abc.ABCMeta

    def buy(self, code, hist, pos_lst):
        return None

    def sell(self, code, hist, pos_lst):
        return None

    @abc.abstractmethod
    def on_data(self, code, hist, pos_lst):
        """
        return {
                "buy_lst": [],
                "sell_lst": []
                }
        """
        pass
