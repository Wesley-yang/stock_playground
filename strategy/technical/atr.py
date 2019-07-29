# -*- coding: utf-8 -*-
# @Author: youerning
# @Date:   2019-07-29 17:38:26
# @Last Modified by:   youerning
# @Last Modified time: 2019-07-29 17:39:00
import pandas as pd


def atr_calc(df, n=14):
    data = pd.DataFrame()
    high = df["high"]
    low = df["low"]
    close = df["close"]
    data['tr0'] = abs(high - low)
    data['tr1'] = abs(high - close.shift())
    data['tr2'] = abs(low - close.shift())
    tr = data[['tr0', 'tr1', 'tr2']].max(axis=1)
    atr = tr.rolling(10).mean()
    df["atr"] = atr
    return df
