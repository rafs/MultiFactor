#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 筹码分布因子
# @Filename: CYQ
# @Date:   : 2018-02-23 18:12
# @Author  : YuJun
# @Email   : yujun_mail@163.com


from src.factors.factor import Factor
import src.factors.cons as factor_ct
from src.util.utils import Utils, SecuTradingStatus
import pandas as pd
from pandas import DataFrame, Series
import numpy as np
from pathlib import Path
import logging
from multiprocessing import Pool, Manager
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

class CYQ(Factor):
    """筹码分布因子类"""
    __days = factor_ct.CYQ_CT.days_num      # 读取过去多少天的日K线行情数据进行因子载荷计算
    _db_file = Path(factor_ct.FACTOR_DB.db_path, factor_ct.CYQ_CT.db_file)      # 因子对应的数据库文件名

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        """
        计算指定日期、指定个股筹码分布的四个代理变量
        Parameters
        -------
        :param code: str
            个股代码, 如600000或SH600000
        :param calc_date: datetime-like, str
            计算日期, 格式YYYY-MM-DD
        :return: pd.Serues
        --------
            个股筹码分布的额四个代理变量
            0. ARC: 筹码分布的均值
            1. VRC: 筹码分布的方差
            2. SRC: 筹码分布的偏度
            3. KRC: 筹码分布的峰度
            若计算失败, 返回None
        """
        # 读取过去__days天的个股复权日K线行情数据
        df_mkt = Utils.get_secu_daily_mkt(code, end=calc_date, ndays=cls.__days, fq=True, range_lookup=True)
        if df_mkt is None:
            return None
        if len(df_mkt) < 20:
            return None
        # 按日期降序排列行情数据
        df_mkt.sort_values(by='date', ascending=False, inplace=True)
        # 遍历行情数据, 计算RC(相对资本收益)向量和ATR(调整换手率)向量
        arr_rc = np.zeros(len(df_mkt))
        arr_atr = np.zeros(len(df_mkt))
        p_c = df_mkt.iloc[0]['close']   # 截止日期的收盘价
        for j in range(len(df_mkt)):
            p_avg = df_mkt.iloc[j]['amount'] / df_mkt.iloc[j]['vol'] * df_mkt[j]['factor']
            arr_rc[j] = (p_c - p_avg) / p_c
            tr_j = df_mkt.iloc[j]['turnover1']
            if j == 0:
                arr_atr[j] = tr_j
            else:
                arr_atr[j] = arr_atr[j-1] / pre_tr * tr_j * ( 1. - pre_tr)
            pre_tr = tr_j
        arc = np.average(arr_rc, weights=arr_atr)
        rc_dev = arr_rc - arc
        n = len(df_mkt)
        vrc = n / (n-1.) * np.sum(arr_atr * rc_dev * rc_dev) / np.sum(arr_atr)
        src = n / (n-1.) * np.sum(arr_atr * np.float_power(rc_dev, 3)) / np.sum(arr_atr) / np.float_power(vrc, 1.5)
        krc = n / (n-1.) * np.sum(arr_atr * np.float_power(rc_dev, 4)) / np.sum(arr_atr) / np.float_power(vrc, 2)
        return pd.Series([arc, vrc, src, krc], index=['arc', 'vrc', 'src', 'krc'])


if __name__ == '__main__':
    pass
