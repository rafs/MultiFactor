#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 价值类因子
# @Filename: Value
# @Date:   : 2018-01-15 23:45
# @Author  : YuJun
# @Email   : yujun_mail@163.com


from src.factors.factor import Factor
import src.factors.cons as factor_ct
from src.util.utils import Utils
from src.util.dataapi.CDataHandler import CDataHandler
from pandas import Series
import os
import datetime
import logging
from multiprocessing import Pool, Manager
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


class Value(Factor):
    """
    价值类因子
    --------
    包含：EP_TTM（TTM净利润/总市值）, BP_LR（净资产(最新财报)/总市值）, OCF_TTM（TTM经营性现金流/总市值）
    --------
    """
    _db_file = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.VALUE_CT.db_file)

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        """
        计算指定日期、指定个股的价值因子，包含EP_TTM, BP_LR, OCF_TTM
        Parameters:
        --------
        :param code: str
            个股代码：如600000或SH600000
        :param calc_date: datetime-like or str
            计算日期，格式YYYY-MM-DD, YYYYMMDD
        :return: pd.Series
        --------
            价值类因子值
            0. EP_TTM: TTM净利润/总市值
            1. BP_LR: 净资产（最新财报）/总市值
            2. OCF_TTM: TTM经营性现金流/总市值
            若计算失败，返回None
        """
        code = Utils.code_to_symbol(code)
        calc_date = Utils.to_date(calc_date)
        # 读取TTM财务数据
        ttm_fin_data = Utils.get_ttm_fin_basic_data(code, calc_date)
        if ttm_fin_data is None:
            return None
        # 读取最新财报数据
        report_date = Utils.get_fin_report_date(calc_date)
        fin_basic_data = Utils.get_fin_basic_data(code, report_date)
        if fin_basic_data is None:
            return None
        # 计算总市值
        mkt_daily = Utils.get_secu_daily_mkt(code, calc_date, fq=False, range_lookup=True)
        if mkt_daily.shape[0] == 0:
            return None
        cap_struct = Utils.get_cap_struct(code, calc_date)
        if cap_struct is None:
            return None
        total_cap = cap_struct.total - cap_struct.liquid_b - cap_struct.liquid_h
        total_mkt_cap = total_cap * mkt_daily.close
        # 计算价值类因子
        ep_ttm = ttm_fin_data['NetProfit'] / total_mkt_cap
        ocf_ttm = ttm_fin_data['NetOperateCashFlow'] / total_mkt_cap
        bp_lr = fin_basic_data.ShareHolderEquity / total_mkt_cap
        return Series([ep_ttm, bp_lr, ocf_ttm], index=['EP_TTM', 'BP_LR', 'OCF_TTM'])


if __name__ == '__main__':
    pass
