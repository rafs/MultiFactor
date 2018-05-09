#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 风险模型中的市值因子
# @Filename: Size
# @Date:   : 2018-04-12 22:29
# @Author  : YuJun
# @Email   : yujun_mail@163.com


from src.factors.factor import Factor
import src.riskmodel.riskfactors.cons as risk_ct
import src.factors.cons as factor_ct
from src.util.utils import Utils
from src.util.dataapi.CDataHandler import  CDataHandler
import pandas as pd
import numpy as np
import logging
import os
import datetime
from multiprocessing import Pool, Manager
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


class LNCAP(Factor):
    """市值因子中的总市值自然对数类(Natural log of market cap)"""
    _db_file = os.path.join(factor_ct.FACTOR_DB.db_path, risk_ct.LNCAP_CT.db_file)

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        """
        计算指定日期、指定个股LNCAP因子载荷
        Parameters:
        --------
        :param code: str
            个股代码, 如SH600000, 600000
        :param calc_date: datetime-like, str
            计算日期, 格式: YYYY-MM-DD
        :return: pd.Series
        --------
            个股的LNCAP因子载荷
            0. code
            1. lncap
            如果计算失败, 返回None
        """
        # 取得个股的非复权收盘价
        df_secu_quote = Utils.get_secu_daily_mkt(code, start=calc_date, fq=False, range_lookup=True)
        if df_secu_quote is None:
            return None
        secu_close = df_secu_quote['close']
        # 取得个股最新的A股总股本数据
        cap_struct = Utils.get_cap_struct(code, calc_date)
        if cap_struct is None:
            return None
        total_share = cap_struct.total - cap_struct.liquid_b - cap_struct.liquid_h
        # 计算总市值的自然对数值
        lncap = np.log(secu_close * total_share)
        return pd.Series([Utils.code_to_symbol(code), lncap], index=['code', 'lncap'])

    @classmethod
    def _calc_factor_loading_proc(cls, code, calc_date, q):
        """
        用于并行计算因子载荷
        Parameters:
        --------
        :param code: str
            个股代码, 如SH600000, 600000
        :param calc_date: datetime-like, str
            计算日期, 格式: YYYY-MM-DD
        :param q: 队列, 用于进程间通信
        :return: 添加因子载荷至队列
        """
        logging.info('[{}] Calc LNCAP factor of {}.'.format(Utils.datetimelike_to_str(calc_date), code))
        lncap_data = None
        try:
            lncap_data = cls._calc_factor_loading(code, calc_date)
        except Exception as e:
            print(e)
        if lncap_data is not None:
            q.put(lncap_data)

    @classmethod
    def calc_factor_loading(cls, start_date, end_date=None, month_end=True, save=False, **kwargs):
        """
        计算指定日期的样本个股的因子载荷, 并保存至因子数据库
        Parameters:
        --------
        :param start_date: datetime-like, str
            开始日期, 格式: YYYY-MM-DD or YYYYMMDD
        :param end_date: datetime-like, str
            结束日期, 如果为None, 则只计算start_date日期的因子载荷, 格式: YYYY-MM-DD or YYYYMMDD
        :param month_end: bool, 默认为True
            如果为True, 则只计算月末时点的因子载荷
        :param save: bool, 默认为True
            是否保存至因子数据库
        :param kwargs:
            'multi_proc': bool, True=采用多进程, False=采用单进程, 默认为False
        :return: dict
            因子载荷数据
        """
        # 取得交易日序列及股票基本信息表
        start_date = Utils.to_date(start_date)
        if end_date is not None:
            end_date = Utils.to_date(end_date)
            trading_days_series = Utils.get_trading_days(start=start_date, end=end_date)
        else:
            trading_days_series = Utils.get_trading_days(end=start_date, ndays=1)
        all_stock_basics = CDataHandler.DataApi.get_secu_basics()
        # 遍历交易日序列, 计算LNCAP因子载荷
        dict_lncap = None
        for calc_date in trading_days_series:
            if month_end and (not Utils.is_month_end(calc_date)):
                continue
            logging.info('[%s] Calc LNCAP factor loading.' % Utils.datetimelike_to_str(calc_date))
            # 遍历个股, 计算个股的LNCAP因子值
            s = (calc_date - datetime.timedelta(days=risk_ct.LNCAP_CT.listed_days)).strftime('%Y%m%d')
            stock_basics = all_stock_basics[all_stock_basics.list_date < s]
            ids = []    # 个股代码list
            lncaps = [] # LNCAP因子值list

            if 'multi_proc' not in kwargs:
                kwargs['multi_proc'] = False
            if not kwargs['multi_proc']:
                # 采用单进程计算LNCAP因子值
                for _, stock_info in stock_basics.iterrows():
                    logging.info("[%s] Calc %s's LNCAP factor loading." % (calc_date.strftime('%Y-%m-%d'), stock_info.symbol))
                    lncap_data = cls._calc_factor_loading(stock_info.symbol, calc_date)
                    if lncap_data is not None:
                        ids.append(lncap_data['code'])
                        lncaps.append(lncap_data['lncap'])
            else:
                # 采用多进程并行计算LNCAP因子值
                q = Manager().Queue()   # 队列, 用于进程间通信, 存储每个进程计算的因子载荷
                p = Pool(4)             # 进程池, 最多同时开启4个进程
                for _, stock_info in stock_basics.iterrows():
                    p.apply_async(cls._calc_factor_loading_proc, args=(stock_info.symbol, calc_date, q,))
                p.close()
                p.join()
                while not q.empty():
                    lncap_data = q.get(True)
                    ids.append(lncap_data['code'])
                    lncaps.append(lncap_data['lncap'])

            date_label = Utils.get_trading_days(start=calc_date, ndays=2)[1]
            dict_lncap = {'date': [date_label]*len(ids), 'id': ids, 'factorvalue': lncaps}
            if save:
                Utils.factor_loading_persistent(cls._db_file, Utils.datetimelike_to_str(calc_date, dash=False), dict_lncap, ['date', 'id', 'factorvalue'])
            # 暂停180秒
            logging.info('Suspending for 180s.')
            time.sleep(180)
        return dict_lncap


if __name__ == '__main__':
    # pass
    LNCAP.calc_factor_loading(start_date='2017-12-29', end_date=None, month_end=False, save=True, multi_proc=True)
