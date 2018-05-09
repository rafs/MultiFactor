#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 风险模型中的账面市值比因子
# @Filename: BTOP
# @Date:   : 2018-05-08 20:33
# @Author  : YuJun
# @Email   : yujun_mail@163.com


from src.factors.factor import Factor
import src.riskmodel.riskfactors.cons as risk_ct
import src.factors.cons as factor_ct
from src.util.utils import Utils
from src.util.dataapi.CDataHandler import  CDataHandler
from src.util.Cache import Cache
import pandas as pd
import numpy as np
import logging
import os
import datetime
from multiprocessing import Pool, Manager
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

class BTOP(Factor):
    """账面市值比因子类(Book-to-Price)"""
    _db_file = os.path.join(factor_ct.FACTOR_DB.db_path, risk_ct.BTOP_CT.db_file)

    _LNCAP_Cache = Cache(2)

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        """
        计算指定日期、指定个股的BTOP因子载荷
        Paramters:
        --------
        :param code: str
            个股代码, 如SH600000, 600000
        :param calc_date: datetime-like, str
            计算日期, 格式: YYYY-MM-DD
        :return: pd.Series
        --------
            个股的BTOP因子载荷
            0. code
            1. btop
            如果计算失败, 返回None
        """
        # 读取个股的财务数据
        fin_report_date = Utils.get_fin_report_date(calc_date)
        fin_basic_data = Utils.get_fin_basic_data(code, fin_report_date)
        if fin_basic_data is None:
            return None
        # 读取个股的市值因子(LNCAP)
        df_lncap = cls._LNCAP_Cache.get(Utils.datetimelike_to_str(calc_date, dash=False))
        if df_lncap is None:
            lncap_path = os.path.join(factor_ct.FACTOR_DB.db_path, risk_ct.LNCAP_CT.db_file)
            df_lncap = Utils.read_factor_loading(lncap_path, Utils.datetimelike_to_str(calc_date, dash=False))
            cls._LNCAP_Cache.set(Utils.datetimelike_to_str(calc_date, dash=False), df_lncap)
        secu_lncap = df_lncap[df_lncap['id'] == Utils.code_to_symbol(code)]
        if secu_lncap.empty:
            return None
        flncap = secu_lncap.iloc[0]['factorvalue']
        # 账面市值比=净资产/市值
        btop = (fin_basic_data['TotalAsset'] - fin_basic_data['TotalLiability']) * 10000 / np.exp(flncap)
        return pd.Series([Utils.code_to_symbol(code), btop], index=['code', 'btop'])

    @classmethod
    def _calc_factor_loading_proc(cls, code, calc_date, q):
        """
        用于并行计算因子治安和
        Parameters:
        --------
        :param code: str
            个股代码, 如SH600000, 600000
        :param calc_date: datetime-like, str
            计算日期, 格式: YYYY-MM-DD
        :param q: 队列, 用于进程间通信
        :return: 添加因子载荷至队列
        """
        logging.info('[{}] Calc BTOP factor of {}.'.format(Utils.datetimelike_to_str(calc_date), code))
        btop_data = None
        try:
            btop_data = cls._calc_factor_loading(code, calc_date)
        except Exception as e:
            print(e)
        if btop_data is not None:
            q.put(btop_data)

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
        # 遍历交易日序列, 计算BTOP因子载荷
        dict_btop = None
        for calc_date in trading_days_series:
            if month_end and (not Utils.is_month_end(calc_date)):
                continue
            logging.info('[%s] Calc BTOP factor loading.' % Utils.datetimelike_to_str(calc_date))
            # 遍历个股, 计算个股的BTOP因子值
            s = (calc_date - datetime.timedelta(days=risk_ct.BTOP_CT.listed_days)).strftime('%Y%m%d')
            stock_basics = all_stock_basics[all_stock_basics.list_date < s]
            ids = []    # 个股代码list
            btops = []  # BTOP因子值list

            if 'multi_proc' not in kwargs:
                kwargs['multi_proc'] = False
            if not kwargs['multi_proc']:
                # 采用单进程计算BTOP因子值
                for _, stock_info in stock_basics.iterrows():
                    logging.info("[%s] Calc %s's BTOP factor loading." % (Utils.datetimelike_to_str(calc_date, dash=True), stock_info.symbol))
                    btop_data = cls._calc_factor_loading(stock_info.symbol, calc_date)
                    if btop_data is not None:
                        ids.append(btop_data['code'])
                        btops.append(btop_data['btop'])
            else:
                # 采用多进程并行计算BTOP因子值
                q = Manager().Queue()   # 队列, 用于进程间通信, 存储每个进程计算的因子载荷
                p = Pool(4)             # 进程池, 最多同时开启4个进程
                for _, stock_info in stock_basics.iterrows():
                    p.apply_async(cls._calc_factor_loading_proc, args=(stock_info.symbol, calc_date, q,))
                p.close()
                p.join()
                while not q.empty():
                    btop_data = q.get(True)
                    ids.append(btop_data['code'])
                    btops.append(btop_data['btop'])

            date_label = Utils.get_trading_days(start=calc_date, ndays=2)[1]
            dict_btop = {'date': [date_label]*len(ids), 'id': ids, 'factorvalue': btops}
            if save:
                Utils.factor_loading_persistent(cls._db_file, Utils.datetimelike_to_str(calc_date, dash=False), dict_btop, ['date', 'id', 'factorvalue'])
            # 暂停180秒
            logging.info('Suspending for 180s.')
            time.sleep(180)
        return dict_btop


if __name__ == '__main__':
    # pass
    BTOP.calc_factor_loading(start_date='2017-12-29', end_date=None, month_end=False, save=True, multi_proc=True)
