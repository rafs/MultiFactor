#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 风险模型的Momentum类因子
# @Filename: Momentum
# @Date:   : 2018-04-09 18:17
# @Author  : YuJun
# @Email   : yujun_mail@163.com


from src.factors.factor import Factor
import src.riskmodel.riskfactors.cons as risk_ct
import src.factors.cons as factor_ct
from src.util.utils import Utils
from src.util.dataapi.CDataHandler import CDataHandler
import pandas as pd
import numpy as np
import logging
import os
import datetime
from multiprocessing import Pool, Manager
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


class RSTR(Factor):
    """相对强度因子类"""
    _db_file = os.path.join(factor_ct.FACTOR_DB.db_path, risk_ct.RSTR_CT.db_file)

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        """
        计算指定日期、指定个股RSTR因子载荷
        Parameters:
        --------
        :param code: str
            个股代码, 如SH600000, 600000
        :param calc_date: datetime-like, str
            计算日期, 格式: YYYY-MM-DD
        :return: pd.Series
        --------
            个股的RSTR因子载荷
            0. code
            1. rstr
            如果计算失败, 返回None
        """
        # 取得个股复权行情数据
        df_secu_quote = Utils.get_secu_daily_mkt(code, end=calc_date, ndays=risk_ct.RSTR_CT.trailing_start+1, fq=True)
        if df_secu_quote is None:
            return None
        if len(df_secu_quote) < risk_ct.RSTR_CT.half_life * 2:
            return None
        df_secu_quote = df_secu_quote.head(len(df_secu_quote) - risk_ct.RSTR_CT.trailing_end)
        df_secu_quote.reset_index(drop=True, inplace=True)
        # 计算个股的日对数收益率
        arr_secu_close = np.array(df_secu_quote.iloc[1:]['close'])
        arr_secu_preclose = np.array(df_secu_quote.shift(1).iloc[1:]['close'])
        arr_secu_daily_ret = np.log(arr_secu_close / arr_secu_preclose)
        # 计算权重(指数移动加权平均)
        T = len(arr_secu_daily_ret)
        time_spans = sorted(range(T), reverse=True)
        alpha = 1 - np.exp(np.log(0.5)/risk_ct.RSTR_CT.half_life)
        x = [1-alpha] * T
        y = [alpha] * (T-1)
        y.insert(0, 1)
        weights = np.float_power(x, time_spans) * y
        # 计算RSTR
        rstr = np.sum(arr_secu_daily_ret * weights)
        return pd.Series([code, rstr], index=['code', 'rstr'])

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
        :return: 添加因子载荷至队列中
        """
        logging.info('[%s] Calc RSTR factor of %s.' % (Utils.datetimelike_to_str(calc_date), code))
        rstr_data = None
        try:
            rstr_data = cls._calc_factor_loading(code, calc_date)
        except Exception as e:
            print(e)
        if rstr_data is not None:
            q.put(rstr_data)

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
            因子载荷
        """
        # 取得交易日序列及股票基本信息表
        start_date = Utils.to_date(start_date)
        if end_date is not None:
            end_date = Utils.to_date(end_date)
            trading_days_series = Utils.get_trading_days(start=start_date, end=end_date)
        else:
            trading_days_series = Utils.get_trading_days(end=start_date, ndays=1)
        all_stock_basics = CDataHandler.DataApi.get_secu_basics()
        # 遍历交易日序列, 计算RSTR因子载荷
        dict_rstr = {}
        for calc_date in trading_days_series:
            if month_end and (not Utils.is_month_end(calc_date)):
                continue
            logging.info('[%s] Calc RSTR factor loading.' % Utils.datetimelike_to_str(calc_date))
            # 遍历个股, 计算个股的RSTR因子值
            s = (calc_date - datetime.timedelta(days=risk_ct.RSTR_CT.half_life*3)).strftime('%Y%m%d')
            stock_basics = all_stock_basics[all_stock_basics.list_date < s]
            ids = []        # 个股代码list
            rstrs = []      # RSTR因子值list

            if 'multi_proc' not in kwargs:
                kwargs['multi_proc'] = False
            if not kwargs['multi_proc']:
                # 采用单进程计算RSTR因子值
                for _, stock_info in stock_basics.iterrows():
                    logging.info("[%s] Calc %s's RSTR factor loading." % (calc_date.strftime('%Y-%m-%d'), stock_info.symbol))
                    rstr_data = cls._calc_factor_loading(stock_info.symbol, calc_date)
                    if rstr_data is not None:
                        ids.append(rstr_data['code'])
                        rstrs.append(rstr_data['rstr'])
            else:
                # 采用多进程并行计算RSTR因子值
                q = Manager().Queue()   # d队列, 用于进程间通信, 存储每个进程计算的因子载荷
                p = Pool(4)             # 进程池, 最多同时开启4个进程
                for _, stock_info in stock_basics.iterrows():
                    p.apply_async(cls._calc_factor_loading_proc, args=(stock_info.symbol, calc_date, q,))
                p.close()
                p.join()
                while not q.empty():
                    rstr_data = q.get(True)
                    ids.append(rstr_data['code'])
                    rstrs.append(rstr_data['rstr'])

            date_label = Utils.get_trading_days(start=calc_date, ndays=2)[1]
            dict_rstr = {'date': [date_label]*len(ids), 'id': ids, 'factorvalue': rstrs}
            if save:
                Utils.factor_loading_persistent(cls._db_file, Utils.datetimelike_to_str(calc_date, dash=False), dict_rstr, ['date', 'id', 'factorvalue'])
            # 暂停180秒
            logging.info('Suspending for 180s.')
            time.sleep(180)
        return dict_rstr


if __name__ == '__main__':
    # pass
    RSTR.calc_factor_loading(start_date='2017-12-29', end_date=None, month_end=False, save=True, multi_proc=True)
