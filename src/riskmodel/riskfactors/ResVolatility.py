#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 风险模型的残差波动率因子类
# @Filename: ResVolatility
# @Date:   : 2018-04-10 18:27
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


class DASTD(Factor):
    """日波动率因子类(Daily standard deviation)"""
    _db_file = os.path.join(factor_ct.FACTOR_DB.db_path, risk_ct.DASTD_CT.db_file)

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        """
        计算指定日期、指定个股DASTD因子载荷
        Parameters:
        --------
        :param code: str
            个股代码, 如SH600000, 600000
        :param calc_date: datetime-like, str
            计算日期, 格式: YYYY-MM-DD
        :return: pd.Series
        --------
            个股的DASTD因子载荷
            0. code
            1. dastr
            如果计算失败, 返回None
        """
        # 取得个股复权行情数据
        df_secu_quote = Utils.get_secu_daily_mkt(code, end=calc_date, ndays=risk_ct.DASTD_CT.trailing+1, fq=True)
        if df_secu_quote is None:
            return None
        df_secu_quote.reset_index(drop=True, inplace=True)
        # 计算个股的日对数收益率序列及收益率均值
        arr_secu_close = np.array(df_secu_quote.iloc[1:]['close'])
        arr_secu_preclose = np.array(df_secu_quote.shift(1).iloc[1:]['close'])
        arr_secu_daily_ret = np.log(arr_secu_close / arr_secu_preclose)
        avg_daily_ret = np.mean(arr_secu_daily_ret)
        # 计算权重(指数移动加权平均)
        T = len(arr_secu_daily_ret)
        time_spans = sorted(range(T), reverse=True)
        alpha = 1 - np.exp(np.log(0.5)/risk_ct.DASTD_CT.half_life)
        x = [1-alpha] * T
        y = [alpha] * (T-1)
        y.insert(0, 1)
        weights = np.float_power(x, time_spans) * y
        # 计算个股DASTD因子值
        dastd = np.sqrt(np.sum((arr_secu_daily_ret - avg_daily_ret) ** 2 * weights))
        return pd.Series([code, dastd], index=['code', 'dastd'])

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
        logging.info('[%s] Calc DASTD factor of %s.' % (Utils.datetimelike_to_str(calc_date), code))
        dastd_data = None
        try:
            dastd_data = cls._calc_factor_loading(code, calc_date)
        except Exception as e:
            print(e)
        if dastd_data is not None:
            q.put(dastd_data)

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
        # 遍历交易日序列, 计算DASTD因子载荷
        dict_dastd = None
        for calc_date in trading_days_series:
            if month_end and (not Utils.is_month_end(calc_date)):
                continue
            logging.info('[%s] Calc DASTD factor loading.' % Utils.datetimelike_to_str(calc_date))
            # 遍历个股, 计算个股的DASTD因子值
            s = (calc_date - datetime.timedelta(days=risk_ct.DASTD_CT.listed_days)).strftime('%Y%m%d')
            stock_basics = all_stock_basics[all_stock_basics.list_date < s]
            ids = []        # 个股代码list
            dastds = []     # DASTD因子值list

            if 'multi_proc' not in kwargs:
                kwargs['multi_proc'] = False
            if not kwargs['multi_proc']:
                # 采用单进程计算DASTD因子值
                for _, stock_info in stock_basics.iterrows():
                    logging.info("[%s] Calc %s's DASTD factor loading." % (calc_date.strftime('%Y-%m-%d'), stock_info.symbol))
                    dastd_data = cls._calc_factor_loading(stock_info.symbol, calc_date)
                    if dastd_data is not None:
                        ids.append(dastd_data['code'])
                        dastds.append(dastd_data['dastd'])
            else:
                # 采用多进程并行计算DASTD因子值
                q = Manager().Queue()   # 队列, 用于进程间通信, 存储每个进程计算的因子载荷
                p = Pool(4)             # 进程池, 最多同时开启4个进程
                for _, stock_info in stock_basics.iterrows():
                    p.apply_async(cls._calc_factor_loading_proc, args=(stock_info.symbol, calc_date, q,))
                p.close()
                p.join()
                while not q.empty():
                    dastd_data = q.get(True)
                    ids.append(dastd_data['code'])
                    dastds.append(dastd_data['dastd'])

            date_label = Utils.get_trading_days(start=calc_date, ndays=2)[1]
            dict_dastd = {'date': [date_label]*len(ids), 'id': ids, 'factorvalue': dastds}
            if save:
                Utils.factor_loading_persistent(cls._db_file, Utils.datetimelike_to_str(calc_date, dash=False), dict_dastd, ['date', 'id', 'factorvalue'])
            # 暂停180秒
            logging.info('Suspending for 180s.')
            time.sleep(180)
        return dict_dastd


class CMRA(Factor):
    """Cumulative range风险因子类"""
    _db_file = os.path.join(factor_ct.FACTOR_DB.db_path, risk_ct.CMRA_CT.db_file)

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        """
        Parameter:
        --------
        :param code: str
            个股代码, 如SH600000, 600000
        :param calc_date: datetime-like, str
            计算日期, 格式: YYYY-MM-DD
        :return: pd.Series
        --------
            个股的CMRA因子载荷
            0. code
            1. cmra
            如果计算失败, 返回None
        """
        # 取得个股日复权行情数据
        df_secu_quote = Utils.get_secu_daily_mkt(code, end=calc_date,ndays=risk_ct.CMRA_CT.trailing*risk_ct.CMRA_CT.days_scale+1, fq=True)
        if df_secu_quote is None:
            return None
        if len(df_secu_quote) < risk_ct.CMRA_CT.listed_days:
            return None
        df_secu_quote.reset_index(drop=True, inplace=True)
        # 计算个股的日对数收益率序列
        # arr_secu_close = np.array(df_secu_quote.iloc[1:]['close'])
        # arr_secu_preclose = np.array(df_secu_quote.shift(1).iloc[1:]['close'])
        # arr_secu_daily_ret = np.log(arr_secu_close / arr_secu_preclose)
        # 每个月计算累积收益率
        # z = []
        # for t in range(1, risk_ct.CMRA_CT.trailing+1):
        #     k = t * risk_ct.CMRA_CT.days_scale - 1
        #     if k > len(arr_secu_daily_ret) - 1:
        #         k = len(arr_secu_daily_ret) - 1
        #         z.append(np.sum(arr_secu_daily_ret[:k]))
        #         break
        #     else:
        #         z.append(np.sum(arr_secu_daily_ret[:k]))

        # 计算每个月的个股价格变化率(1+r)
        z = []
        for t in range(1, risk_ct.CMRA_CT.trailing+1):
            k = t * risk_ct.CMRA_CT.days_scale
            if k > len(df_secu_quote)-1:
                k = len(df_secu_quote)-1
                z.append(df_secu_quote.iloc[k]['close']/df_secu_quote.iloc[0]['close'])
                break
            else:
                z.append(df_secu_quote.iloc[k]['close']/df_secu_quote.iloc[0]['close'])
        cmra = np.log(max(z)) - np.log(min(z))
        return pd.Series([code, cmra], index=['code', 'cmra'])

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
        logging.info('[%s] Calc CMRA factor of %s.' % (Utils.datetimelike_to_str(calc_date), code))
        cmra_data = None
        try:
            cmra_data = cls._calc_factor_loading(code, calc_date)
        except Exception as e:
            print(e)
        if cmra_data is not None:
            q.put(cmra_data)

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
        # 遍历交易日序列, 计算CMRA因子载荷
        dict_cmra = None
        for calc_date in trading_days_series:
            if month_end and (not Utils.is_month_end(calc_date)):
                continue
            logging.info('[%s] Calc CMRA factor loading.' % Utils.datetimelike_to_str(calc_date))
            # 遍历个股, 计算个股的CMRA因子值
            s = (calc_date - datetime.timedelta(days=risk_ct.CMRA_CT.listed_days)).strftime('%Y%m%d')
            stock_basics = all_stock_basics[all_stock_basics.list_date < s]
            ids = []        # 个股代码list
            cmras = []      # CMRA因子值list

            if 'multi_proc' not in kwargs:
                kwargs['multi_proc'] = False
            if not kwargs['multi_proc']:
                # 采用单进程计算CMRA因子值
                for _, stock_info in stock_basics.iterrows():
                    logging.info("[%s] Calc %s's CMRA factor loading." % (calc_date.strftime('%Y-%m-%d'), stock_info.symbol))
                    cmra_data = cls._calc_factor_loading(stock_info.symbol, calc_date)
                    if cmra_data is not None:
                        ids.append(cmra_data['code'])
                        cmras.append(cmra_data['cmra'])
            else:
                # 采用多进程并行计算CMRA因子值
                q = Manager().Queue()   # 队列, 用于进程间通信, 存储每个进程计算的因子载荷
                p = Pool(4)             # 进程池, 最多同时开启4个进程
                for _, stock_info in stock_basics.iterrows():
                    p.apply_async(cls._calc_factor_loading_proc, args=(stock_info.symbol, calc_date, q,))
                p.close()
                p.join()
                while not q.empty():
                    cmra_data = q.get(True)
                    ids.append(cmra_data['code'])
                    cmras.append(cmra_data['cmra'])

            date_label = Utils.get_trading_days(start=calc_date, ndays=2)[1]
            dict_cmra = {'date': [date_label]*len(ids), 'id': ids, 'factorvalue': cmras}
            if save:
                Utils.factor_loading_persistent(cls._db_file, Utils.datetimelike_to_str(calc_date, dash=False), dict_cmra, ['date', 'id', 'factorvalue'])
            # 暂停180秒
            logging.info('Suspending for 180s.')
            time.sleep(180)
        return dict_cmra


if __name__ == '__main__':
    pass
    # DASTD.calc_factor_loading(start_date='2017-12-29', end_date=None, month_end=False, save=True, multi_proc=True)
    CMRA.calc_factor_loading(start_date='2017-12-29', end_date=None, month_end=False, save=True, multi_proc=False)
    # CMRA.calc_secu_factor_loading(code='002129', calc_date='2017-12-29')
