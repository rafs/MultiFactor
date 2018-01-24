#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 传统的动量因子类
# @Filename: Momentum
# @Date:   : 2018-01-10 16:58
# @Author  : YuJun
# @Email   : yujun_mail@163.com


from src.factors.factor import Factor
import src.factors.cons as factor_ct
from src.util.utils import Utils
from src.util.dataapi.CDataHandler import CDataHandler
# import pandas as pd
from pandas import Series
import os
import datetime
import logging
from multiprocessing import Pool, Manager
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


class Momentum(Factor):
    """传统动量因子类"""
    _db_file = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.MOMENTUM_CT.db_file)     # 因子对应的数据库文件名

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        """
        计算指定日期、指定个股的动量因子，包含短期动量和长期动量因子
        Parameters:
        --------
        :param code: str
            个股代码，如SH600000或600000
        :param calc_date: datetime-like or str
            因子载荷计算日期，格式YYYY-MM-DD, YYYYMMDD
        :return: pd.Series
        --------
            传统动量因子值，分为短期和长期动量
            0. short_term_0: 短期动量0
            1. short_term_1: 短期动量1
            2. long_term_0: 长期动量0
            3. long_term_1: 长期动量1
            若计算失败，返回None
        """
        short_terms = [int(x) for x in factor_ct.MOMENTUM_CT.short_term_days.split('|')]    # 短期动量的交易日天数list
        long_terms = [int(x) for x in factor_ct.MOMENTUM_CT.long_term_days.split('|')]      # 长期动量的交易日天数list
        momentum_terms = short_terms + long_terms
        # 构造momentum_lable
        momentum_label = []
        for days in short_terms:
            momentum_label.append('short_term_%d' % days)
        for days in long_terms:
            momentum_label.append('long_term_%d' % days)
        # 计算动量
        momentum_value = []
        for days in momentum_terms:
            ret = Utils.calc_interval_ret(code, end=calc_date, ndays=days)
            if ret is None:
                if len(momentum_value) == 0:
                    return None     # 如果最短期的动量计算失败，那么返回None
                else:
                    ret = momentum_value[-1]
            momentum_value.append(round(ret, 6))
        momentum = Series(momentum_value, index=momentum_label)
        return momentum

    @classmethod
    def _calc_factor_loading_proc(cls, code, calc_date, q):
        """
        用于并行计算因子载荷
        Parameters:
        --------
        :param code: str
            个股代码，如600000或SH600000
        :param calc_date: datetime-like or str
            计算日期，格式YYYY-MM-DD or YYYYMMDD
        :param q: 队列，用于进程间通信
        :return: 添加因子载荷至队列中
        """
        logging.info('[%s] Calc Momentum factor of %s.' % (Utils.datetimelike_to_str(calc_date), code))
        momentum = None
        try:
            momentum = cls._calc_factor_loading(code, calc_date)
        except Exception as e:
            print(e)
        if momentum is not None:
            momentum['id'] = Utils.code_to_symbol(code)
            q.put(momentum)

    @classmethod
    def calc_factor_loading(cls, start_date, end_date=None, month_end=True, save=False, **kwargs):
        """
        计算指定日期的样本个股的因子载荷，并保存至因子数据库
        Parameters:
        --------
        :param start_date: datetime-like, str
            开始日期，格式：YYYY-MM-DD or YYYYMMDD
        :param end_date: datetime-like, str
            结束日期，格式：YYYY-MM-DD or YYYYMMDD
            如果为None，则只计算start_date日期的因子载荷
        :param month_end:bool, 默认True
            如果为True，则只结算月末时点的因子载荷
        :param save: bool, 默认False
            是否保存至因子数据库
        :return: 因子载荷，DataFrame
        --------
            因子载荷，DataFrame
            0. date: 日期
            1. id: 证券symbol
            2. short_term_0: 第一个短期动量因子
            3. short_term_1: 第二个短期动量因子
            4. long_term_0: 第一个长期动量因子
            5. long_term_1: 第二个长期动量因子
        """
        # 取得交易日序列及股票基本信息表
        # start_date = Utils.to_date(start_date)
        trading_days_series = Utils.get_trading_days(start=start_date, end=end_date)
        all_stock_basics = CDataHandler.DataApi.get_secu_basics()
        # 遍历交易日序列，计算动量因子
        dict_momentum = None
        momentum_label = cls.momentum_label()
        for calc_date in trading_days_series:
            if month_end and (not Utils.is_month_end(calc_date)):
                continue
            dict_momentum = {'date':[], 'id':[]}
            for label in momentum_label:
                dict_momentum[label] = []
            # 遍历个股，计算个股动量因子
            s = (calc_date - datetime.timedelta(days=90)).strftime('%Y%m%d')
            stock_basics = all_stock_basics[all_stock_basics.list_date < s]

            # 采用单进程进行计算
            # for _, stock_info in stock_basics.iterrows():
            #     momentum_data = cls._calc_factor_loading(stock_info.symbol, calc_date)
            #     if momentum_data is not None:
            #         logging.info("[%s] calc %s's momentum factor loading." % (calc_date.strftime('%Y-%m-%d'), stock_info.symbol))
            #         dict_momentum['id'].append(Utils.code_to_symbol(stock_info.symbol))
            #         for label in momentum_label:
            #             dict_momentum[label].append(momentum_data[label])

            # 采用多进程并行计算动量因子载荷
            q = Manager().Queue()   # 队列，用于进程间通信，存储每个进程计算的因子载荷
            p = Pool(4)             # 进程池，最多同时开启4个进程
            for _, stock_info in stock_basics.iterrows():
                p.apply_async(cls._calc_factor_loading_proc, args=(stock_info.symbol, calc_date, q,))
            p.close()
            p.join()
            while not q.empty():
                momentum_data = q.get(True)
                dict_momentum['id'].append(momentum_data['id'])
                for label in momentum_label:
                    dict_momentum[label].append(momentum_data[label])

            date_label = Utils.get_trading_days(start=calc_date, ndays=2)[1]
            dict_momentum['date'] = [date_label] * len(dict_momentum['id'])
            # 保存因子载荷至因子数据库
            if save:
                Utils.factor_loading_persistent(cls._db_file, calc_date.strftime('%Y%m%d'), dict_momentum)
            # 休息60秒
            logging.info('Suspending for 60s.')
            time.sleep(60)
        return dict_momentum


    @classmethod
    def momentum_label(cls):
        """取得动量因子的label"""
        short_terms = [int(x) for x in factor_ct.MOMENTUM_CT.short_term_days.split('|')]  # 短期动量的交易日天数list
        long_terms = [int(x) for x in factor_ct.MOMENTUM_CT.long_term_days.split('|')]  # 长期动量的交易日天数list
        # 构造momentum_lable
        momentum_label = []
        for days in short_terms:
            momentum_label.append('short_term_%d' % days)
        for days in long_terms:
            momentum_label.append('long_term_%d' % days)
        return momentum_label


if __name__ == '__main__':
    # pass
    # Momentum._calc_factor_loading('300355', '2012-12-31')
    Momentum.calc_factor_loading(start_date='2017-01-01', end_date='2017-12-31', month_end=True, save=True)
