#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 日内动量因子
# @Filename: IntradayMomentum
# @Date:   : 2017-12-28 17:29
# @Author  : YuJun
# @Email   : yujun_mail@163.com


from src.factors.factor import Factor
import src.factors.cons as factor_ct
from src.util.utils import Utils
from src.util.dataapi.CDataHandler import CDataHandler
# import pandas as pd
from pandas import DataFrame, Series
import os
import math
import datetime
import logging
from multiprocessing import Pool, Manager
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


class IntradayMomentum(Factor):
    """日内动量因子类"""
    __days = factor_ct.INTRADAYMOMENTUM_CT.days_num     # 读取过去多少天的分钟行情数据进行因子载荷计算
    _db_file = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.INTRADAYMOMENTUM_CT.db_file)     # 因子对应的数据库文件名

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        """
        计算指定日期、指定个股日内各时点动量值
        Parameters
        --------
        :param code: str
            个股代码，如600000或SH600000
        :param calc_date: datetime-like, str
            因子载荷计算日期，格式YYYY-MM-DD
        :return: pd.Series
        --------
            日内个时点的动量值，各个index对应的含义如下：
            0. m0: 隔夜时段动量
            1. m1: 第一个小时动量
            2. m2: 第二个小时动量
            3. m3: 第三个小时动量
            4. m4: 第四个小时动量
            5. m_normal: 传统动量
            若计算失败，返回None
        """
        # 取得过去90天的交易日序列，按日期降序排列
        trading_days = Utils.get_trading_days(end=calc_date, ndays=90, ascending=False)
        # 取得个股过去90天中的最近21天的1分钟行情数据，根据每天的分钟行情读取日内5个时点的价格，并计算日内收益值
        mkt_data = DataFrame()
        mkt_data_header = ['date', 'p0930', 'p1030', 'p1130', 'p1400', 'p1500']
        intra_day_ret = DataFrame()
        ret_header = ['date', 'r0', 'r1', 'r2', 'r3', 'r4']
        k = 0
        for trading_day in trading_days:
            df_1m_mkt = Utils.get_min_mkt(code, trading_day, fq=True)
            if df_1m_mkt is None:
                continue
            # 计算日内5个时点的价格
            time_label = '%s 09:31:00' % trading_day.strftime('%Y-%m-%d')
            p0930 = df_1m_mkt[df_1m_mkt.datetime == time_label].iloc[0].open
            time_label = '%s 10:30:00' % trading_day.strftime('%Y-%m-%d')
            p1030 = df_1m_mkt[df_1m_mkt.datetime == time_label].iloc[0].close
            time_label = '%s 11:30:00' % trading_day.strftime('%Y-%m-%d')
            p1130 = df_1m_mkt[df_1m_mkt.datetime == time_label].iloc[0].close
            time_label = '%s 14:00:00' % trading_day.strftime('%Y-%m-%d')
            p1400 = df_1m_mkt[df_1m_mkt.datetime == time_label].iloc[0].close
            time_label = '%s 15:00:00' % trading_day.strftime('%Y-%m-%d')
            p1500 = df_1m_mkt[df_1m_mkt.datetime == time_label].iloc[0].close
            s = Series([trading_day, p0930, p1030, p1130, p1400, p1500], index=mkt_data_header)
            mkt_data = mkt_data.append(s, ignore_index=True)
            # 计算日内收益
            if k > 0:
                r0 = math.log(mkt_data.iloc[k-1].p0930 / mkt_data.iloc[k].p1500)
                r1 = math.log(mkt_data.iloc[k-1].p1030 / mkt_data.iloc[k-1].p0930)
                r2 = math.log(mkt_data.iloc[k-1].p1130 / mkt_data.iloc[k-1].p1030)
                r3 = math.log(mkt_data.iloc[k-1].p1400 / mkt_data.iloc[k-1].p1130)
                r4 = math.log(mkt_data.iloc[k-1].p1500 / mkt_data.iloc[k-1].p1400)

                # r0 = mkt_data.iloc[k - 1].p0930 / mkt_data.iloc[k].p1500 -1.0
                # r1 = mkt_data.iloc[k - 1].p1030 / mkt_data.iloc[k - 1].p0930 - 1.0
                # r2 = mkt_data.iloc[k - 1].p1130 / mkt_data.iloc[k - 1].p1030 - 1.0
                # r3 = mkt_data.iloc[k - 1].p1400 / mkt_data.iloc[k - 1].p1130 - 1.0
                # r4 = mkt_data.iloc[k - 1].p1500 / mkt_data.iloc[k - 1].p1400 - 1.0

                s = Series([mkt_data.iloc[k-1].date, r0, r1, r2, r3, r4], index=ret_header)
                intra_day_ret = intra_day_ret.append(s, ignore_index=True)
            k += 1
            if k > cls.__days:
                break
        if k <= cls.__days:
            return None
        intra_day_ret = intra_day_ret.sort_values(by='date')
        # mkt_data = mkt_data.sort_values(by='date')
        # mkt_data = mkt_data.reset_index(drop=True)
        # 计算传统动量因子值，=过去20日的涨跌幅
        m_normal = math.log(mkt_data.iloc[0].p1500 / mkt_data.iloc[-1].p1500)

        # m_normal = mkt_data.iloc[0].p1500 / mkt_data.iloc[-1].p1500 - 1.0

        # 遍历上述取得的行情数据，计算每日的日内收益值
        # intra_day_ret = DataFrame()
        # ret_header = ['date', 'r0', 'r1', 'r2', 'r3', 'r4']
        # for k in range(1, len(mkt_data)):
        #     r0 = math.log(mkt_data.iloc[k].p0930 / mkt_data.iloc[k-1].p1500)
        #     r1 = math.log(mkt_data.iloc[k].p1030 / mkt_data.iloc[k].p0930)
        #     r2 = math.log(mkt_data.iloc[k].p1130 / mkt_data.iloc[k].p1030)
        #     r3 = math.log(mkt_data.iloc[k].p1400 / mkt_data.iloc[k].p1130)
        #     r4 = math.log(mkt_data.iloc[k].p1500 / mkt_data.iloc[k].p1400)
        #     s = Series([mkt_data.iloc[k].date, r0, r1, r2, r3, r4], index=ret_header)
        #     intra_day_ret = intra_day_ret.append(s, ignore_index=True)
        intra_day_ret = intra_day_ret.set_index('date')
        # 个股的日内各时点的动量因子值等于过去20个交易日各个r_i累加
        intra_day_momentum = intra_day_ret.sum()
        intra_day_momentum.index = ['m0', 'm1', 'm2', 'm3', 'm4']
        intra_day_momentum['m_normal'] = m_normal
        return intra_day_momentum

    @classmethod
    def _calc_factor_loading_proc(cls, code, calc_date, q):
        """
        用于并行计算因子载荷
        Parameters
        --------
        :param code: str
            个股代码，如600000或SH600000
        :param calc_date: datetime-like or str
            计算日期，格式：YYYY-MM-DD
        :param q: 队列，用于进程间通信
        :return: 添加因子载荷至队列中
        """
        logging.info('[%s] Calc Intaday Momentum of %s.' % (Utils.datetimelike_to_str(calc_date), code))
        momentum_data = None
        try:
            momentum_data = cls._calc_factor_loading(code, calc_date)
        except Exception as e:
            print(e)
        if momentum_data is not None:
            q.put((Utils.code_to_symbol(code), momentum_data.m0, momentum_data.m1, momentum_data.m2,
                   momentum_data.m3, momentum_data.m4, momentum_data.m_normal))

    @classmethod
    def calc_factor_loading(cls, start_date, end_date=None, month_end=True, save=False):
        """
        计算指定日期的样本个股的因子载荷，并保存至因子数据库
        Parameters
        --------
        :param start_date: datetime-like, str
            开始日期，格式：YYYY-MM-DD or YYYYMMDD
        :param end_date: datetime-like, str
            结束日期，如果为None，则只计算start_date日期的因子载荷，格式：YYYY-MM-DD or YYYYMMDD
        :param month_end: bool，默认True
            如果为True，则只计算月末时点的因子载荷
        :param save: bool，默认False
            是否保存至因子数据库
        :return: 因子载荷，DataFrame
        --------
            因子载荷，DataFrame
            0. date: 日期
            1. id: 证券symbol
            2. m0: 隔夜时段动量
            3. m1: 第一个小时动量
            4. m2: 第二个小时动量
            5. m3: 第三个小时动量
            6. m4: 第四个小时动量
            7. m_normal: 传统动量
        """
        # 取得交易日序列及股票基本信息表
        start_date = Utils.to_date(start_date)
        if end_date is not None:
            end_date = Utils.to_date(end_date)
            trading_days_series = Utils.get_trading_days(start=start_date, end=end_date)
        else:
            trading_days_series = Utils.get_trading_days(end=start_date, ndays=1)
        all_stock_basics = CDataHandler.DataApi.get_secu_basics()
        # 遍历交易日序列，计算日内动量因子值
        dict_intraday_momentum = None
        for calc_date in trading_days_series:
            if month_end and (not Utils.is_month_end(calc_date)):
                continue
            dict_intraday_momentum = {'date': [], 'id': [], 'm0': [], 'm1': [],
                                      'm2': [], 'm3': [], 'm4': [], 'm_normal': []}
            # 遍历个股，计算个股日内动量值
            s = (calc_date - datetime.timedelta(days=90)).strftime('%Y%m%d')
            stock_basics = all_stock_basics[all_stock_basics.list_date < s]

            # 采用单进程进行计算
            # for _, stock_info in stock_basics.iterrows():
            #     momentum_data = cls._calc_factor_loading(stock_info.symbol, calc_date)
            #     if momentum_data is not None:
            #         logging.info("[%s] %s's intraday momentum = (%0.4f,%0.4f,%0.4f,%0.4f,%0.4f,%0.4f)" % (calc_date.strftime('%Y-%m-%d'),stock_info.symbol, momentum_data.m0, momentum_data.m1, momentum_data.m2, momentum_data.m3, momentum_data.m4, momentum_data.m_normal))
            #         dict_intraday_momentum['id'].append(Utils.code_to_symbol(stock_info.symbol))
            #         dict_intraday_momentum['m0'].append(round(momentum_data.m0, 6))
            #         dict_intraday_momentum['m1'].append(round(momentum_data.m1, 6))
            #         dict_intraday_momentum['m2'].append(round(momentum_data.m2, 6))
            #         dict_intraday_momentum['m3'].append(round(momentum_data.m3, 6))
            #         dict_intraday_momentum['m4'].append(round(momentum_data.m4, 6))
            #         dict_intraday_momentum['m_normal'].append(round(momentum_data.m_normal, 6))

            # 采用多进程并行计算日内动量因子载荷
            q = Manager().Queue()   # 队列，用于进程间通信，存储每个进程计算的因子载荷
            p = Pool(4)             # 进程池，最多同时开启4个进程
            for _, stock_info in stock_basics.iterrows():
                p.apply_async(cls._calc_factor_loading_proc, args=(stock_info.symbol, calc_date, q,))
            p.close()
            p.join()
            while not q.empty():
                momentum_data = q.get(True)
                dict_intraday_momentum['id'].append(momentum_data[0])
                dict_intraday_momentum['m0'].append(round(momentum_data[1], 6))
                dict_intraday_momentum['m1'].append(round(momentum_data[2], 6))
                dict_intraday_momentum['m2'].append(round(momentum_data[3], 6))
                dict_intraday_momentum['m3'].append(round(momentum_data[4], 6))
                dict_intraday_momentum['m4'].append(round(momentum_data[5], 6))
                dict_intraday_momentum['m_normal'].append(round(momentum_data[6], 6))

            date_label = Utils.get_trading_days(calc_date, ndays=2)[1]
            dict_intraday_momentum['date'] = [date_label] * len(dict_intraday_momentum['id'])
            # 保存因子载荷至因子数据库
            if save:
                Utils.factor_loading_persistent(cls._db_file, calc_date.strftime('%Y%m%d'), dict_intraday_momentum)
            # 休息300秒
            logging.info('Suspending for 300s.')
            time.sleep(300)
        return dict_intraday_momentum


if __name__ == '__main__':
    # pass
    IntradayMomentum.calc_factor_loading(start_date='2013-02-01', end_date='2013-12-31', month_end=True, save=True)
