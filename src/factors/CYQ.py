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
from src.util.dataapi.CDataHandler import CDataHandler
import pandas as pd
from pandas import DataFrame, Series
import numpy as np
import os
from pathlib import Path
import logging
import datetime
import calendar
from multiprocessing import Pool, Manager
import csv
import statsmodels.api as sm
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


class CYQ(Factor):
    """筹码分布因子类"""
    # __days = factor_ct.CYQ_CT.days_num      # 读取过去多少天的日K线行情数据进行因子载荷计算
    _db_file = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.CYQ_CT.db_file)      # 因子对应的数据库文件名
    # _db_proxies_path = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.CYQ_CT.proxies_db_file)  # 筹码分布代理变量保存文件的路径

    @classmethod
    def _calc_factor_loading1(cls, code, calc_date):
        """
        计算指定日期、指定个股筹码分布的四个代理变量以及下一期(下个月)的收益率
        Parameters
        -------
        :param code: str
            个股代码, 如600000或SH600000
        :param calc_date: datetime-like, str
            计算日期, 格式YYYY-MM-DD
        :return: pd.Series
        --------
            个股筹码分布的额四个代理变量
            0. arc: 筹码分布的均值
            1. vrc: 筹码分布的方差
            2. src: 筹码分布的偏度
            3. krc: 筹码分布的峰度
            4. next_ret: 下一期的收益率
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
            p_avg = df_mkt.iloc[j]['amount'] / df_mkt.iloc[j]['vol'] * df_mkt.iloc[j]['factor']
            arr_rc[j] = (p_c - p_avg) / p_c
            tr_j = df_mkt.iloc[j]['turnover1']
            if j == 0:
                arr_atr[j] = tr_j
            else:
                arr_atr[j] = arr_atr[j-1] / pre_tr * tr_j * ( 1. - pre_tr)
            pre_tr = tr_j
        arc = np.average(arr_rc, weights=arr_atr)
        if np.isnan(arc):
            return None
        rc_dev = arr_rc - arc
        n = len(df_mkt)
        vrc = n / (n-1.) * np.sum(arr_atr * rc_dev * rc_dev) / np.sum(arr_atr)
        if np.isnan(vrc):
            return None
        src = n / (n-1.) * np.sum(arr_atr * np.float_power(rc_dev, 3)) / np.sum(arr_atr) / np.float_power(vrc, 1.5)
        if np.isnan(src):
            return None
        krc = n / (n-1.) * np.sum(arr_atr * np.float_power(rc_dev, 4)) / np.sum(arr_atr) / np.float_power(vrc, 2)
        if np.isnan(krc):
            return None
        # 计算个股下一期的收益率
        # next_date = calc_date + datetime.timedelta(days=1)
        next_date = Utils.get_trading_days(start=calc_date, ndays=2)[1]
        wday, month_range = calendar.monthrange(next_date.year, next_date.month)
        date_end = datetime.datetime(next_date.year, next_date.month, month_range)
        next_ret = Utils.calc_interval_ret(code, start=next_date, end=date_end)
        if next_ret is None:
            return None
        else:
            return pd.Series([arc, vrc, src, krc, next_ret], index=['arc', 'vrc', 'src', 'krc', 'next_ret'])

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        """
        计算指定日期、指定个股筹码分布数据
        Parameters:
        --------
        :param code: str
            个股代码, 如600000或SH600000
        :param calc_date: datetime-like, str
            计算日期, 格式YYYY-MM-DD
        :return: tuple(code, close, cyq_data)
        --------
            1. code
            2. close: float
            个股在calc_date的收盘价
            3. cyq_data: pd.Series
            个股从IPO开始、至calc_date为止的筹码分布数据
            Series的index为筹码价格, values为对应每个筹码价格的持仓比例
            若计算失败, 返回None
        """
        # 读取个股IPO数据
        ipo_data = Utils.get_ipo_info(code)
        if ipo_data is None:
            return None
        if ipo_data['发行价格'][:-1] == '--':
            return None
        ipo_price = float(ipo_data['发行价格'][:-1])
        # 读取个股上市以来的日复权行情数据
        mkt_data = Utils.get_secu_daily_mkt(code, end=calc_date, fq=True)
        secu_close = mkt_data.iloc[-1]['close']
        # 计算每天的均价
        mkt_data['vwap'] = np.around(mkt_data['amount'] / mkt_data['vol'] * mkt_data['factor'], 2)
        mkt_data.dropna(axis=0, how='any', inplace=True)
        # 行情数据按日期降序排列
        mkt_data.sort_values(by='date', ascending=False, inplace=True)
        mkt_data.reset_index(drop=True, inplace=True)
        # 计算筹码分布
        cyq_data = mkt_data[['vwap', 'turnover1']]
        cyq_data = cyq_data.append(Series([ipo_price, 0], index=['vwap', 'turnover1']), ignore_index=True)
        cyq_data['minusTR'] = 1 - cyq_data['turnover1']
        cyq_data['cumprod_TR'] = cyq_data['minusTR'].cumprod().shift(1)
        cyq_data.loc[0, 'cumprod_TR'] = 1.
        cyq_data['cyq'] = cyq_data['turnover1'] * cyq_data['cumprod_TR']
        secu_cyq = cyq_data['cyq'].groupby(cyq_data['vwap']).sum()
        # 如果筹码价格数量小于30个, 返回None
        if len(secu_cyq) < 30:
            return None
        secu_cyq = secu_cyq[secu_cyq.values > 0.00001]
        return (Utils.code_to_symbol(code), secu_close, secu_cyq)

    @classmethod
    def _calc_factor_loading_proc1(cls, code, calc_date, q):
        """
        用于并行计算因子载荷
        Parameters:
        --------
        :param code: str
            个股代码, 如600000 or SH600000
        :param calc_date: datetime-like or str
            计算日期, 格式: YYYY-MM-DD
        :param q: 队列, 用于进程间通信
        :return: 添加因子载荷至队列中
        """
        logging.info('[%s] Calc CYQ factor of %s.' % (Utils.datetimelike_to_str(calc_date), code))
        cyq_data = None
        try:
            cyq_data = cls._calc_factor_loading(code, calc_date)
        except Exception as e:
            print(e)
        if cyq_data is not None:
            cyq_data['id'] = Utils.code_to_symbol(code)
            q.put(cyq_data)

    @classmethod
    def _calc_factor_loading_proc(cls, code, calc_date, q):
        """
        用于并行计算因子载荷
        Parameters:
        --------
        :param code: str
            个股代码, 如600000 or SH600000
        :param calc_date: datetime-like or str
            计算日期, 格式: YYYY-MM-DD
        :param q: 队列, 用于进程间通信
        :return: 添加因子载荷至队列中
        """
        logging.info('[%s] Calc CYQ factor of %s.' % (Utils.datetimelike_to_str(calc_date), code))
        cyq_data = None
        try:
            cyq_data = cls._calc_factor_loading(code, calc_date)
        except Exception as e:
            print(e)
        if cyq_data is not None:
            q.put(cyq_data)

    @classmethod
    def calc_factor_loading1(cls, start_date, end_date=None, month_end=True, save=False, **kwargs):
        """
        计算指定日期的样本个股的因子载荷, 并保存至因子数据库
        Parameters:
        --------
        :param start_date: datetime-like or str
            开始日期, 格式: YYYY-MM-DD or YYYYMMDD
        :param end_date: datetime-like, str
            结束日期, 如果为None, 则只计算start_date日期的因子载荷, 格式:YYYY-MM-DD or YYYYMMDD
        :param month_end: bool, 默认True
            如果为True, 则只计算月末时点的因子载荷
        :param save: bool, 默认True
            是否保存至因子数据库
        :param kwargs:
        :return: dict
            因子载荷
        --------
        """
        # 取得交易日序列及股票基本信息表
        start_date = Utils.to_date(start_date)
        if end_date is not None:
            end_date = Utils.to_date(end_date)
            trading_days_series = Utils.get_trading_days(start=start_date, end=end_date)
        else:
            trading_days_series = Utils.get_trading_days(end=start_date, ndays=1)
        all_stock_basics = CDataHandler.DataApi.get_secu_basics()
        # 遍历交易日序列, 计算筹码分布因子载荷
        dict_cyq = None
        for calc_date in trading_days_series:
            if month_end and (not Utils.is_month_end(calc_date)):
                continue
            logging.info('[%s] Calc CYQ factor loading.' % Utils.datetimelike_to_str(calc_date))
            # 遍历个股, 计算个股筹码分布因子值
            df_proxies = DataFrame()
            s = (calc_date - datetime.timedelta(days=365)).strftime('%Y%m%d')
            stock_basics = all_stock_basics[all_stock_basics.list_date < s]

            trading_day = Utils.get_trading_days(calc_date, ndays=2)[1]
            # 采用单进程计算筹码因子分布的代理变量
            # for _, stock_info in stock_basics.iterrows():
            #     cyq_proxies = cls._calc_factor_loading(stock_info.symbol, calc_date)
            #     if cyq_proxies is not None:
            #         logging.info("[%s] %s's cyq proxies = (%0.4f,%0.4f,%0.4f,%0.4f,%0.4f)" % (calc_date.strftime('%Y-%m-%d'), stock_info.symbol, cyq_proxies['arc'], cyq_proxies['vrc'], cyq_proxies['src'], cyq_proxies['krc'], cyq_proxies['next_ret']))
            #         # cyq_proxies['date'] = trading_day
            #         cyq_proxies['id'] = Utils.code_to_symbol(stock_info.symbol)
            #         df_proxies = df_proxies.append(cyq_proxies, ignore_index=True)

            # 采用多进程进行并行计算筹码分布因子的代理变量
            q = Manager().Queue()   # 队列, 用于进程间通信, 存储每个进程计算的因子载荷
            p = Pool(4)             # 进程池, 最多同时开启4个进程
            for _, stock_info in stock_basics.iterrows():
                p.apply_async(cls._calc_factor_loading_proc, args=(stock_info.symbol, calc_date, q,))
            p.close()
            p.join()
            while not q.empty():
                cyq_proxies = q.get(True)
                # cyq_proxies['date'] = trading_day
                df_proxies = df_proxies.append(cyq_proxies, ignore_index=True)

            # 保存筹码分布代理变量数据
            df_proxies['date'] = trading_day
            proxies_file_path = cls._db_proxies_path + '_%s.csv' % Utils.datetimelike_to_str(calc_date, dash=False)
            df_proxies.to_csv(proxies_file_path, index=False, columns=['date', 'id', 'arc', 'vrc', 'src', 'krc', 'next_ret'])

            # 导入筹码分布因子的代理变量数据
            # cyq_proxies_path = cls._db_proxies_path + '_%s.csv' % Utils.datetimelike_to_str(calc_date, dash=False)
            # df_proxies = pd.read_csv(cyq_proxies_path, header=0)

            # 计算marc, 代理变量权重及筹码分布因子载荷
            marc = df_proxies['arc'].median()
            proxies_weight_file = Path(factor_ct.FACTOR_DB.db_path ,factor_ct.CYQ_CT.proxies_weight_file)
            if proxies_weight_file.exists():
                df_proxies_weight = pd.read_csv(proxies_weight_file, header=0, parse_dates=[0])
                df_proxies_weight = df_proxies_weight[df_proxies_weight.date < calc_date].tail(24)
                if len(df_proxies_weight) < 24:
                    with open(proxies_weight_file, 'a', newline='') as f:
                        csv_writer = csv.writer(f)
                        csv_writer.writerow([calc_date.strftime('%Y-%m-%d'), marc, 0, 0, 0, 0, 0])
                else:
                    df_proxies_data = DataFrame()
                    if marc > 0:
                        df_proxies_weight = df_proxies_weight[df_proxies_weight.marc > 0]
                    elif marc < 0:
                        df_proxies_weight = df_proxies_weight[df_proxies_weight.marc < 0]
                    for _, weight_info in df_proxies_weight.iterrows():
                        proxies_file_path = cls._db_proxies_path + '_%s.csv' % Utils.datetimelike_to_str(weight_info['date'], False)
                        df_proxies_data = df_proxies_data.append(pd.read_csv(proxies_file_path, header=0), ignore_index=True)
                    next_ret = np.array(df_proxies_data['next_ret'])
                    cyq_data = np.array(df_proxies_data[['arc', 'vrc', 'src', 'krc']])
                    cyq_data = sm.add_constant(cyq_data)
                    cyq_model = sm.OLS(next_ret, cyq_data)
                    cyq_result = cyq_model.fit()
                    cyq_weights = np.around(cyq_result.params, 6)
                    with open(proxies_weight_file, 'a', newline='') as f:
                        csv_writer = csv.writer(f)
                        csv_writer.writerow([calc_date.strftime('%Y-%m-%d'), marc, cyq_weights[0], cyq_weights[1], cyq_weights[2], cyq_weights[3], cyq_weights[4]])
                    # 计算筹码分布因子载荷
                    arr_proxies = np.array(df_proxies[['arc', 'vrc', 'src', 'krc']])
                    arr_weight = np.array([cyq_weights[1], cyq_weights[2], cyq_weights[3], cyq_weights[4]]).reshape((4,1))
                    intercept = cyq_weights[0]
                    arr_cyq = np.around(np.dot(arr_proxies, arr_weight)+intercept, 6)
                    dict_cyq = {'date': list(df_proxies['date']), 'id': list(df_proxies['id']), 'factorvalue': list(arr_cyq.reshape((len(arr_cyq),)))}
                    # 保存因子载荷至因子数据库
                    if save:
                        Utils.factor_loading_persistent(cls._db_file, calc_date.strftime('%Y%m%d'), dict_cyq, columns=['date', 'id', 'factorvalue'])
            else:
                with open(proxies_weight_file, 'w', newline='') as f:
                    csv_writer = csv.writer(f)
                    csv_writer.writerow(['date', 'marc', 'intcpt', 'arc_w', 'vrc_w', 'src_w', 'krc_w'])
                    csv_writer.writerow([calc_date.strftime('%Y-%m-%d'), marc, 0, 0, 0, 0, 0])
            # 休息300秒
            logging.info('Suspending for 200s.')
            time.sleep(200)

    @classmethod
    def calc_factor_loading(cls, start_date, end_date=None, month_end=True, save=False, **kwargs):
        """
        计算指定日期的样本个股的因子载荷, 并保存至因子数据库
        Parameters:
        --------
        :param start_date: datetime-like or str
            开始日期, 格式: YYYY-MM-DD or YYYYMMDD
        :param end_date: datetime-like, str
            结束日期, 如果为None, 则只计算start_date日期的因子载荷, 格式:YYYY-MM-DD or YYYYMMDD
        :param month_end: bool, 默认True
            如果为True, 则只计算月末时点的因子载荷
        :param save: bool, 默认True
            是否保存至因子数据库
        :param kwargs:
        :return: dict
            因子载荷
        --------
        """
        # 取得交易日序列及股票基本信息表
        start_date = Utils.to_date(start_date)
        if end_date is not None:
            end_date = Utils.to_date(end_date)
            trading_days_series = Utils.get_trading_days(start=start_date, end=end_date)
        else:
            trading_days_series = Utils.get_trading_days(end=start_date, ndays=1)
        all_stock_basics = CDataHandler.DataApi.get_secu_basics()
        # 遍历交易日序列, 计算筹码分布因子载荷
        dict_cyq = {}
        for calc_date in trading_days_series:
            if month_end and (not Utils.is_month_end(calc_date)):
                continue
            logging.info('[%s] Calc CYQ factor loading.' % Utils.datetimelike_to_str(calc_date))
            # 遍历个股, 计算个股筹码分布因子值
            s= (calc_date - datetime.timedelta(days=180)).strftime('%Y%m%d')
            stock_basics = all_stock_basics[all_stock_basics.list_date < s]

            secu_cyq_path = Path(factor_ct.FACTOR_DB.db_path, factor_ct.CYQ_CT.db_file,
                                 'secu_cyq/%s' % calc_date.strftime('%Y-%m-%d'))
            if not secu_cyq_path.exists():
                secu_cyq_path.mkdir()
            ids = []
            rps = []

            # 采用单进程计算筹码分布数据, 及当前价格的相对位置(=当前价格-平均成本)/平均成本
            # for _, stock_info in stock_basics.iterrows():
            #     logging.info("[%s] Calc %s's cyq data." % (calc_date.strftime('%Y-%m-%d'), stock_info.symbol))
            #     secu_cyq = cls._calc_factor_loading(stock_info.symbol, calc_date)
            #     if secu_cyq is not None:
            #         secu_code, secu_close, cyq_data = secu_cyq
            #         # 保存个股的筹码分布数据
            #         cyq_data.to_csv(Path(secu_cyq_path, '%s.csv' % secu_code), header=True)
            #         # 计算当前价格的相对位置
            #         avg_cyq = np.sum(np.array(cyq_data.index) * np.array(cyq_data.values))
            #         relative_position = round((secu_close - avg_cyq) / avg_cyq, 4)
            #         ids.append(secu_code)
            #         rps.append(relative_position)

            # 采用多进程进行并行计算筹码分布数据, 及当前价格的相对位置(=当前价格-平均成本)/平均成本
            q = Manager().Queue()   # 队列, 用于进程间通信, 存储每个进程计算的因子载荷
            p = Pool(4)             # 进程池, 最多同时开启4个进程
            for _, stock_info in stock_basics.iterrows():
                p.apply_async(cls._calc_factor_loading_proc, args=(stock_info.symbol, calc_date, q, ))
            p.close()
            p.join()
            while not q.empty():
                secu_cyq = q.get(True)
                secu_code, secu_close, cyq_data = secu_cyq
                # 保存个股的筹码分布数据
                cyq_data.to_csv(Path(secu_cyq_path, '%s.csv' % secu_code), header=True)
                # 计算当前价格的相对位置
                avg_cyq = np.sum(np.array(cyq_data.index) * np.array(cyq_data.values))
                relative_position = round((secu_close - avg_cyq) / avg_cyq, 4)
                ids.append(secu_code)
                rps.append(relative_position)

            date_label = Utils.get_trading_days(calc_date, ndays=2)[1]
            dict_cyq = {'date': [date_label]*len(ids), 'id': ids, 'factorvalue': rps}
            if save:
                cyq_data_path = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.CYQ_CT.db_file, factor_ct.CYQ_CT.CYQ_rp_file)
                Utils.factor_loading_persistent(cyq_data_path, Utils.datetimelike_to_str(calc_date, dash=False), dict_cyq, ['date', 'id', 'factorvalue'])
            # 休息90秒
            logging.info('Suspending for 100s.')
            time.sleep(100)
        return dict_cyq


if __name__ == '__main__':
    # pass
    CYQ.calc_factor_loading(start_date='2017-5-1', end_date='2017-12-31', month_end=True, save=True)
    # CYQ._calc_factor_loading('000722', '2006-12-29')
