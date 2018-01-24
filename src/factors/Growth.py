#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 成长类因子
# @Filename: Growth
# @Date:   : 2018-01-16 18:41
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


class Growth(Factor):
    """
    成长类因子
    --------
    包含：npg_ttm(净利润增长率_TTM), opg_ttm(营业收入增长率_TTM)
    --------
    """
    _db_file = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.GROWTH_CT.db_file)

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        """
        计算指定日期、指定个股的成长因子，包含npg_ttm, opg_ttm
        Parameters:
        --------
        :param code: str
            个股代码，如600000或SH600000
        :param calc_date: datetime-like or str
            计算日期，格式YYYY-MM-DD, YYYYMMDD
        :return: pd.Series
        --------
            成长类因子值
            0. id: 证券代码
            1. npg_ttm: 净利润增长率_TTM
            2. opg_ttm: 营业收入增长率_TTM
            若计算失败, 返回None
        """
        code = Utils.code_to_symbol(code)
        calc_date = Utils.to_date(calc_date)
        # 读取最新的TTM财务数据
        ttm_fin_data_latest = Utils.get_ttm_fin_basic_data(code, calc_date)
        if ttm_fin_data_latest is None:
            return None
        # 读取去年同期TTM财务数据
        try:
            pre_date = datetime.datetime(calc_date.year-1, calc_date.month, calc_date.day)
        except ValueError:
            pre_date = calc_date - datetime.timedelta(days=366)
        ttm_fin_data_pre = Utils.get_ttm_fin_basic_data(code, pre_date)
        if ttm_fin_data_pre is None:
            return None
        # 计算成长类因子值
        if abs(ttm_fin_data_pre['NetProfit']) < 0.1:
            return None
        npg_ttm = (ttm_fin_data_latest['NetProfit'] - ttm_fin_data_pre['NetProfit']) / abs(ttm_fin_data_pre['NetProfit'])
        if abs(ttm_fin_data_pre['MainOperateRevenue']) < 0.1:
            return None
        opg_ttm = (ttm_fin_data_latest['MainOperateRevenue'] - ttm_fin_data_pre['MainOperateRevenue']) / abs(ttm_fin_data_pre['MainOperateRevenue'])
        return Series([code, round(npg_ttm, 4), round(opg_ttm, 4)], index=['id', 'npg_ttm', 'opg_ttm'])

    @classmethod
    def _calc_factor_loading_proc(cls, code, calc_date, q):
        """
        用于并行计算因子载荷
        Parameters:
        --------
        :param code: str
            个股代码，如600000或SH600000
        :param calc_date: datetime-like, str
            计算日期，格式：YYYY-MM-DD or YYYYMMDD
        :param q: 队列，用于进程间通信
        :return: 添加因子载荷至队列中
        """
        logging.info('[%s] Calc Growth factor of %s.' % (Utils.datetimelike_to_str(calc_date), code))
        growth = None
        try:
            growth = cls._calc_factor_loading(code, calc_date)
        except Exception as e:
            print(e)
        if growth is not None:
            q.put(growth)

    @classmethod
    def calc_factor_loading(cls, start_date, end_date=None, month_end=True, save=False, **kwargs):
        """
        计算指定日期的样本个股的因子载荷，并保存至因子数据库
        Parameters:
        --------
        :param start_date: datetime-like, str
            开始日期，格式：YYYY-MM-DD or YYYYMMDD
        :param end_date: datetime-like, str; 默认为None
            结束日期，格式：YYYY-MM-DD or YYYYMMDD
            如果为None，则只计算start_date日期的因子载荷
        :param month_end: bool, 默认True
            如果为True，则只计算月末时点的因子载荷
        :param save: bool, 默认False
            是否保存至因子数据库
        :return: 因子载荷，pd.DataFrame
        --------
            因子载荷，pd.DataFrame
            0. date: 日期
            1. id: 日期
            2. npg_ttm: 净利润增长率_TTM
            3. opg_ttm: 营业收入增长率_TTM
        """
        # 取得交易日序列及股票基本信息表
        trading_days_series = Utils.get_trading_days(start=start_date, end=end_date)
        all_stock_basics = CDataHandler.DataApi.get_secu_basics()
        # 遍历交易日序列，计算价值因子载荷
        dict_growth = None
        for calc_date in trading_days_series:
            if month_end and (not Utils.is_month_end(calc_date)):
                continue
            dict_growth = {'date': [], 'id': [], 'npg_ttm': [], 'opg_ttm': []}
            # 遍历个股，计算个股成长因子载荷
            s = (calc_date - datetime.timedelta(days=90)).strftime('%Y%m%d')
            stock_basics = all_stock_basics[all_stock_basics.list_date < s]

            # 采用单进程进行计算成长因子
            # for _, stock_info in stock_basics.iterrows():
            #     logging.info("[%s] calc %s's growth factor loading." % (calc_date.strftime('%Y-%m-%d'), stock_info.symbol))
            #     growth_data = cls._calc_factor_loading(stock_info.symbol, calc_date)
            #     if growth_data is not None:
            #         dict_growth['id'].append(Utils.code_to_symbol(stock_info.symbol))
            #         dict_growth['npg_ttm'].append(growth_data['npg_ttm'])
            #         dict_growth['opg_ttm'].append(growth_data['opg_ttm'])

            # 采用多进程并行计算成长因子
            q = Manager().Queue()   # 队列，用于进程间通信，存储每个进程计算的因子载荷
            p = Pool(4)             # 进程池，最多同时开启4个进程
            for _, stock_info in stock_basics.iterrows():
                p.apply_async(cls._calc_factor_loading_proc, args=(stock_info.symbol, calc_date, q,))
            p.close()
            p.join()
            while not q.empty():
                growth_data = q.get(True)
                dict_growth['id'].append(growth_data['id'])
                dict_growth['npg_ttm'].append(growth_data['npg_ttm'])
                dict_growth['opg_ttm'].append(growth_data['opg_ttm'])

            date_label = Utils.get_trading_days(start=calc_date, ndays=2)[1]
            dict_growth['date'] = [date_label] * len(dict_growth['id'])
            # 保存因子载荷至因子数据库
            if save:
                columns = ['date', 'id', 'npg_ttm', 'opg_ttm']
                Utils.factor_loading_persistent(cls._db_file, calc_date.strftime('%Y%m%d'), dict_growth, columns)
            # 休息120秒
            logging.info('Suspending for 120s.')
            time.sleep(120)
        return dict_growth


if __name__ == '__main__':
    # pass
    Growth._calc_factor_loading('603818', '2016-02-29')
    # Growth.calc_factor_loading(start_date='2016-02-01', end_date='2016-02-29', month_end=True, save=True)
