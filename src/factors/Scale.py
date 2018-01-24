#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 
# @Filename: Scale
# @Date:   : 2018-01-08 17:34
# @Author  : YuJun
# @Email   : yujun_mail@163.com


from src.factors.factor import Factor
import src.factors.cons as factor_ct
from src.util.utils import Utils
from src.util.dataapi.CDataHandler import CDataHandler
from pandas import Series
import math
import os
from multiprocessing import Pool, Manager
import datetime
import logging
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


class Scale(Factor):
    """规模因子类"""
    _db_file = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.SCALE_CT.db_file)

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        """
        计算指定日期、指定个股的规模因子值
        Parameters:
        --------
        :param code: str
            个股代码，如600000、SH600000
        :param calc_date: datetime-like, str
            规模因子计算日期，格式YYYY-MM-DD或YYYYMMDD
        :return: pd.Series
        --------
            个股规模因子值，各个index对应的含义如下：
            0. LnTotalMktCap: 总市值对数
            1. LnLiquidMktCap: 流通市值对数
            若计算失败，返回None
        """
        # 取得证券截止指定日期最新的非复权行情数据
        code = Utils.code_to_symbol(code)
        calc_date = Utils.to_date(calc_date)
        mkt_daily = Utils.get_secu_daily_mkt(code, calc_date, fq=False, range_lookup=True)
        if mkt_daily.shape[0] == 0:
            return None
        # 取得证券截止指定日期前最新的股本结构数据
        cap_struct = Utils.get_cap_struct(code, calc_date)
        if cap_struct is None:
            return None
        # 计算证券的规模因子
        scale_factor = Series()
        total_cap = cap_struct.total - cap_struct.liquid_b - cap_struct.liquid_h
        scale_factor['LnTotalMktCap'] = math.log(total_cap * mkt_daily.close)
        scale_factor['LnLiquidMktCap'] = math.log(cap_struct.liquid_a * mkt_daily.close)
        return scale_factor

    @classmethod
    def _calc_factor_loading_proc(cls, code, calc_date, q):
        """
        用于并行计算因子载荷
        Parameters:
        --------
        :param code: str
            个股代码，如600000或SH600000
        :param calc_date: datetime-like or str
            计算日期，格式YYYY-MM-DD 或YYYYMMDD
        :param q: 队列，用于进程间通信
        :return: 添加因子载荷至队列中
        """
        logging.info('[%s] Calc market capitalization of %s' %(Utils.datetimelike_to_str(calc_date), code))
        mkt_cap = None
        try:
            mkt_cap = cls._calc_factor_loading(code, calc_date)
        except Exception as e:
            print(e)
        if mkt_cap is not None:
            # logging.info("[%s'] %s's total mkt cap = %.0f, liquid mkt cap = %.0f" % (Utils.datetimelike_to_str(calc_date), code, mkt_cap.LnTotalMktCap, mkt_cap.LnLiquidMktCap))
            q.put((Utils.code_to_symbol(code), mkt_cap.LnTotalMktCap, mkt_cap.LnLiquidMktCap))

    @classmethod
    def calc_factor_loading(cls, start_date, end_date=None, month_end=True, save=False, **kwargs):
        """
        计算指定日期的样本股的因子载荷，并保存至因子数据库
        Parameters:
        --------
        :param start_date: datetime-like or str
            开始日期，格式：YYYY-MM-DD or YYYYMMDD
        :param end_date: datetime-like or str
            结束日期，格式：YYYY-MM-DD or YYYYMMDD
        :param month_end: bool, 默认True
            如果为True，则只计算月末时点的因子载荷；否则每个交易日都计算
        :param save: bool, 默认False
            是否保存至因子数据库
        :return: 因子载荷，DataFrame
        --------
            因子载荷，DataFrame
            0. date: 日期
            1. id: 证券symbol
            2. LnTotalMktCap: 总市值对数值
            3. LnLiquidMktCap: 流通市值对数值
        """
        # 取得交易日序列股票基本信息表
        start_date = Utils.to_date(start_date)
        if end_date is not None:
            end_date = Utils.to_date(end_date)
            trading_days_series = Utils.get_trading_days(start=start_date, end=end_date)
        else:
            trading_days_series = Utils.get_trading_days(end=start_date, ndays=1)
        all_stock_basics = CDataHandler.DataApi.get_secu_basics()
        # 遍历交易日序列，计算规模因子值
        dict_scale = None
        for calc_date in trading_days_series:
            if month_end and (not Utils.is_month_end(calc_date)):
                continue
            dict_scale = {'date': [], 'id': [], 'LnTotalMktCap':[], 'LnLiquidMktCap': []}
            # 遍历个股，计算个股规模因子值
            s = (calc_date - datetime.timedelta(days=90)).strftime('%Y%m%d')
            stock_basics = all_stock_basics[all_stock_basics.list_date < s]

            # 采用单进程进行计算规模因子
            # for _, stock_info in stock_basics.iterrows():
            #     scale_data = cls._calc_factor_loading(stock_info.symbol, calc_date)
            #     if scale_data is not None:
            #         logging.info("[%s] %s's total mkt cap = %.0f, liquid mkt cap = %.0f" % (calc_date.strftime('%Y-%m-%d'), stock_info.symbol, scale_data.LnTotalMktCap, scale_data.LnLiquidMktCap))
            #         dict_scale['id'].append(Utils.code_to_symbol(stock_info.symbol))
            #         dict_scale['LnTotalMktCap'].append(round(scale_data.LnTotalMktCap, 4))
            #         dict_scale['LnLiquidMktCap'].append(round(scale_data.LnLiquidMktCap, 4))

            # 采用多进程并行计算规模因子
            q = Manager().Queue()   # 队列，用于进程间通信，存储每个进程计算的规模因子值
            p = Pool(4)             # 进程池，最多同时开启4个进程
            for _, stock_info in stock_basics.iterrows():
                p.apply_async(cls._calc_factor_loading_proc, args=(stock_info.symbol, calc_date, q,))
            p.close()
            p.join()
            while not q.empty():
                scale_data = q.get(True)
                dict_scale['id'].append(scale_data[0])
                dict_scale['LnTotalMktCap'].append(round(scale_data[1], 4))
                dict_scale['LnLiquidMktCap'].append(round(scale_data[2], 4))

            date_label = Utils.get_trading_days(start=calc_date, ndays=2)[1]
            dict_scale['date'] = [date_label] * len(dict_scale['id'])
            # 保存规模因子载荷至因子数据库
            if save:
                Utils.factor_loading_persistent(cls._db_file, calc_date.strftime('%Y%m%d'), dict_scale)
            # 休息60秒
            logging.info('Suspending for 60s.')
            time.sleep(60)
        return dict_scale


if __name__ == '__main__':
    # pass
    Scale.calc_factor_loading(start_date='2017-01-01', end_date='2017-12-31', month_end=True, save=True)