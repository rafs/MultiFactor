#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 价值类因子
# @Filename: Value
# @Date:   : 2018-01-15 23:45
# @Author  : YuJun
# @Email   : yujun_mail@163.com


from src.factors.factor import Factor
import src.factors.cons as factor_ct
import src.util.cons as util_ct
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
    包含：ep_ttm（TTM净利润/总市值）, bp_lr（净资产(最新财报)/总市值）, ocf_ttm（TTM经营性现金流/总市值）
    --------
    """
    _db_file = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.VALUE_CT.db_file)

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        """
        计算指定日期、指定个股的价值因子，包含ep_ttm, bp_lr, ocf_ttm
        Parameters:
        --------
        :param code: str
            个股代码：如600000或SH600000
        :param calc_date: datetime-like or str
            计算日期，格式YYYY-MM-DD, YYYYMMDD
        :return: pd.Series
        --------
            价值类因子值
            0. ep_ttm: TTM净利润/总市值
            1. bp_lr: 净资产（最新财报）/总市值
            2. ocf_ttm: TTM经营性现金流/总市值
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
        ep_ttm = ttm_fin_data['NetProfit'] * util_ct.FIN_DATA_AMOUNT_UNIT / total_mkt_cap
        ocf_ttm = ttm_fin_data['NetOperateCashFlow'] * util_ct.FIN_DATA_AMOUNT_UNIT / total_mkt_cap
        bp_lr = fin_basic_data['ShareHolderEquity'] * util_ct.FIN_DATA_AMOUNT_UNIT / total_mkt_cap
        return Series([round(ep_ttm, 6), round(bp_lr, 6), round(ocf_ttm, 6)], index=['ep_ttm', 'bp_lr', 'ocf_ttm'])

    @classmethod
    def _calc_factor_loading_proc(cls, code, calc_date, q):
        """
        用于并行计算因子载荷
        Parameters:
        --------
        :param code: str
            个股代码，如600000或SH600000
        :param calc_date: datetime-like or str
            计算日期，格式：YYYY-MM-DD or YYYYMMMDD
        :param q: 队列，用于进程间通信
        :return: 添加因子载荷至队列中
        """
        logging.info('[%s] Calc Value factor of %s.' % (Utils.datetimelike_to_str(calc_date), code))
        value = None
        try:
            value = cls._calc_factor_loading(code, calc_date)
        except Exception as e:
            print(e)
        if value is not None:
            value['id'] = Utils.code_to_symbol(code)
            q.put(value)

    @classmethod
    def calc_factor_loading(cls, start_date, end_date=None, month_end=True, save=False, **kwargs):
        """
        计算指定日期的样本个股的因子载荷，并保存至因子数据库
        Parameters:
        --------
        :param start_date: datetime-like, str
            开始日期，格式：YYYY-MM-DD or YYYYMMDD
        :param end_date: datetime-like, str；默认为None
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
            1. id: 证券symbol
            2. ep_ttm: TTM净利润/总市值
            3. bp_lr: 净资产（最新财报）/总市值
            4. ocf_ttm: TTM经营性现金流/总市值
        """
        # 取得交易日序列及股票基本信息表
        trading_days_series = Utils.get_trading_days(start=start_date, end=end_date)
        all_stock_basics = CDataHandler.DataApi.get_secu_basics()
        # 遍历交易日序列，计算价值因子载荷
        dict_value = None
        for calc_date in trading_days_series:
            if month_end and (not Utils.is_month_end(calc_date)):
                continue
            dict_value = {'date': [], 'id': [], 'ep_ttm': [], 'bp_lr': [], 'ocf_ttm': []}
            # 遍历个股，计算个股价值因子载荷
            s = (calc_date - datetime.timedelta(days=90)).strftime('%Y%m%d')
            stock_basics = all_stock_basics[all_stock_basics.list_date < s]

            # 采用单进程进行计算价值因子
            # for _, stock_info in stock_basics.iterrows():
            #     logging.info("[%s] calc %s's value factor loading." % (calc_date.strftime('%Y-%m-%d'), stock_info.symbol))
            #     value_data = cls._calc_factor_loading(stock_info.symbol, calc_date)
            #     if value_data is not None:
            #         dict_value['id'].append(Utils.code_to_symbol(stock_info.symbol))
            #         dict_value['ep_ttm'].append(value_data['ep_ttm'])
            #         dict_value['bp_lr'].append(value_data['bp_lr'])
            #         dict_value['ocf_ttm'].append(value_data['ocf_ttm'])

            # 采用多进程并行计算价值因子
            q = Manager().Queue()   # 队列，用于进程间通信，存储每个进程计算的因子载荷
            p = Pool(4)             # 进程池，最多同时开启4个进程
            for _, stock_info in stock_basics.iterrows():
                p.apply_async(cls._calc_factor_loading_proc, args=(stock_info.symbol, calc_date, q,))
            p.close()
            p.join()
            while not q.empty():
                value_data = q.get(True)
                dict_value['id'].append(value_data['id'])
                dict_value['ep_ttm'].append(value_data['ep_ttm'])
                dict_value['bp_lr'].append(value_data['bp_lr'])
                dict_value['ocf_ttm'].append(value_data['ocf_ttm'])

            date_label = Utils.get_trading_days(start=calc_date, ndays=2)[1]
            dict_value['date'] = [date_label] * len(dict_value['id'])
            # 保存因子载荷至因子数据库
            if save:
                columns=['date', 'id', 'ep_ttm', 'bp_lr', 'ocf_ttm']
                Utils.factor_loading_persistent(cls._db_file, calc_date.strftime('%Y%m%d'), dict_value, columns)
            # 休息120秒
            logging.info('Suspending for 120s.')
            time.sleep(120)
        return dict_value



if __name__ == '__main__':
    # pass
    # Value._calc_factor_loading('000800', '2012-12-31')
    Value.calc_factor_loading(start_date='2017-01-01', end_date='2017-12-31', month_end=True, save=True)

