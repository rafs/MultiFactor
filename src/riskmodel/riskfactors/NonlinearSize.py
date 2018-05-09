#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 风险模型中的非线性市值因子
# @Filename: NonlinearSize
# @Date:   : 2018-05-08 01:00
# @Author  : YuJun
# @Email   : yujun_mail@163.com


from src.factors.factor import Factor
import src.riskmodel.riskfactors.cons as risk_ct
import src.factors.cons as factor_ct
from src.util.utils import Utils
import pandas as pd
import numpy as np
import logging
import os
import statsmodels.api as sm

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


class NLSIZE(Factor):
    """非线性市值因子类(Non-linear Size)"""
    _db_file = os.path.join(factor_ct.FACTOR_DB.db_path, risk_ct.NLSIZE_CT.db_file)

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        pass

    @classmethod
    def _calc_factor_loading_proc(cls, code, calc_date, q):
        pass

    @classmethod
    def calc_factor_loading(cls, start_date, end_date=None, month_end=True, save=False, **kwargs):
        """
        计算指定日期的样本个股的因子载荷, 并保存至因子数据库
        Parameters:
        --------
        :param start_date: datetime-like, str
            开始日期, 格式：YYYY-MM-DD or YYYYMMDD
        :param end_date: datetime-like, str
            结束日期, 如果为None, 则只计算start_date日期的因子载荷, 格式: YYYY-MM-DD or YYYYMMDD
        :param month_end: bool, 默认为True
            如果为True, 则只计算月末时点的因子载荷
        :param save: bool, 默认为True
            是否保存至因子数据库
        :param kwargs:
        :return: dict
            因子载荷数据
        """
        # 取得交易日序列
        start_date = Utils.to_date(start_date)
        if end_date is not None:
            end_date = Utils.to_date(end_date)
            trading_days_series = Utils.get_trading_days(start=start_date, end=end_date)
        else:
            trading_days_series = Utils.get_trading_days(end=start_date, ndays=1)
        # 遍历交易日序列, 计算NLSIZE因子载荷
        dict_nlsize = None
        for calc_date in trading_days_series:
            if month_end and (not Utils.is_month_end(calc_date)):
                continue
            logging.info('[%s] Calc NLSIZE factor loading.' % Utils.datetimelike_to_str(calc_date))
            # 读取Size因子载荷数据
            lncap_data_path = os.path.join(factor_ct.FACTOR_DB.db_path, '{}_{}.csv'.format(risk_ct.LNCAP_CT.db_file, Utils.datetimelike_to_str(calc_date, dash=False)))
            if not os.path.exists(lncap_data_path):
                logging.info('[%s] 的Size因子载荷数据不存在.' % Utils.datetimelike_to_str(calc_date))
                continue
            df_lncap = pd.read_csv(lncap_data_path, header=0)
            # Size因子数组
            arr_size = np.array(df_lncap['factorvalue'])
            # Size因子三次方数组
            arr_size_cube = arr_size ** 3
            # 相对Size因子正交化
            model = sm.OLS(arr_size_cube, arr_size)
            result = model.fit()
            # 对残差值进行缩尾处理和标准化
            n = len(result.resid)
            arr_resid = result.resid.reshape(n, 1)
            arr_resid_winsorized = Utils.clean_extreme_value(arr_resid)
            arr_resid_standardized = Utils.normalize_data(arr_resid_winsorized)
            # 保存NLSIZE因子载荷数据
            dict_nlsize = dict({'date': df_lncap['date'].values, 'id': df_lncap['id'].values, 'factorvalue': arr_resid_standardized.reshape(n,)})
            if save:
                Utils.factor_loading_persistent(cls._db_file, Utils.datetimelike_to_str(calc_date, dash=False), dict_nlsize, ['date', 'id', 'factorvalue'])


if __name__ == '__main__':
    # pass
    NLSIZE.calc_factor_loading(start_date='2017-12-28', end_date=None, month_end=False, save=True)
