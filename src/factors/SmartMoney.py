#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Filename: SmartMoney
# @Date:   : 2017-10-30 17:50
# @Author  : YuJun
# @Email   : yujun_mail@163.com


from src.factors.factor import Factor
import tushare as ts
import datetime
import numpy as np
import os
import pandas as pd
from pandas import DataFrame
from pandas import Series
import math
# import shelve
from src.util.utils import Utils
import src.factors.cons as factor_ct
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


def _get_year_tradingdays(year):
    """
    取得指定年份的交易日列表
    Parameters:
    ------
    :param year: int
        指定年份，如2017
    :return: list of datetime.datetime
        指定年份的交易日列表，降序排列，日期格式：YYYY-MM-DD
    """
    ts_conn = ts.get_apis()
    start_date = '%d-01-01' % year
    end_date = '%d-10-31' % year
    df = ts.bar(code='000001', conn=ts_conn, start_date=start_date, end_date=end_date, freq='D', asset='INDEX')
    pydate_array = df.index.to_pydatetime()
    # date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(pydate_array)
    ts.close_apis(ts_conn)
    return list(pydate_array)


def _get_trading_days(end_date, days_num):
    """
    取得交易日期列表
    :param end_date: 截止日期（含），类型=datetime.date
    :param days_num: 交易日数量
    :return: 交易日列表，降序排列，日期格式：YYYY-MM-DD
    """
    ts_conn = ts.get_apis()
    start_date = end_date + datetime.timedelta(days=-days_num)
    df = ts.bar(code='000001', conn=ts_conn, start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'), freq='D', asset='INDEX')
    pydate_array = df.index.to_pydatetime()
    date_only_array = np.vectorize(lambda s: s.strftime('%Y-%m-%d'))(pydate_array)
    ts.close_apis(ts_conn)
    return list(date_only_array)


def _get_min_mkts_fq(code, days, ret_num):
    """
    获取个股指定日期的复权分钟行情数据，日期范围由days列表指定，返回ret_num天的数据
    Parameters:
    ------
    :param code:string
        个股代码，如SH600000
    :param days:list of string, YYYY-MM-DD
        日期列表
    :param ret_num:int
        返回的交易日数量
    :return:
        例如：如果要取得浦发银行过去30个交易日中的10个交易日复权分钟行情数据，那么参数设置为：
             code=SH600000, days为过去30个交易日列表, ret_num=10
    ------
      DataFrame
        0: code，个股代码，如SH600000
        1: time，时间，格式YYYY-MM-DD hh:mm:ss
        2: open，开盘价
        3: high，最高价
        4: low，最低价
        5: close，收盘价
        6: volume，成交量(手)
        7: amount，成交金额(元)
        8: factor，复权系数
        如果给定的日期范围内读取分钟数据天数小于ret_num天，那么be_enough=False，否则be_enough=True
    """
    # cfg = ConfigParser()
    # cfg.read('config.ini')
    # db_path = cfg.get('factor_db', 'db_path')   # 读取因子数据库路径
    db_path = factor_ct.FACTOR_DB.db_path
    df_min_mkt = DataFrame()
    k = 0
    for str_date in days:
        mkt_file_path = os.path.join(db_path, 'ElementaryFactor/mkt_1min_FQ/%s/%s.csv' % (str_date, code))
        if os.path.isfile(mkt_file_path):
            # 读取个股每天的分钟行情数据
            df = pd.read_csv(mkt_file_path,
                             names=['code', 'time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'factor'],
                             skiprows=[0])
            # 计算每分钟的涨跌幅，每天第一分钟的涨跌幅=close/open-1
            df['ret'] = df['close'] / df['close'].shift(1) - 1.0
            df.ix[0, 'ret'] = df.ix[0, 'close'] / df.ix[0, 'open'] - 1.0
            # 拼接数据
            df_min_mkt = df_min_mkt.append(df, ignore_index=True)
            k += 1
            if k >= ret_num:
                break
    be_enough = True
    if k < ret_num:
        be_enough = False
    return be_enough, df_min_mkt


class SmartMoney(Factor):
    """聪明钱因子类"""
    __days = factor_ct.SMARTMONEY_CT.days_num   # 读取过去多少天的分钟行情进行因子载荷计算
    _db_file = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.SMARTMONEY_CT.db_file)   # 因子对应数据库文件名

    # def __init__(self):
    #     """
    #     因子初始化
    #     self.__days: 读取过去几天的分钟行情进行因子载荷计算
    #     self._db_file: 因子对应数据库文件名
    #     """
    #     super(SmartMoney, self).__init__()
    #     cfg = ConfigParser()
    #     cfg.read('config.ini')
    #     self.__days = cfg.getint('smartmoney', 'days_num')
    #     self._db_file = os.path.join(cfg.get('factor_db', 'db_path'), cfg.get('smartmoney', 'db_file'))

    @classmethod
    def _calc_factor_loading(cls, code, trading_days):
        """
        计算指定日期、指定个股的聪明钱因子载荷
        :param code: 个股代码，如SH600000
        :param trading_days: 用于读取分钟行情的交易日期列表
        :return:
        """
        #  取得过去30天的交易日期
        # trading_days = _get_trading_days(calc_date, 30)
        # 取得过去self.__days天交易日的分钟行情数据
        be_enough, df_min_mkt = _get_min_mkts_fq(code, trading_days, cls.__days)
        # 计算SmartMoney因子载荷值
        if be_enough:
            # 1.计算指标S_t = abs(R_t)/sqrt(V_t), R_t=第t分钟涨跌幅, V_t=第t分钟成交量
            df_min_mkt['ind_s'] = df_min_mkt.apply(lambda x: abs(x.ret)/math.sqrt(x.volume*100.0) if x.volume > 0 else 0, axis=1)
            # 2.降序排列指标S
            df_min_mkt = df_min_mkt.sort_values(by='ind_s', ascending=False)
            # 3.计算累积成交量、累积成交金额
            df_min_mkt['accum_volume'] = df_min_mkt['volume'].cumsum()
            df_min_mkt['accum_amount'] = df_min_mkt['amount'].cumsum()
            # 4.找到累积成交量占比前20%找到累积成交量占比前20%的交易，视为聪明钱（smart）交易, 那么聪明钱的情绪因子Q=VWAP_{smart}/VWAP_{all}
            total_volume = df_min_mkt.iloc[df_min_mkt.shape[0]-1]['accum_volume'] * 100
            total_amount = df_min_mkt.iloc[df_min_mkt.shape[0]-1]['accum_amount']
            smart_volume = int(df_min_mkt.iloc[df_min_mkt.shape[0]-1]['accum_volume'] * 0.2)
            vwap_all = total_amount / total_volume
            vwap_smart = 0.0
            for row in df_min_mkt.itertuples():
                if row.accum_volume > smart_volume:
                    vwap_smart = row.accum_amount / (row.accum_volume*100.0)
                    break
            smart_q = vwap_smart / vwap_all
        else:
            smart_q = -1
        # 返回个股的SmartMoney因子载荷值
        return smart_q

    @classmethod
    def calc_factor_loading(cls, start_date, end_date=None, month_end=True, save=False):
        """
        计算指定日期的样本个股的因子载荷，并保存至因子数据库
        Parameters
        --------
        :param start_date: datetime-like, str
            开始日期
        :param end_date: datetime-like, str，默认None
            结束日期，如果为None，则只计算start_date日期的因子载荷
        :param month_end: bool，默认True
            只计算月末时点的因子载荷
        :param save: 是否保存至因子数据库，默认为False
        :return: 因子载荷，DataFrame
        --------
            因子载荷,DataFrame
            0: ID, 证券ID，为索引
            1: factorvalue, 因子载荷
            如果end_date=None，返回start_date对应的因子载荷数据
            如果end_date!=None，返回最后一天的对应的因子载荷数据
            如果没有计算数据，返回None
        """
        # 0.取得交易日序列
        start_date = Utils.to_date(start_date)
        if end_date is not None:
            end_date = Utils.to_date(end_date)
            trading_days_series = Utils.get_trading_days(start=start_date, end=end_date)
        else:
            trading_days_series = Utils.get_trading_days(end=start_date, ndays=1)
        # 遍历交易日序列，计算SMartQ因子载荷
        dict_factor = None
        for calc_date in trading_days_series:
            dict_factor = {'ID': [], 'factorvalue': []}
            if month_end and (not Utils.is_month_end(calc_date)):
                continue
            # 1.获取用于读取分钟行情的交易日列表（过去30天的交易日列表）
            trading_days = _get_trading_days(calc_date, 30)
            # 2.取得样本个股信息
            stock_basics = ts.get_stock_basics()
            d = int((calc_date - datetime.timedelta(days=90)).strftime('%Y%m%d'))
            stock_basics = stock_basics[(stock_basics.timeToMarket < d) & (stock_basics.timeToMarket > 0)]
            # 3.遍历样本个股代码，计算Smart_Q因子载荷值
            dict_factor = {'ID': [], 'factorvalue': []}
            # df_factor.index.name = 'ID'
            for code, _ in stock_basics.iterrows():
                code = '%s%s' % ('SH' if code[:2] == '60' else 'SZ', code)
                factor_loading = cls._calc_factor_loading(code, trading_days)
                print("[%s]Calculating %s's SmartMoney factor loading = %.4f." % (calc_date.strftime('%Y-%m-%d'), code, factor_loading))
                if factor_loading != -1:
                    # df_factor.ix[code, 'factorvalue'] = factor_loading
                    dict_factor['ID'].append(code)
                    dict_factor['factorvalue'].append(factor_loading)
            # 4.保存因子载荷至因子数据库
            if save:
                # db = shelve.open(cls._db_file, flag='c', protocol=None, writeback=False)
                # try:
                #     db[calc_date.strftime('%Y%m%d')] = df_factor
                # finally:
                #     db.close()
                Utils.factor_loading_persistent(cls._db_file, calc_date.strftime('%Y%m%d'), dict_factor)
        return dict_factor


def SmartMoney_BackTest(start, end):
    """
    聪明钱因子的历史回测
    --------
    :param start: string
        测试开始日期，格式：YYYY-MM-DD，开始日期应该为月初的前一个交易日，即月末交易日
    :param end: string
        测试结束日期，格式：YYYY-MM-DD
    :return:
    """
    # 定义组合持仓变量、组合净值序列变量
    port_0_holding = DataFrame()    # 多头组合的最新持仓,columns:<date,code,hold_vol,buyprice>
    port_1_holding = DataFrame()    # 1至5组组合的最新持仓,columns:<date,code,hold_vol,buyprice>
    port_2_holding = DataFrame()
    port_3_holding = DataFrame()
    port_4_holding = DataFrame()
    port_5_holding = DataFrame()
    port_nav = DataFrame()          # 组合净值序列,columns:<date(index),port_0,port_1,port_2,port_3,port_4,port_5,port_ls>
    # 组合规模设置为1亿元
    port_init_scale = 100000000.0
    # 取得开始、结束日期区间内的交易日列表
    trading_days = Utils.get_trading_days(start=start, end=end)
    # 初始化组合净值序列
    port_nav_header = ['date', 'port_0', 'port_1', 'port_2', 'port_3', 'port_4', 'port_5', 'port_ls']
    port_nav = port_nav.append(Series([trading_days[0], port_init_scale, port_init_scale, port_init_scale, port_init_scale, port_init_scale, port_init_scale, 0.0], index=port_nav_header), ignore_index=True)
    port_nav.set_index('date', drop=True, inplace=True)
    # 遍历交易日，如果是月初，调仓；否则更新组合估值
    pre_trading_day = trading_days[0]
    for trading_day in trading_days[1:]:
        # 如果是第一个交易日或月初，那么进行构建组合或调仓
        if trading_day == trading_days[1] or trading_day.month != pre_trading_day.month:
            logging.debug('%s,月初进行调仓' % trading_day.strftime('%Y-%m-%d'))
            # 1.取得全体A股作为样本股
            stock_basics = ts.get_stock_basics()
            # 2.剔除样本股中的ST股和上市未满60天的新股
            d = int((pre_trading_day - datetime.timedelta(days=90)).strftime('%Y%m%d'))
            stock_basics = stock_basics[(stock_basics.timeToMarket < d) & (stock_basics.timeToMarket > 0)]
            # 3.在样本股中剔除最近20天涨幅最大的20%个股
            stock_basics['ret20'] = np.zeros(stock_basics.shape[0])
            for code, _ in stock_basics.iterrows():
                stock_basics.ix[code, 'ret20'] = Utils.calc_interval_ret(secu_code=code, end=pre_trading_day, ndays=20)
            stock_basics = stock_basics.sort_values(by='ret20', ascending=False, inplace=False).iloc[int(stock_basics.shape[0]*0.2):]
            # 4.在样本股中选取Q因子最小的10%，组成SMART组合。涨停、停牌不买入，跌停、停牌不卖出
            # 4.1.从因子数据库中读取处于pre_trading_day当天的SmartMoney因子载荷，如果数据库中不存在当天的因子载荷，则计算之
            #     取得（或计算的）因子载荷的格式为DataFrame，索引名称为'ID'，因子载荷项列名为'factorvalue'
            # db = shelve.open(SmartMoney.get_db_file(), flag='c', protocol=None, writeback=False)
            # if pre_trading_day.strftime('%Y%m%d') in db:
            #     try:
            #         df_smartmoney = db[pre_trading_day.strftime('%Y%m%d')]
            #     except KeyError:
            #         df_smartmoney = SmartMoney.calc_factor_loading(pre_trading_day.date())
            #     finally:
            #         db.close()
            # else:
            #     df_smartmoney = SmartMoney.calc_factor_loading(pre_trading_day.date())
            df_smartmoney = Utils.read_factor_loading(SmartMoney.get_db_file(), pre_trading_day.strftime('%Y%m%d'))
            if df_smartmoney.shape[0] == 0:
                df_smartmoney = SmartMoney.calc_factor_loading(pre_trading_day, month_end=False, save=False)
            # 4.2.遍历stock_basics，从因子载荷DataFrame中抽取对应的值
            stock_basics['smart_money'] = np.zeros(stock_basics.shape[0])
            for code, _ in stock_basics.iterrows():
                if Utils.code_to_symbol(code) in df_smartmoney.index:
                    stock_basics.ix[code, 'smart_money'] = df_smartmoney.ix[Utils.code_to_symbol(code), 'factorvalue']
                else:
                    stock_basics.ix[code, 'smart_money'] = -1.0
            # 4.3.剔除因子载荷小于0的数据，并按照因子载荷升序排列
            stock_basics = stock_basics[stock_basics.smart_money > 0]
            stock_basics.sort_values(by='smart_money', ascending=True, inplace=True)
            # 5.构建组合：等权重，规模=1亿元，格式DataFrame<date,code,hold_vol,close_FQ>，
            # 5.1.构建5个分组合，并计算5个分组和多空组合的净值
            num_in_group = int(stock_basics.shape[0]/5)
            sub_port_header = ['date', 'code', 'hold_vol', 'buyprice']
            # 5.1.1.构建分组合1的持仓，并计算分组合1的净值
            logging.debug('构建分组合1')
            port_1_holding = DataFrame()
            for code, _ in stock_basics.iloc[0:num_in_group].iterrows():    # 添加持仓（剔除停牌股）
                mkt_data = Utils.get_secu_daily_mkt(code, trading_day, fq=True)
                if len(mkt_data) > 0:
                    port_1_holding = port_1_holding.append(
                        Series([trading_day, code, 0, mkt_data.open], index=sub_port_header), ignore_index=True)
            hold_num = port_1_holding.shape[0]
            port_nav.ix[trading_day, 'port_1'] = 0.0
            for ind, holding in port_1_holding.iterrows():   # 遍历分组合1持仓，计算每个持仓的持仓量，以及分组合1净值
                port_1_holding.ix[ind, 'hold_vol'] = port_nav.iloc[-2].port_1 / hold_num / holding.buyprice
                mkt_data = Utils.get_secu_daily_mkt(holding.code, trading_day, fq=True, range_lookup=True)
                port_nav.ix[trading_day, 'port_1'] += port_1_holding.ix[ind, 'hold_vol'] * mkt_data.close
            port_1_holding.to_csv(os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.SMARTMONEY_CT.backtest_path,
                                               'port_1_%s.csv' % trading_day.strftime('%Y%m%d')), index=False, columns=sub_port_header)
            # 5.1.2.构建分组合2的持仓，并计算分组合2的净值
            logging.debug('构建分组合2')
            port_2_holding = DataFrame()
            for code, _ in stock_basics.iloc[num_in_group:num_in_group * 2].iterrows():     # 添加持仓（剔除停牌股）
                mkt_data = Utils.get_secu_daily_mkt(code, trading_day, fq=True)
                if len(mkt_data) > 0:
                    port_2_holding = port_2_holding.append(
                        Series([trading_day, code, 0, mkt_data.open], index=sub_port_header), ignore_index=True)
            hold_num = port_2_holding.shape[0]
            port_nav.ix[trading_day, 'port_2'] = 0.0
            for ind, holding in port_2_holding.iterrows():  # 遍历分组合2持仓，计算每个持仓的持仓量，以及分组合2净值
                port_2_holding.ix[ind, 'hold_vol'] = port_nav.iloc[-2].port_2 / hold_num / holding.buyprice
                mkt_data = Utils.get_secu_daily_mkt(holding.code, trading_day, fq=True, range_lookup=True)
                port_nav.ix[trading_day, 'port_2'] += port_2_holding.ix[ind, 'hold_vol'] * mkt_data.close
            port_2_holding.to_csv(os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.SMARTMONEY_CT.backtest_path,
                                               'port_2_%s.csv' % trading_day.strftime('%Y%m%d')), index=False, columns=sub_port_header)
            # 5.1.3.构建分组合3的持仓，并计算分组合3的净值
            logging.debug('构建分组合3')
            port_3_holding = DataFrame()
            for code, _ in stock_basics.iloc[num_in_group * 2:num_in_group * 3].iterrows():     # 添加持仓（剔除停牌股）
                mkt_data = Utils.get_secu_daily_mkt(code, trading_day, fq=True)
                if len(mkt_data) > 0:
                    port_3_holding = port_3_holding.append(
                        Series([trading_day, code, 0, mkt_data.open], index=sub_port_header), ignore_index=True)
            hold_num = port_3_holding.shape[0]
            port_nav.ix[trading_day, 'port_3'] = 0.0
            for ind, holding in port_3_holding.iterrows():  # 遍历分组合3持仓，计算每个持仓的持仓量，以及分组合3净值
                port_3_holding.ix[ind, 'hold_vol'] = port_nav.iloc[-2].port_3 / hold_num / holding.buyprice
                mkt_data = Utils.get_secu_daily_mkt(holding.code, trading_day, fq=True, range_lookup=True)
                port_nav.ix[trading_day, 'port_3'] += port_3_holding.ix[ind, 'hold_vol'] * mkt_data.close
            port_3_holding.to_csv(os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.SMARTMONEY_CT.backtest_path,
                                               'port_3_%s.csv' % trading_day.strftime('%Y%m%d')), index=False, columns=sub_port_header)
            # 5.1.4.构建分组合4的持仓，并计算分组合4的净值
            logging.debug('构建分组合4')
            port_4_holding = DataFrame()
            for code, _ in stock_basics.iloc[num_in_group * 3:num_in_group * 4].iterrows():     # 添加持仓（剔除停牌股）
                mkt_data = Utils.get_secu_daily_mkt(code, trading_day, fq=True)
                if len(mkt_data) > 0:
                    port_4_holding = port_4_holding.append(
                        Series([trading_day, code, 0, mkt_data.open], index=sub_port_header), ignore_index=True)
            hold_num = port_4_holding.shape[0]
            port_nav.ix[trading_day, 'port_4'] = 0.0
            for ind, holding in port_4_holding.iterrows():  # 遍历分组合4持仓，计算每个持仓的持仓量，以及分组合4净值
                port_4_holding.ix[ind, 'hold_vol'] = port_nav.iloc[-2].port_4 / hold_num / holding.buyprice
                mkt_data = Utils.get_secu_daily_mkt(holding.code, trading_day, fq=True, range_lookup=True)
                port_nav.ix[trading_day, 'port_4'] += port_4_holding.ix[ind, 'hold_vol'] * mkt_data.close
            port_4_holding.to_csv(os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.SMARTMONEY_CT.backtest_path,
                                               'port_4_%s.csv' % trading_day.strftime('%Y%m%d')), index=False, columns=sub_port_header)
            # 5.1.5.构建分组合5的持仓，并计算分组合5的净值
            logging.debug('构建分组合5')
            port_5_holding = DataFrame()
            for code, _ in stock_basics.iloc[num_in_group * 4:].iterrows():     # 添加持仓（剔除停牌股）
                mkt_data = Utils.get_secu_daily_mkt(code, trading_day, fq=True)
                if len(mkt_data) > 0:
                    port_5_holding = port_5_holding.append(
                        Series([trading_day, code, 0, mkt_data.open], index=sub_port_header), ignore_index=True)
            hold_num = port_5_holding.shape[0]
            port_nav.ix[trading_day, 'port_5'] = 0.0
            for ind, holding in port_5_holding.iterrows():  # 遍历分组合5持仓，计算每个持仓的持仓量，以及分组合净值
                port_5_holding.ix[ind, 'hold_vol'] = port_nav.iloc[-2].port_5 / hold_num / holding.buyprice
                mkt_data = Utils.get_secu_daily_mkt(holding.code, trading_day, fq=True, range_lookup=True)
                port_nav.ix[trading_day, 'port_5'] += port_5_holding.ix[ind, 'hold_vol'] * mkt_data.close
            port_5_holding.to_csv(os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.SMARTMONEY_CT.backtest_path,
                                               'port_5_%s.csv' % trading_day.strftime('%Y%m%d')), index=False, columns=sub_port_header)
            # 5.1.6.计算多空组合的净值
            port_nav.ix[trading_day, 'port_ls'] = port_nav.ix[trading_day, 'port_1'] - port_nav.ix[trading_day, 'port_5']
            # 5.2.构建多头组合的持仓，并计算多头组合的净值
            logging.debug('构建多头组合')
            port_0_holding = DataFrame()
            for code, _ in stock_basics.iloc[0:int(stock_basics.shape[0] * factor_ct.SMARTMONEY_CT.constituent_ratio)].iterrows():
                mkt_data = Utils.get_secu_daily_mkt(code, trading_day, fq=True)
                if len(mkt_data) > 0:
                    port_0_holding = port_0_holding.append(
                        Series([trading_day, code, 0, mkt_data.open], index=sub_port_header), ignore_index=True)
            hold_num = port_0_holding.shape[0]
            port_nav.ix[trading_day, 'port_0'] = 0.0
            for ind, holding in port_0_holding.iterrows():  # 遍历多头组合持仓，计算每个持仓的持仓量，以及多头组合净值
                port_0_holding.ix[ind, 'hold_vol'] = port_nav.iloc[-2].port_0 / hold_num / holding.buyprice
                mkt_data = Utils.get_secu_daily_mkt(holding.code, trading_day, fq=True, range_lookup=True)
                port_nav.ix[trading_day, 'port_0'] += port_0_holding.ix[ind, 'hold_vol'] * mkt_data.close
            port_0_holding.to_csv(os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.SMARTMONEY_CT.backtest_path,
                                               'port_0_%s.csv' % trading_day.strftime('%Y%m%d')), index=False, columns=sub_port_header)
        # 如果不是第一个交易日或月初，计算组合净值
        else:
            logging.debug('%s,组合估值' % trading_day.strftime('%Y-%m-%d'))
            # 遍历分组合1，计算分组合1净值
            port_nav.ix[trading_day, 'port_1'] = 0.0
            for _, holding in port_1_holding.iterrows():
                mkt_data = Utils.get_secu_daily_mkt(holding.code, trading_day, fq=True, range_lookup=True)
                # try:
                port_nav.ix[trading_day, 'port_1'] += holding.hold_vol * mkt_data.close
                # except AttributeError:
                #     print('Mkt data Error, code = %s, date = %s.' % (holding.code, trading_day.strftime('%Y-%m-%d')))
            # 遍历分组合2，计算分组合2净值
            port_nav.ix[trading_day, 'port_2'] = 0.0
            for _, holding in port_2_holding.iterrows():
                mkt_data = Utils.get_secu_daily_mkt(holding.code, trading_day, fq=True, range_lookup=True)
                port_nav.ix[trading_day, 'port_2'] += holding.hold_vol * mkt_data.close
            # 遍历分组合3，计算分组合3净值
            port_nav.ix[trading_day, 'port_3'] = 0.0
            for _, holding in port_3_holding.iterrows():
                mkt_data = Utils.get_secu_daily_mkt(holding.code, trading_day, fq=True, range_lookup=True)
                port_nav.ix[trading_day, 'port_3'] += holding.hold_vol * mkt_data.close
            # 遍历分组合4，计算分组合4净值
            port_nav.ix[trading_day, 'port_4'] = 0.0
            for _, holding in port_4_holding.iterrows():
                mkt_data = Utils.get_secu_daily_mkt(holding.code, trading_day, fq=True, range_lookup=True)
                port_nav.ix[trading_day, 'port_4'] += holding.hold_vol * mkt_data.close
            # 遍历分组合5，计算分组合5净值
            port_nav.ix[trading_day, 'port_5'] = 0.0
            for _, holding in port_5_holding.iterrows():
                mkt_data = Utils.get_secu_daily_mkt(holding.code, trading_day, fq=True, range_lookup=True)
                port_nav.ix[trading_day, 'port_5'] += holding.hold_vol * mkt_data.close
            # 多空组合的净值等于分组合1的净值减去分组合5的净值
            port_nav.ix[trading_day, 'port_ls'] = port_nav.ix[trading_day, 'port_1'] - port_nav.ix[trading_day, 'port_5']
            # 遍历多头组合，计算多头组合的净值
            port_nav.ix[trading_day, 'port_0'] = 0.0
            for _, holding in port_0_holding.iterrows():
                mkt_data = Utils.get_secu_daily_mkt(holding.code, trading_day, fq=True, range_lookup=True)
                port_nav.ix[trading_day, 'port_0'] += holding.hold_vol * mkt_data.close
        # 更改pre_trading_day
        pre_trading_day = trading_day
    # 保存组合净值序列数据
    port_nav.to_csv(os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.SMARTMONEY_CT.backtest_path, 'port_nav.csv'),
                    index=True, columns=port_nav_header)


if __name__ == '__main__':
    # 计算SmartQ因子载荷
    SmartMoney.calc_factor_loading('2012-12-31', save=True)
    # trading_days = _get_year_tradingdays(2016)
    # trading_days.sort()
    # pre_trading_day = trading_days[0]
    # for trading_day in trading_days:
    #     if trading_day.month != pre_trading_day.month:
    #         SmartMoney.calc_factor_loading(pre_trading_day.date(), True)
    #         time.sleep(300)
    #     pre_trading_day = trading_day
    # SmartMoney.calc_factor_loading(trading_days[-1], True)
    # print('The end.')

    # test_start = '2012-12-31'
    # test_end = '2013-01-31'
    # SmartMoney_BackTest(test_start, test_end)
