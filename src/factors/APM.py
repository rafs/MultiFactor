#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Filename: APM
# @Date:   : 2017-12-09 22:44
# @Author  : YuJun
# @Email   : yujun_mail@163.com


from src.factors.factor import Factor
import src.factors.cons as factor_ct
from src.util.utils import Utils, SecuTradingStatus
from src.util.dataapi.CDataHandler import CDataHandler
# from src.factors.Scale import Scale
# from src.factors.Value import Value
# from src.factors.Growth import Growth
# from src.factors.Momentum import Momentum
import os
import pandas as pd
import numpy as np
from pandas import DataFrame
from pandas import Series
import statsmodels.api as sm
import datetime
import logging
from multiprocessing import Pool, Manager
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


class APM(Factor):
    """APM因子类"""
    __days = factor_ct.APM_CT.days_num      # 读取过去多少天的分钟行情数据进行因子载荷计算
    _db_file = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.APM_CT.apm_db_file)      # 因子对应数据库文件名

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        """
        计算指定日期、指定个股APM因子的stat统计量
        --------
        :param code: string
            个股代码，如600000
        :param calc_date: datetime-like, str
            因子载荷计算日期，格式YYYY-MM-DD
        :return: float
        --------
            stat统计量，计算APM因子载荷的中间变量
        """
        # 1.取得过去40个交易日序列，交易日按降序排列
        calc_date = Utils.to_date(calc_date)
        trading_days = Utils.get_trading_days(end=calc_date, ndays=40, ascending=False)

        # 2.取得个股及指数过去__days+1个交易日每个交易日的开盘价、中午收盘价和当天收盘价
        #   开盘价为09:31分钟线的开盘价，中午收盘价为11:30分钟线的收盘价，当天收盘价为15:00分钟线的收盘价
        #   返回的数据格式为DataFrame，columns=['date','open','mid_close','close']，按日期升序排列
        # secu_mkt_data = DataFrame()
        # index_mkt_data = DataFrame()
        # mkt_data_header = ['date', 'open', 'mid_close', 'close']
        # k = 0
        # for trading_day in trading_days:
        #     df_1min_data = Utils.get_min_mkt(Utils.code_to_symbol(code), trading_day, fq=True)
        #     if df_1min_data is not None:
        #         str_date = Utils.datetimelike_to_str(trading_day)
        #         fopen = df_1min_data[df_1min_data.datetime == '%s 09:31:00' % str_date].iloc[0].open
        #         fmid_close = df_1min_data[df_1min_data.datetime == '%s 11:30:00' % str_date].iloc[0].close
        #         fclose = df_1min_data[df_1min_data.datetime == '%s 15:00:00' % str_date].iloc[0].close
        #         secu_mkt_data = secu_mkt_data.append(
        #             Series([str_date, fopen, fmid_close, fclose], index=mkt_data_header), ignore_index=True)
        #
        #         df_1min_data = Utils.get_min_mkt(factor_ct.APM_CT.index_code, trading_day, index=True, fq=True)
        #         fopen = df_1min_data[df_1min_data.datetime == '%s 09:31:00' % str_date].iloc[0].open
        #         fmid_close = df_1min_data[df_1min_data.datetime == '%s 11:30:00' % str_date].iloc[0].close
        #         fclose = df_1min_data[df_1min_data.datetime == '%s 15:00:00' % str_date].iloc[0].close
        #         index_mkt_data = index_mkt_data.append(
        #             Series([str_date, fopen, fmid_close, fclose], index=mkt_data_header), ignore_index=True)
        #         k += 1
        #         if k > cls.__days:
        #             break
        # if k <= cls.__days:
        #     return None
        # secu_mkt_data = secu_mkt_data.sort_values(by='date')
        # secu_mkt_data = secu_mkt_data.reset_index(drop=True)
        # index_mkt_data = index_mkt_data.sort_values(by='date')
        # index_mkt_data = index_mkt_data.reset_index(drop=True)
        # #  3.计算个股及指数的上午收益率数组r_t^{am},R_t^{am}和下午收益率数组r_t^{pm},R_t^{pm}，并拼接为一个数组
        # #    拼接后的收益率数组，上半部分为r_t^{am} or R_t^{am}，下半部分为r_t^{pm} or R_t^{pm}
        # r_am_array = np.zeros((cls.__days, 1))
        # r_pm_array = np.zeros((cls.__days, 1))
        # for ind in secu_mkt_data.index[1:]:
        #     r_am_array[ind-1, 0] = secu_mkt_data.loc[ind, 'mid_close'] / secu_mkt_data.loc[ind-1, 'close'] - 1.0
        #     r_pm_array[ind-1, 0] = secu_mkt_data.loc[ind, 'close'] / secu_mkt_data.loc[ind, 'mid_close'] - 1.0
        # r_apm_array = np.concatenate((r_am_array, r_pm_array), axis=0)
        #
        # R_am_array = np.zeros((cls.__days, 1))
        # R_pm_array = np.zeros((cls.__days, 1))
        # for ind in index_mkt_data.index[1:]:
        #     R_am_array[ind-1, 0] = index_mkt_data.loc[ind, 'mid_close'] / index_mkt_data.loc[ind-1, 'close'] - 1.0
        #     R_pm_array[ind-1, 0] = index_mkt_data.loc[ind, 'close'] / index_mkt_data.loc[ind, 'mid_close'] - 1.0
        # R_apm_array = np.concatenate((R_am_array, R_pm_array), axis=0)

        # 遍历交易日序列，计算个股及指数的上午收益率(r_am_array,R_am_array)和下午收益率序列(r_pm_array,R_pm_array)
        r_am_array = np.zeros((cls.__days, 1))
        r_pm_array = np.zeros((cls.__days, 1))
        R_am_array = np.zeros((cls.__days, 1))
        R_pm_array = np.zeros((cls.__days, 1))
        k = 0
        for trading_day in trading_days:
            df_1min_data = Utils.get_min_mkt(Utils.code_to_symbol(code), trading_day, fq=True)
            if df_1min_data is not None:
                str_date = Utils.datetimelike_to_str(trading_day)
                fopen = df_1min_data[df_1min_data.datetime == '%s 09:31:00' % str_date].iloc[0].open
                fmid_close = df_1min_data[df_1min_data.datetime == '%s 11:30:00' % str_date].iloc[0].close
                fclose = df_1min_data[df_1min_data.datetime == '%s 15:00:00' % str_date].iloc[0].close
                r_am_array[k, 0] = fmid_close / fopen - 1.0
                r_pm_array[k, 0] = fclose / fmid_close - 1.0

                df_1min_data = Utils.get_min_mkt(factor_ct.APM_CT.index_code, trading_day, index=True, fq=True)
                fopen = df_1min_data[df_1min_data.datetime == '%s 09:31:00' % str_date].iloc[0].open
                fmid_close = df_1min_data[df_1min_data.datetime == '%s 11:30:00' % str_date].iloc[0].close
                fclose = df_1min_data[df_1min_data.datetime == '%s 15:00:00' % str_date].iloc[0].close
                R_am_array[k, 0] = fmid_close / fopen - 1.0
                R_pm_array[k, 0] = fclose / fmid_close - 1.0

                k += 1
                if k == cls.__days:
                    break
        if k < cls.__days:
            return None
        r_apm_array = np.concatenate((r_am_array, r_pm_array), axis=0)
        R_apm_array = np.concatenate((R_am_array, R_pm_array), axis=0)

        # 4.个股收益率数组相对于指数收益率进行线性回归
        #   将指数收益率数组添加常数项
        R_apm_array = sm.add_constant(R_apm_array)
        #   线性回归：r_i = \alpha + \beta * R_i + \epsilon_i
        stat_model = sm.OLS(r_apm_array, R_apm_array)
        stat_result = stat_model.fit()
        resid_array = stat_result.resid.reshape((cls.__days*2, 1))   # 回归残差数组
        # 5.计算stat统计量
        #   以上得到的__days*2个残差\epsilon_i中，属于上午的记为\epsilon_i^{am},属于下午的记为\epsilong_i^{pm}，计算每日上午与
        #   下午残差的差值：$\sigma_t = \spsilon_i^{am} - \epsilon_i^{pm}$，为了衡量上午与下午残差的差异程度，设计统计量：
        #   $stat = \frac{\mu(\sigma_t)}{\delta(\sigma_t)\sqrt(N)}$，其中\mu为均值，\sigma为标准差,N=__days，总的来说
        #   统计量stat反映了剔除市场影响后股价行为上午与下午的差异程度。stat数值大（小）于0越多，则股票在上午的表现越好（差）于下午。
        delta_array = resid_array[:cls.__days] - resid_array[cls.__days:]   # 上午与 下午的残差差值
        delta_avg = np.mean(delta_array)    # 残差差值的均值
        delta_std = np.std(delta_array)     # 残差差值的标准差
        # 如果残差差值的标准差接近于0，返回None
        if np.fabs(delta_std) < 0.0001:
            return None
        stat = delta_avg / delta_std / np.sqrt(cls.__days)
        # logging.info('%s, stat = %.6f' % (code, stat))
        return stat

    @classmethod
    def _calc_factor_loading_proc(cls, code, calc_date, q):
        logging.info('[%s] Calc APM of %s.' % (calc_date.strftime('%Y-%m-%d'), code))
        stat = cls._calc_factor_loading(code, calc_date)
        ret20 = Utils.calc_interval_ret(code, end=calc_date, ndays=20)
        if stat is not None and ret20 is not None:
            q.put((Utils.code_to_symbol(code), stat, ret20))

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
            只计算月末时点的因子载荷，该参数只在end_date不为None时有效，并且不论end_date是否为None，都会计算第一天的因子载荷
        :param save: 是否保存至因子数据库，默认为False
        :return: 因子载荷，DataFrame
        --------
            因子载荷,DataFrame
            0: id, 证券ID
            1: factorvalue, 因子载荷
            如果end_date=None，返回start_date对应的因子载荷数据
            如果end_date!=None，返回最后一天的对应的因子载荷数据
            如果没有计算数据，返回None
        """
        # 1.取得交易日序列及股票基本信息表
        start_date = Utils.to_date(start_date)
        if end_date is not None:
            end_date = Utils.to_date(end_date)
            trading_days_series = Utils.get_trading_days(start=start_date, end=end_date)
        else:
            trading_days_series = Utils.get_trading_days(end=start_date, ndays=1)
        all_stock_basics = CDataHandler.DataApi.get_secu_basics()
        # 2.遍历交易日序列，计算APM因子载荷
        dict_apm = None
        for calc_date in trading_days_series:
            dict_apm = {'date': [], 'id': [], 'factorvalue': []}
            if month_end and (not Utils.is_month_end(calc_date)):
                continue
            # 2.1.遍历个股，计算个股APM.stat统计量，过去20日收益率，分别放进stat_lst,ret20_lst列表中
            s = (calc_date - datetime.timedelta(days=90)).strftime('%Y%m%d')
            stock_basics = all_stock_basics[all_stock_basics.list_date < s]
            stat_lst = []
            ret20_lst = []
            symbol_lst = []

            # 采用单进程计算
            # for _, stock_info in stock_basics.iterrows():
            #     stat_i = cls._calc_factor_loading(stock_info.symbol, calc_date)
            #     ret20_i = Utils.calc_interval_ret(stock_info.symbol, end=calc_date, ndays=20)
            #     if stat_i is not None and ret20_i is not None:
            #         stat_lst.append(stat_i)
            #         ret20_lst.append(ret20_i)
            #         symbol_lst.append(Utils.code_to_symbol(stock_info.symbol))
            #         logging.info('APM of %s = %f' % (stock_info.symbol, stat_i))

            # 采用多进程并行计算
            q = Manager().Queue()
            p = Pool(4)     # 最多同时开启4个进程
            for _, stock_info in stock_basics.iterrows():
                p.apply_async(cls._calc_factor_loading_proc, args=(stock_info.symbol, calc_date, q,))
            p.close()
            p.join()
            while not q.empty():
                apm_value = q.get(True)
                symbol_lst.append(apm_value[0])
                stat_lst.append(apm_value[1])
                ret20_lst.append(apm_value[2])

            assert len(stat_lst) == len(ret20_lst)
            assert len(stat_lst) == len(symbol_lst)

            # 2.2.构建APM因子
            # 2.2.1.将统计量stat对动量因子ret20j进行截面回归：stat_j = \beta * Ret20_j + \epsilon_j
            #     残差向量即为对应个股的APM因子
            # 截面回归之前，先对stat统计量和动量因子进行去极值和标准化处理
            stat_arr = np.array(stat_lst).reshape((len(stat_lst), 1))
            ret20_arr = np.array(ret20_lst).reshape((len(ret20_lst), 1))
            stat_arr = Utils.clean_extreme_value(stat_arr)
            stat_arr = Utils.normalize_data(stat_arr)
            ret20_arr = Utils.clean_extreme_value(ret20_arr)
            ret20_arr = Utils.normalize_data(ret20_arr)
            # 回归分析
            # ret20_arr = sm.add_constant(ret20_arr)
            apm_model = sm.OLS(stat_arr, ret20_arr)
            apm_result = apm_model.fit()
            apm_lst = list(np.around(apm_result.resid, 6))  # amp因子载荷精确到6位小数
            assert len(apm_lst) == len(symbol_lst)
            # 2.2.2.构造APM因子字典，并持久化
            date_label = Utils.get_trading_days(calc_date, ndays=2)[1]
            dict_apm = {'date': [date_label]*len(symbol_lst), 'id': symbol_lst, 'factorvalue': apm_lst}
            if save:
                Utils.factor_loading_persistent(cls._db_file, calc_date.strftime('%Y%m%d'), dict_apm)

            # 2.3.构建PureAPM因子
            # 将stat_arr转换为DataFrame, 此时的stat_arr已经经过了去极值和标准化处理
            df_stat = DataFrame(stat_arr, index=symbol_lst, columns=['stat'])
            # 取得提纯的因变量因子
            df_dependent_factor = cls.get_dependent_factors(calc_date)
            # 将df_stat和因变量因子拼接
            df_data = pd.concat([df_stat, df_dependent_factor], axis=1, join='inner')
            # OLS回归，提纯APM因子
            arr_data = np.array(df_data)
            pure_apm_model = sm.OLS(arr_data[:, 0], arr_data[:, 1:])
            pure_apm_result = pure_apm_model.fit()
            pure_apm_lst = list(np.around(pure_apm_result.resid, 6))
            pure_symbol_lst = list(df_data.index)
            assert len(pure_apm_lst) == len(pure_symbol_lst)
            # 构造pure_apm因子字典，并持久化
            dict_pure_apm = {'date': [date_label]*len(pure_symbol_lst), 'id': pure_symbol_lst, 'factorvalue': pure_apm_lst}
            pure_apm_db_file = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.APM_CT.pure_apm_db_file)
            if save:
                Utils.factor_loading_persistent(pure_apm_db_file, calc_date.strftime('%Y%m%d'), dict_pure_apm)
            # 休息360秒
            logging.info('Suspended for 360s.')
            time.sleep(360)
        return dict_apm

    # @classmethod
    # def calc_dependent_factors(cls, code, calc_date):
    #     """
    #     计算用于APM因子提纯的相关因子值，包括行业、规模、价值、成长、短期动量、长期动量
    #     Parameters:
    #     --------
    #     :param code: str
    #         个股代码，如600000或SH600000
    #     :param calc_date: datetime-like or str
    #         计算日期，格式：YYYY-MM-DD or YYYYMMDD
    #     :return: np.array
    #     --------
    #         用于APM因子提纯的相关因子向量:
    #         np.array([申万一级行业哑变量, 规模因子, 价值因子, 成长因子, 短期动量, 长期动量])
    #     """
    #     code = Utils.code_to_symbol(code)
    #     calc_date = Utils.to_date(calc_date)
    #     # 取得行业分布
    #     ind_dist = Utils.get_ind_dist(code)
    #     # 计算规模因子
    #     scale_factor = Scale._calc_factor_loading(code, calc_date)
    #     if scale_factor is None:
    #         return None
    #     # 计算价值因子
    #     value_factor = Value._calc_factor_loading(code, calc_date)
    #     if value_factor is None:
    #         return None
    #     # 计算成长因子
    #     growth_factor = Growth._calc_factor_loading(code, calc_date)
    #     if growth_factor is None:
    #         return None
    #     # 计算动量因子
    #     momentum_factor = Momentum._calc_factor_loading(code, calc_date)
    #     if momentum_factor is None:
    #         return None
    #     # stack the factor
    #     return np.array(list(ind_dist + [scale_factor, value_factor, growth_factor, momentum_factor]))


def apm_backtest(start, end, pure_factor=False):
    """
    APM因子的历史回测
    Parameters:
    --------
    :param start: datetime-like, str
        回测开始日期，格式：YYYY-MM-DD，开始日期应该为月初的前一个交易日，即月末交易日
    :param end: datetime-like, str
        回测结束日期，格式：YYYY-MM-DD
    :param pure_factor: bool, 默认False
        是否是对纯净因子做回测
    :return:
    """
    # 取得开始结束日期间的交易日数据
    trading_days = Utils.get_trading_days(start, end)
    # 读取截止开始日期前最新的组合回测数据
    prev_trading_day = Utils.get_prev_n_day(trading_days.iloc[0], 1)
    if pure_factor:
        backtest_path = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.APM_CT.pure_backtest_path)
    else:
        backtest_path = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.APM_CT.backtest_path)
    factor_data, port_nav = Utils.get_backtest_data(backtest_path, trading_days.iloc[0])
    # factor_data = None  # 记录每次调仓时最新入选个股的APM因子信息,pd.DataFrame<date,factorvalue,id,buyprice>
    if port_nav is None:
        port_nav = DataFrame({'date': [prev_trading_day.strftime('%Y-%m-%d')], 'nav': [1.0]})
    # 遍历交易日，如果是月初，则读取APM因子载荷值；如果不是月初，则进行组合估值
    for trading_day in trading_days:
        if factor_data is None:
            nav = port_nav[port_nav.date == prev_trading_day.strftime('%Y-%m-%d')].iloc[0].nav
        else:
            nav = port_nav[port_nav.date == factor_data.iloc[0].date].iloc[0].nav
        interval_ret = 0.0
        # 月初进行调仓
        if Utils.is_month_start(trading_day):
            logging.info('[%s] 月初调仓.' % Utils.datetimelike_to_str(trading_day, True))
            # 调仓前，先估值计算按均价卖出原先组合个股在当天的估值
            if factor_data is not None:
                for ind, factor_info in factor_data.iterrows():
                    daily_mkt = Utils.get_secu_daily_mkt(factor_info.id, trading_day, fq=True, range_lookup=True)
                    if daily_mkt.date == trading_day.strftime('%Y-%m-%d'):
                        vwap_price = daily_mkt.amount / daily_mkt.vol * daily_mkt.factor
                    else:
                        vwap_price = daily_mkt.close
                    interval_ret += vwap_price / factor_info.buyprice - 1.0
                interval_ret /= float(len(factor_data))
                nav *= (1.0 + interval_ret)
            # 读取factor_data
            if pure_factor:
                factor_data_path = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.APM_CT.pure_apm_db_file)
            else:
                factor_data_path = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.APM_CT.apm_db_file)
            factor_data = Utils.read_factor_loading(factor_data_path, Utils.datetimelike_to_str(prev_trading_day, False))
            # 遍历factor_data，剔除在调仓日没有正常交易（如停牌）、及涨停的个股
            ind_to_be_delted = []
            for ind, factor_info in factor_data.iterrows():
                trading_status = Utils.trading_status(factor_info.id, trading_day)
                if trading_status == SecuTradingStatus.Suspend or trading_status == SecuTradingStatus.LimitUp:
                    ind_to_be_delted.append(ind)
            factor_data = factor_data.drop(ind_to_be_delted, axis=0)
            # 对factor_data按因子值降序排列，取前10%个股
            factor_data = factor_data.sort_values(by='factorvalue', ascending=False)
            factor_data = factor_data.iloc[:int(len(factor_data)*0.1)]
            # 遍历factor_data，添加买入价格，并估值计算当天调仓后的组合收益
            factor_data['buyprice'] = 0.0
            interval_ret = 0.0
            for ind, factor_info in factor_data.iterrows():
                daily_mkt = Utils.get_secu_daily_mkt(factor_info.id, trading_day, fq=True, range_lookup=False)
                assert len(daily_mkt) > 0
                factor_data.loc[ind, 'buyprice'] = daily_mkt.amount / daily_mkt.vol * daily_mkt.factor
                interval_ret += daily_mkt.close / factor_data.loc[ind, 'buyprice'] - 1.0
            interval_ret /= float(len(factor_data))
            nav *= (1.0 + interval_ret)
            # 保存factor_data
            if pure_factor:
                port_data_path = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.APM_CT.pure_backtest_path,
                                              'port_data_%s.csv' % Utils.datetimelike_to_str(trading_day, False))
            else:
                port_data_path = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.APM_CT.backtest_path,
                                              'port_data_%s.csv' % Utils.datetimelike_to_str(trading_day, False))
            factor_data.to_csv(port_data_path, index=False)
        else:
            # 非调仓日，对组合进行估值
            logging.info('[%s] 月中估值.' % Utils.datetimelike_to_str(trading_day, True))
            if factor_data is not None:
                for ind, factor_info in factor_data.iterrows():
                    daily_mkt = Utils.get_secu_daily_mkt(factor_info.id, trading_day, fq=True, range_lookup=True)
                    interval_ret += daily_mkt.close / factor_info.buyprice - 1.0
                interval_ret /= float(len(factor_data))
                nav *= (1.0 + interval_ret)
        # 添加nav
        port_nav = port_nav.append(Series({'date': trading_day.strftime('%Y-%m-%d'), 'nav': nav}), ignore_index=True)
        # 设置prev_trading_day
        prev_trading_day = trading_day
    # 保存port_nav
    if pure_factor:
        port_nav_path = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.APM_CT.pure_backtest_path, 'port_nav.csv')
    else:
        port_nav_path = os.path.join(factor_ct.FACTOR_DB.db_path, factor_ct.APM_CT.backtest_path, 'port_nav.csv')
    port_nav.to_csv(port_nav_path, index=False)


if __name__ == '__main__':
    # pass
    # APM._calc_factor_loading('SZ002558','2015-12-31')
    # APM.calc_factor_loading('2012-12-31', save=True)
    # APM.calc_factor_loading(start_date='2017-11-01', end_date='2017-12-31', month_end=True, save=True)
    apm_backtest('2015-01-01', '2018-01-03', pure_factor=True)
