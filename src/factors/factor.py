#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Filename: factor
# @Date:   : 2017-11-01 21:16
# @Author  : YuJun
# @Email   : yujun_mail@163.com


class Factor(object):
    """因子基类"""
    _db_file = ''   # 因子对应的在因子数据库的数据文件名(包含数据)
    # def __init__(self):
    #     self._db_file = ''  # 因子对应的在因子数据库的数据文件名(包含数据)

    @classmethod
    def get_db_file(cls):
        return cls._db_file

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
            0: ID, 证券ID，为索引
            1: factorvalue, 因子载荷
        """
        pass

    @classmethod
    def calc_secu_factor_loading(cls, code, calc_date):
        """
        计算指定日期、指定个股的因子载荷
        :param code: 个股代码
        :param calc_date: 计算日期
        :return:
        """
        return cls._calc_factor_loading(code, calc_date)

    @classmethod
    def _calc_factor_loading(cls, code, calc_date):
        """
        计算指定日期、指定个股的因子载荷
        :param code: 个股代码
        :param calc_date: 计算日期
        :return:
        """
        pass

    @classmethod
    def _calc_factor_loading_proc(cls, code, calc_date, q):
        """
        用于并行计算因子载荷
        Parameters
        --------
        :param code: str
            个股代码，如果600000
        :param calc_date: datetime like or str
            计算日期，格式YYYY-MM-DD
        :param q: 队列，用于进程间通信
        :return: 添加因子载荷至队列q中
        """
        pass


if __name__ == '__main__':
    pass
