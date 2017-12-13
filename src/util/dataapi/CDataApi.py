#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 数据接口基类
# @Filename: CDataApi
# @Date:   : 2017-12-13 16:01
# @Author  : YuJun
# @Email   : yujun_mail@163.com


class CDataApi(object):
    """数据接口基类"""

    @classmethod
    def get_secu_basics(cls, inst_type=1):
        """
        取得证券基础信息表
        Parameters
        --------
        :param inst_type: int
            证券类别，1：股票，5：分级基金，101：股指期货，103：商品期货
        return: DataFrame
        --------
            0: symbol, string, 个股代码, 如600000.SH
            1: name, string, 证券名称
            2: status, string, 上市状态, 1:上市, 3:退市, 8:暂停上市
            3: market, string, 交易所代码
            4: list_date, string, 上市日期
        """
        pass

if __name__ == '__main__':
    pass
