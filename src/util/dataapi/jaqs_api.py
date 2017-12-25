#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# abstract : jaqs数据接口的包装类
# @Filename: jaqs_api
# @Date:   : 2017-12-13 01:00
# @Author  : YuJun
# @Email   : yujun_mail@163.com

from jaqs.data.dataapi import DataApi
import src.util.dataapi.cons as ct
from src.util.dataapi.CDataApi import CDataApi

class JaqsApi(CDataApi):
    # 登录jaqs接口
    _api = DataApi(addr=ct.JAQS_DATA_SERVER)
    _api.login(ct.JAQS_LOGIN_USR, ct.JAQS_LONGIN_TOKEN)

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
            0: symbol, string, 个股代码, 如600000
            1: name, string, 证券名称
            2: status, string, 上市状态, 1:上市, 3:退市, 8:暂停上市
            3: market, string, 交易所代码
            4: list_date, string, 上市日期
            如果出错，返回None
        """
        df_basics, msg = cls._api.query(
                                        view='jz.instrumentInfo',
                                        fields='status,list_date,name,market',
                                        filter='inst_type=%d&status=1&market=SH,SZ&symbol=' % inst_type,
                                        data_format='pandas')
        if msg != '0,':
            return None
        df_basics.symbol = df_basics.symbol.map(lambda x: x.split('.')[0])
        return df_basics


if __name__ == '__main__':
    pass
