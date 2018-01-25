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
from src.util.utils import Utils

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

    @classmethod
    def download_index_cons(cls, idx_code, start_date, end_date):
        """
        下载指数成分股数据
        Parameters:
        --------
        :param idx_code: str
            指数代码，格式600000
        :param start_date: datetime-like, str
            开始日期
        :param end_date: datetime-like, str
            结束日期
        :return: bool
            下载成功返回True, 否则返回False
        """
        code = Utils.code_to_tssymbol(idx_code, True)
        start = Utils.datetimelike_to_str(start_date, False)
        end = Utils.datetimelike_to_str(end_date, False)
        df_idx_cons, msg = cls._api.query(view='lb.indexCons',
                                          fields='',
                                          filter='index_code=%s&start_date=%s&end_date=%s' % (code, start, end),
                                          data_format='pandas')
        if msg != '0,':
            return False
        else:

            file_path = '/Volumes/DB/FactorDB/ElementaryFactor/index_cons/%s.csv' % idx_code
            df_idx_cons.sort_values(by='in_date').to_csv(file_path, index=False)
            return True


if __name__ == '__main__':
    import datetime
    JaqsApi.download_index_cons('000985', '20050101', datetime.date.today().strftime('%Y%m%d'))
