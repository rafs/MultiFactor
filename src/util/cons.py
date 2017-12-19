#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Filename: conns
# @Date:   : 2017-11-29 18:42
# @Author  : YuJun
# @Email   : yujun_mail@163.com

# 因子数据库根目录
DB_PATH = '/Users/davidyujun/Dropbox/FactorDB'
# 日行情复权数据相对目录
MKT_DAILY_FQ = 'ElementaryFactor/mkt_daily_FQ'
# 日行情非复权数据相对目录
MKT_DAILY_NOFQ = 'ElementaryFactor/mkt_daily_NoFQ'
# 分钟行情复权数据相对目录
MKT_MIN_FQ = 'ElementaryFactor/mkt_1min_FQ'
# 分钟行情非复权数据相对目录
MKT_MIN_NOFQ = 'ElementaryFactor/mkt_1min_NoFQ'

# 日行情复权数据的表头
MKT_DAILY_FQ_HEADER = ['code', 'date', 'open', 'high', 'low', 'close', 'vol', 'amount', 'turnover1', 'turnover2', 'factor']
# 日行情非复权数据的表头
MKT_DAILY_NOFQ_HEADER = ['date', 'open', 'high', 'low', 'close', 'vol', 'amount', 'turnover1', 'turnover2']
# 分钟行情复权数据的表头
MKT_MIN_FQ_HEADER = ['code', 'datetime', 'open', 'high', 'low', 'close', 'vol', 'amount', 'factor']

# 因子载荷文件持久化形式
FACTOR_LOADING_PERSISTENCE_TYPE='shelve,csv'
# 读取因子载荷采用的持久化形式
USING_PERSISTENCE_TYPE='shelve'

# 去极值方法中mad的乘数
CLEAN_EXTREME_VALUE_MULTI_CONST=5.2


if __name__ == '__main__':
    pass
