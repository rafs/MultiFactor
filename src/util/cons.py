#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Filename: conns
# @Date:   : 2017-11-29 18:42
# @Author  : YuJun
# @Email   : yujun_mail@163.com

# 因子数据库根目录
# DB_PATH = '/Users/davidyujun/Dropbox/FactorDB'
DB_PATH = '/Volumes/DB/FactorDB'
# 日行情复权数据相对目录
MKT_DAILY_FQ = 'ElementaryFactor/mkt_daily_FQ'
# 日行情非复权数据相对目录
MKT_DAILY_NOFQ = 'ElementaryFactor/mkt_daily_NoFQ'
# 分钟行情复权数据相对目录
MKT_MIN_FQ = 'ElementaryFactor/mkt_1min_FQ'
# 分钟行情非复权数据相对目录
MKT_MIN_NOFQ = 'ElementaryFactor/mkt_1min_NoFQ'
# 股本结构数据相对目录
CAP_STRUCT = 'ElementaryFactor/cap_struct'

# 日行情复权数据的表头
MKT_DAILY_FQ_HEADER = ['code', 'date', 'open', 'high', 'low', 'close', 'vol', 'amount', 'turnover1', 'turnover2', 'factor']
# 日行情非复权数据的表头
MKT_DAILY_NOFQ_HEADER = ['date', 'open', 'high', 'low', 'close', 'vol', 'amount', 'turnover1', 'turnover2']
# 分钟行情复权数据的表头
MKT_MIN_FQ_HEADER = ['code', 'datetime', 'open', 'high', 'low', 'close', 'vol', 'amount', 'factor']
# 股票股本结构数据的表头
CAP_STRUCT_HEADER = ['code', 'date', 'reason', 'total', 'liquid_a', 'liquid_b', 'liquid_h']

# 因子载荷文件持久化形式
FACTOR_LOADING_PERSISTENCE_TYPE='shelve,csv'
# 读取因子载荷采用的持久化形式，csv或shelve
USING_PERSISTENCE_TYPE='csv'

# 去极值方法中mad的乘数
CLEAN_EXTREME_VALUE_MULTI_CONST=5.2


if __name__ == '__main__':
    pass
