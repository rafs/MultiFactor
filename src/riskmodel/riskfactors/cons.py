#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 风险模型的cons
# @Filename: cons
# @Date:   : 2018-03-21 18:21
# @Author  : YuJun
# @Email   : yujun_mail@163.com

from src.util.dottabledict import DottableDict

# BETA风险因子的配置参数
BETA_CT = DottableDict({'db_file': 'RiskFactor/BETA/BETA',           # Beta因子在因子数据库的相对路径
                        'benchmark': 'SH000300',
                        'trailing': 252,
                        'half_life': 63})

# HSIGMA风险因子的配置参数
HSIGMA_CT = DottableDict({'db_file': 'RiskFactor/HSIGMA/HSIGMA',
                          'benchmark': 'SH000300',
                          'trailing': 252,
                          'half_life': 63})

# RSTR风险因子的配置参数
RSTR_CT = DottableDict({'db_file': 'RiskFactor/RSTR/RSTR',
                        'trailing_start': 504,
                        'trailing_end': 21,
                        'half_life': 126})

# DASTD风险因子的配置参数
DASTD_CT = DottableDict({'db_file': 'RiskFactor/DASTD/DASTD',
                         'trailing': 252,
                         'half_life': 42,
                         'listed_days': 180})

# CMRA风险因子的配置参数
CMRA_CT = DottableDict({'db_file': 'RiskFactor/CMRA/CMRA',
                        'trailing': 12,
                        'days_scale': 21,
                        'listed_days': 180})

if __name__ == '__main__':
    pass
