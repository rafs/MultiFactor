#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 数据处理接口，处理行情、基本面、因子载荷相关数据的读取和存储
# @Filename: CDataHandler
# @Date:   : 2017-12-13 16:40
# @Author  : YuJun
# @Email   : yujun_mail@163.com

from src.util.dataapi import cons as ct
from src.util.dataapi.jaqs_api import JaqsApi


class CDataHandler(object):
    """
    数据处理接口
    处理行情、基本面、因子相关数据的读取和存储
    """
    # 设置数据因子api
    DataApi = JaqsApi() if ct.DATA_API == 'jaqs' else None



if __name__ == '__main__':
    pass
