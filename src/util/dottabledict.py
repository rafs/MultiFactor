#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Filename: dottabledict
# @Date:   : 2017-12-06 19:53
# @Author  : YuJun
# @Email   : yujun_mail@163.com


class DottableDict(dict):
    """支持点操作符的字典类"""
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self.__dict__ = self


if __name__ == '__main__':
    pass
