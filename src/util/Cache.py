#!/usr/bin/env/ python3
# -*- coding: utf-8 -*-
# @Abstract: 缓存类
# @Filename: Cache
# @Date:   : 2017-12-14 17:24
# @Author  : YuJun
# @Email   : yujun_mail@163.com


class Cache(object):

    def __init__(self, maxsize=100):
        self.cache = {}
        self.order = []     # least recently used first
        self.maxsize = maxsize

    def get(self, key):
        try:
            item = self.cache[key]  # KeyError if not present
            self.order.remove(key)
            self.order.append(key)
        except KeyError:
            item = None
        return item

    def set(self, key, value):
        if key in self.cache:
            self.order.remove(key)
        elif len(self.cache) >= self.maxsize:
            # discard least recently used item
            del self.cache[self.order.pop(0)]
        self.cache[key] = value
        self.order.append(key)

    def size(self):
        return len(self.cache)

    def set_maxsize(self, max_size):
        self.maxsize = max_size

if __name__ == '__main__':
    pass
