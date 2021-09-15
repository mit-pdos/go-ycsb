#!/usr/bin/env python

config = {
    'read': 0.95,
    'write': 0.05,
    'keys': 1e6,
    'hosts': {
        'memkv': '18.26.5.5:12200',
        'rediskv': '18.26.5.5:6379',
    },
    'benchcores': range(0,8)
}
