#!/usr/bin/env python

# Template: {name:'1s3c', clnts:[], srvs: []}

# ben;
configs = []
configs.append({'name':'1s1c', 'clnts':range(40,80), 'srvs': [[0]]})
configs.append({'name':'1s2c', 'clnts':range(40,80), 'srvs': [[0, 1]]})
configs.append({'name':'1s3c', 'clnts':range(40,80), 'srvs': [[0, 1, 2]]})
configs.append({'name':'1s4c', 'clnts':range(40,80), 'srvs': [[0, 1, 2, 3]]})
configs.append({'name':'1s5c', 'clnts':range(40,80), 'srvs': [[0, 1, 2, 3, 4]]})
configs.append({'name':'1s6c', 'clnts':range(40,80), 'srvs': [[0, 1, 2, 3, 4, 5]]})
configs.append({'name':'1s7c', 'clnts':range(40,80), 'srvs': [range(7)]})
configs.append({'name':'1s8c', 'clnts':range(40,80), 'srvs': [range(8)]})
configs.append({'name':'1s9c', 'clnts':range(40,80), 'srvs': [range(9)]})
configs.append({'name':'1s10c', 'clnts':range(40,80), 'srvs': [range(10)]})

# configs.append({'name':'1s2c', 'clnts':range(40,80), 'srvs': [[0, 1]]})
configs.append({'name':'2s2c', 'clnts':range(40,80), 'srvs': [[0, 1], [10, 11]]})
configs.append({'name':'3s2c', 'clnts':range(40,80), 'srvs': [[0, 1], [10, 11], [20, 21]]})
configs.append({'name':'4s2c', 'clnts':range(40,80), 'srvs': [[0, 1], [10, 11], [20, 21], [30, 31]]})
