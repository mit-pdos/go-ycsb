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
