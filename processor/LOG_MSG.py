#!/usr/bin/env python3
# coding: utf-8

# In[1]:


import time
import pandas as pd


# In[2]:


class Log_msg:
    LOG_ORDER = -1
    
    def __init__(self, name, type_obj):
        self.name = name
        self.type_obj = type_obj
        self.order = -1 
        self.logs = []

        
    def log_msg(self, object_name, instance_name, msg):
        self.logs.append({
            'type': self.type_obj,
            'owner': self.name,
            'object_name': object_name,
            'instance': instance_name,
            'msg': msg,
            'order': self.order + 1,
            'timestamp': time.time()
        })
        
        self.order += 1
        
        
        
    def get_info(self):
        _ret = pd.DataFrame([], columns=['type','owner','object_name','instance','msg','timestamp'])
        
        for _log in self.logs:
            _s = pd.Series({
                'type': _log['type'],
                'owner':_log['owner'],
                'object_name': _log['object_name'],
                'instance': _log['instance'],
                'msg': _log['msg'],
                'order': _log['order'],
                'timestamp': _log['timestamp']
            }, name= _log['order'])
            
            _ret = pd.concat([_ret, pd.DataFrame(_s).T])
        
        return pd.DataFrame(_ret)[['type','owner','object_name','instance','msg','timestamp']]
    
if False:  
    L = Log_msg('prueba','top1')
    L.log_msg('obj1','create','sos mismo')
    L.log_msg('obj1','create','sos mismo')
    L.log_msg('obj1','create','sos mismo')
    L.log_msg('obj1','delete','ttttt mismo')
    print(L.get_info())

