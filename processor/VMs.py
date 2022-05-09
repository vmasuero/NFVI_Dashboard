#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
import pandas as pd


# In[2]:


class Flavor_VM:
    def __init__(self, name:int, vcpus:int, memory:int, ssd=False, sriov=False):
        self.name = name
        self.vcpus = vcpus
        self.memory = memory
        self.ssd = ssd
        self.sriov = sriov
        
    def get_info(self):
        return pd.DataFrame([{
            'name': self.name,
            'vcpus': self.vcpus,
            'memory':self.memory,
            'ssd': self.ssd,
            'sriov': self.sriov
        }])
        
class VM:
    def __init__(self, name, component, vnf, flavor: Flavor_VM, avz='default', datacenter='default', node_select='', affinity=[], antiaffinity=[]):
        self.name = name
        self.component = component
        self.vnf = vnf
        self.avz = avz
        self.datacenter = datacenter
        self.node_select = node_select if re.match(r'(.*)[a-zA-Z]+',node_select) else 'NO_NODE_SELECT'
        self.flavor = flavor
        self.affinity = affinity
        self.antiaffinity = antiaffinity
        
        self.status = 'not_deployed'
        self.core_deployed = None
        self.server_deployed = None
        self.id = None

        
    def get_info(self):
        return pd.DataFrame([{
            'server_deployed': self.server_deployed.name if self.server_deployed else None ,
            'vm': self.name,
            'component': self.component,
            'vnf': self.vnf,
            'vcpus': self.flavor.vcpus,
            'memory': self.flavor.memory,
            'processor_deployed': self.core_deployed,
            'node_select': self.node_select,
            'avz': self.avz,
            'datacenter': self.datacenter,
            'status': self.status,
            'affinity': self.affinity,
            'antiaffinity': self.antiaffinity,
            'id': int(self.id),
            'server_cluster_id': int(self.server_deployed.id) if self.server_deployed else None
        }])
    
    def get_copy(self):
        return VM(self.name, self.component, self.vnf, self.flavor, self.avz, self.datacenter, self.node_select, self.affinity, self.antiaffinity)
    

