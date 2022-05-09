#!/usr/bin/env python
# coding: utf-8

# In[68]:


from VMs import VM
from LOG_MSG import Log_msg
from collections import defaultdict
import pandas as pd

import time
from random import randint


# In[69]:


class Type_Server:
    def __init__(self, name:int, cores:int, vcpus:int, system_vcpus:int, hyperthreading:bool, memory:int, processor='default', ssd=False, sriov=False):
        self.name = name
        self.cores = cores
        self.vcpus = vcpus
        self.system_vcpus = system_vcpus
        self.hyperthreading = hyperthreading
        self.processor = processor
        self.memory = memory
        self.ssd = ssd
        self.sriov = sriov
        
        
    def get_info(self):
        return pd.DataFrame([{
            'name' : self.name,
            'cores' : self.cores,
            'vcpus' : self.vcpus,
            'system_vcpus' : self.system_vcpus,
            'hyperthreading' : self.hyperthreading, 
            'processor' : self.processor
        }])
  
 
class Server:   
    def __init__(self, name, server_type:Type_Server, avz='default', datacenter='default', virtual=False):
        self.name = name
        self.id = None
        self.idxs = list(range(1000))[::-1]
        self.logger = Log_msg(self.name,'SERVER')
        
        self.server_type = server_type
        self.capacity_per_core = server_type.vcpus*2 if server_type.hyperthreading else server_type.vcpus
        self.reserved_system = server_type.system_vcpus
        self.max_cpu_per_core = self.capacity_per_core - server_type.system_vcpus
        self.num_cores = server_type.cores
        
        self.avz = avz
        self.datacenter = datacenter
        self.vms = defaultdict(lambda: None)

        self.virtual = virtual
        self.status = 'not_deployed'
        self.logs = []
        
        
    def get_info(self):
      
        _ret = []
        _vms_status = self.get_vms_status()
               
        
        for _core in range(self.server_type.cores):
            if _core  in _vms_status['processor_deployed']:
                _vms_status_core = _vms_status[_vms_status['processor_deployed'] == _core]
            else:
                _vms_status_core = pd.DataFrame([0], columns=['vcpus'])
            
            _ret.append({
                'id':int(self.id) if self.id != None else -1,
                'server': self.name,
                'server_type': self.server_type.name,
                'vcpus': self.capacity_per_core,
                'vcpus_reserved': _vms_status_core.vcpus.sum(),
                'vcpus_free': self.get_free_vcpus()[_core],
                'processor': _core,
                'vcpus_system': self.reserved_system,
                'memory': self.server_type.memory,
                'vms': sum([1 for k,v in self.vms.items() if v.core_deployed == _core]),
                'avz': self.avz,
                'datacenter': self.datacenter,
                'virtual': self.virtual,
                'status': self.status
            })
        
        ret_df = pd.DataFrame(_ret)
        return ret_df
    
    def get_index(self):
        return int(self.idxs.pop())
            
    def get_free_vcpus(self):
        _cores = pd.Series(self.num_cores*[self.max_cpu_per_core])
        
        for k,v in self.vms.items():
            _cores[v.core_deployed] -= v.flavor.vcpus
            
        return _cores
    
    def get_vms_loads(self):
        return [v.flavor.vcpus for k,v in self.vms.items()]
    
    @staticmethod
    def order_bins(loads, bins, capacity, max_policy=False):
        _bins = pd.Series(bins*[capacity])
        _loads = sorted(loads, reverse=True)
        _order = []
    
        if sum(_loads) > bins*capacity:
            print("Not Enough Capacity")
            return None
    
        for _load in _loads:
            _delta = (_bins - _load) 
            _delta = _delta[_delta >= 0]

            if _delta.empty:
                print("Cant arrange loads in capacity")
                return None
            
            if max_policy:
                _idx = _delta.idxmax()
            else:
                _idx = _delta.idxmin()
                
            _bins[_idx] -= _load
            _order.append((_idx,_load))

        return _order
     
    def deploy_vm(self, vm: VM):
  
        _order_loads = self.order_bins(self.get_vms_loads() + [vm.flavor.vcpus], self.num_cores, self.max_cpu_per_core)
    
        if _order_loads  == None:
            print("Policy of MIN_CORE_FREE_SPACE Core Free Fail, trying with MAX_CORE_FREE_SPACE")
            _order_loads = self.order_bins(self.get_vms_loads() + [vm.flavor.vcpus], self.num_cores, self.max_cpu_per_core, max_policy=True)
        
        if _order_loads:
            _vm_idx = self.get_index() 
            vm.id = _vm_idx
            self.vms[_vm_idx] = vm
            vm.status = 'deployed'
            vm.server_deployed = self
            
            _order_loads_df = pd.DataFrame(_order_loads, columns=['Core','Load'])
            
            for k,v in self.vms.items():
                _idx_max = _order_loads_df.loc[_order_loads_df['Load'] == v.flavor.vcpus,'Load'].idxmax()             
                self.vms[k].core_deployed = _order_loads_df.loc[_idx_max,'Core']
                _order_loads_df = _order_loads_df.drop(_idx_max)

            print('DEPLOY_VM','OK','VM: %s deployed '%vm.name)
            self.logger.log_msg('DEPLOY_VM','OK','VM: %s deployed '%vm.name)
            return True
        
        print("No hay recursos por Core en Servidor: %s para VM: %s"%(self.name, vm.name))
        self.logger.log_msg('DEPLOY_VM','NOK','No existen suficientes recursos')
        return False
    
    
    def get_vm_by_name(self, name: str):

        for k,v in self.vms.items():
            if v.name == name:
                return v
        
        return None
    
    
    def get_vms_status(self):
        _vm_status_col = ['id','server_deployed', 'vm', 'component', 'vnf', 'vcpus', 'memory','processor_deployed', 'avz', 'affinity', 'antiaffinity','status']
        _ret_df = pd.DataFrame([], columns=_vm_status_col)

   
        for _vm_id,_vm in self.vms.items():
            _ret_df = pd.concat([_ret_df,_vm.get_info()], ignore_index=True)
 
        _ret_df['vcpus'] = _ret_df['vcpus'].astype(int)
        _ret_df['id'] = _ret_df['id'].astype(int)
        return _ret_df
        
    
    def remove_vm(self, vm_name: str):
        _vm = self.get_vm_by_name(vm_name)
        
        if _vm == None:
            print("WARNING: Could not remove the VM:, not found %s"%vm_name)
            self.logger.log_msg('REMOVE_VM','INFO','VM: %s could not be removed,not found '%vm_name)
            return True
        
        _core_selected  = _vm.core_deployed
        self.vms.pop(_vm.id)
        
        _vm.deployed = False
        _vm.server_deployed = None
        _vm.core_deployed = None
        del _vm
        
        
        
        self.logger.log_msg('REMOVE_VM','OK','VM: %s  removed '%vm_name)
        return True
    
    def remove_all_vms(self):
        _vm_list = [vm for k,vm in self.vms.items()]
        
        for _vm in _vm_list:
            self.remove_vm(_vm.name)
            
        return True
    
    
    def get_copy(self):    
        _server_bckp = Server(self.name, self.server_type, avz=self.avz, datacenter=self.datacenter,  virtual=self.virtual )
        _server_bckp.status = self.status
        
        for vm_id,_vm in self.vms.items():
            _server_bckp.deploy_vm(_vm.get_copy())
                    
        return _server_bckp



# In[ ]:




