#!/usr/bin/env python3
# coding: utf-8

# In[1]:


from VMs import VM
from LOG_MSG import Log_msg
from collections import defaultdict
import pandas as pd
import time
from random import randint


# In[2]:


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
        
        self.free_vcpus = defaultdict(lambda: None)
        for _core in range(server_type.cores):
            self.free_vcpus[_core] = self.capacity_per_core - server_type.system_vcpus 

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
                'id':int(self.id),
                'server': self.name,
                'server_type': self.server_type.name,
                'vcpus': self.capacity_per_core,
                'vcpus_reserved': _vms_status_core.vcpus.sum(),
                'vcpus_free': self.free_vcpus[_core],
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
        return pd.Series(dict(self.free_vcpus))
    
    def deploy_vm_unit(self, vm: VM): 
                
        _s_free =  self.get_free_vcpus() - vm.flavor.vcpus
        
        if all(_s_free < 0):
            print("No hay recursos en Servidor: %s para VM: %s"%(self.name, vm.name))
            self.logger.log_msg('DEPLOY_VM','NOK','No existen suficientes recursos')
            return False
        
        _s_free = _s_free[_s_free >= 0]
        
        
        #DEV11
        #_core_selected = _s_free.idxmax()
        _core_selected = _s_free.idxmin()

        #vm.cluster_id = self.get_index()
        _vm_idx = self.get_index()
        
        #self.vms[vm.cluster_id] = vm
        self.vms[_vm_idx] = vm
        self.free_vcpus[_core_selected] -= vm.flavor.vcpus
               
        vm.status = 'deployed'
        vm.server_deployed = self
        vm.core_deployed = _core_selected
        vm.id = _vm_idx
        
        self.logger.log_msg('DEPLOY_VM','OK','VM: %s deployed '%vm.name)
        return True
    
    def get_vm_by_name(self, name: str):

        for k,v in self.vms.items():
            if v.name == name:
                return v
        
        return None
    
    
    def get_vms_status(self):
        _vm_status_col = ['server_deployed', 'vm', 'component', 'vnf', 'vcpus', 'memory','processor_deployed', 'avz', 'affinity', 'antiaffinity','status']
        _ret_df = pd.DataFrame([], columns=_vm_status_col)
        
        for _vm_id,_vm in self.vms.items():
            _ret_df = pd.concat([_ret_df,_vm.get_info()], ignore_index=True)
            
        return _ret_df
        
    
    def remove_vm(self, vm_name: str):
        _vm = self.get_vm_by_name(vm_name)
        
        if _vm == None:
            print("WARNING: Could not remove the VM:, not found %s"%vm_name)
            self.logger.log_msg('REMOVE_VM','INFO','VM: %s could not be removed,not found '%vm_name)
            return True
        
        _core_selected  = _vm.core_deployed
        self.vms.pop(_vm.id)
        
        self.free_vcpus[_core_selected] += _vm.flavor.vcpus

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
    
    def flush_server(self):   
        
        self.free_vcpus = defaultdict(lambda: None)
        for _core in range(self.server_type.cores):
            self.free_vcpus[_core] = self.capacity_per_core - self.server_type.system_vcpus 

        self.vms = defaultdict(lambda: None)
    
    def get_copy(self):    
        _server_bckp = Server(self.name, self.server_type, avz=self.avz, datacenter=self.datacenter,  virtual=self.virtual )
        _server_bckp.status = self.status
        
        for vm_id,_vm in self.vms.items():
            _server_bckp.deploy_vm(_vm.get_copy())
                    
        return _server_bckp

    def deploy_vm(self, vm: VM):
        
        if self.get_free_vcpus().sum() < vm.flavor.vcpus:
            print("no eonugh resources to deploy VM: %s, needed: %d  server has %d "%(vm.name, vm.flavor.vcpus, self.get_free_vcpus().sum()))
            return False
            

        if len(self.vms) == 0:
            return self.deploy_vm_unit(vm)
        else:
            _vms_copy_rollback = {k:_vm.get_copy() for k,_vm in self.vms.items()}
            _vms_copy = {k:_vm.get_copy() for k,_vm in self.vms.items()}
            _vms_copy[1000] = vm

            to_order = {k:vm.flavor.vcpus for k,_vm in _vms_copy.items()} 
            to_order = {k:v for k,v in sorted(to_order.items(), key=lambda x: x[1], reverse=True)}

            self.flush_server()

            for k,v in to_order.items():
                if not self.deploy_vm_unit(_vms_copy[k]):
                    
                    #### Roll Back #####
                    print("no eonugh resources to deploy VM: %s...... Rolling Back"%(vm.name))

                    self.flush_server()
                    for k1,v1 in _vms_copy_rollback.items():
                        self.deploy_vm_unit(_vms_copy_rollback[k1])
                        
                    return False
        
        return True

