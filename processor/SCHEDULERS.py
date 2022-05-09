#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from SERVERS import Server
from VMs import VM
from collections import defaultdict
from random import randint
import queue
from LOG_MSG import Log_msg
import numpy as np
from copy import deepcopy
import time


# In[2]:


class Scheduler:
    
    def __init__(self, sch_name, default_server: Server):
        self.name = sch_name
        self.idxs = list(range(10000))[::-1]
        self.servers = defaultdict(lambda: None)
        self.default_server = default_server
        #self.vms = []
        self.servers_status = pd.DataFrame([], columns=['id', 'server', 'vcpus', 'vcpus_reserved', 'vcpus_free','processor', 'vcpus_system', 'memory', 'vms', 'avz', 'virtual','status'])
        self.servers_status_q = queue.Queue()
        self.vms_status = pd.DataFrame([], columns=['server_deployed', 'vm', 'component', 'vnf', 'vcpus', 'memory','processor_deployed', 'avz', 'affinity', 'antiaffinity', 'status','id', 'node_select'])
        self.vms_status_q = queue.Queue()
        self.logger = Log_msg(sch_name,"SCHEDULER")

    def is_empty(self):
        
        for k,v in self.servers.items():
            if v != None:
                return False
        
        return True
    
    def get_index(self):
        return self.idxs.pop()
        
    def deploy_server(self, server:Server): 
             
        if self.get_server_by_name(server.name):
            print("Server already exist: %s"%server.name)
            self.logger.log_msg('DEPLOY_SERVER','NOK',"Server already exist: %s"%server.name)
            return False
        
        server.id = self.get_index()        
        server.status = 'deployed'
        self.servers[server.id] = server
                
        self.servers_status_q.put(('CREATE',server.get_info()))
        self.logger.log_msg('DEPLOY_SERVER','OK',"Server deployed: %s"%server.name)
        return True
    
    def remove_server(self, server:str):
        _server = self.get_server_by_name(server)
       
        if _server == None:
            print("Server not found: %s"%server)
            self.logger.log_msg('REMOVE_SERVER','NOK','Server %s removed, not found'%server)
            return False
            
        _server.status = 'not_deployed'
        self.servers_status_q.put(('DELETE',_server.get_info()))
        

        for k,_vm in _server.vms.items():
            self.vms_status_q.put(('DELETE',_vm.get_info()))
            del _vm

        self.servers.pop(_server.id)
        self.logger.log_msg('REMOVE_SERVER','OK','Server %s removed'%server)
     
        return True
    
    def deploy_vm(self, server: Server, vm: VM):

        if server.get_vm_by_name(vm.name):
            print("ERROR: Deploying VM, VM already exsist %s"%vm.name)
            self.logger.log_msg('DEPLOY_VM','NOK',"Deploying VM: VM  not exist: %s"%vm.name)
            return False
              
        if server.deploy_vm(vm):
   
            self.servers_status_q.put(('UPDATE',server.get_info()))

            for k,_vm in server.vms.items():
                self.vms_status_q.put(('UPDATE',_vm.get_info()))

            self.logger.log_msg('DEPLOY_VM','OK',"Deploying VM %s deployed in Server %s"%(vm.name,server))
            return True
        
        self.logger.log_msg('DEPLOY_VM','NOK'," NO resources Deploying VM %s deployed in Server %s"%(vm.name,server))
        return False
    
    def remove_vm(self, vm: str):  
        _vm = self.get_vm_by_name(vm)
        
        if _vm:
            _server = _vm.server_deployed
            _server.remove_vm(vm)
        
            self.vms_status_q.put(('DELETE',_vm.get_info()))
            self.servers_status_q.put(('UPDATE',_server.get_info()))
            self.logger.log_msg('REMOVE_VM','OK',"VM removed: %s"%vm)
            print("VM removed: %s from Server %s"%(vm, _server.name))
            return True
        
        self.logger.log_msg('REMOVE_VM','NOK',"VM not found: %s"%vm)
        print("WARNING:  VM not found: %s"%vm)
        return False
    
   
    
    def get_vm_by_name(self, vm: str):
        
        for k,_server in self.servers.items():
            _vm_found = _server.get_vm_by_name(vm)
            if _vm_found:
                return _vm_found
            
        return None
            
       
    
    def get_server_by_name(self, name: str):

        for k,v in self.servers.items():
            if v.name == name:
                return v
            
        return None
    
    
    def get_server_by_id(self, server_id: int):
        
        if not self.servers[server_id]:
            print("WARNING: Server not found: server_id %s"%server_id)
            return None
            
        return self.servers[server_id]
    

    def status_reload(self):
        self.servers_status = pd.DataFrame([], columns=['id', 'server', 'vcpus', 'vcpus_reserved', 'vcpus_free','processor', 'vcpus_system', 'memory', 'vms', 'avz', 'virtual','status'])
        self.servers_status_q = queue.Queue()
        self.vms_status = pd.DataFrame([], columns=['server_deployed', 'vm', 'component', 'vnf', 'vcpus', 'memory','processor_deployed', 'avz', 'affinity', 'antiaffinity', 'status','id', 'node_select'])
        self.vms_status_q = queue.Queue()

        for _server_id, _server in self.servers.items():
            self.servers_status_q.put(('CREATE',_server.get_info()))
            for _cluster_vm_id, _vm in _server.vms.items():
                self.vms_status_q.put(('CREATE',_vm.get_info()))
                
        return True
    
    
    def get_servers_status(self):
      
        while not self.servers_status_q.empty():
            _cmd, _reg = self.servers_status_q.get()
                     
            if (_cmd == 'UPDATE') | (_cmd == 'DELETE'):
                #self.servers_status = self.servers_status[self.servers_status.id != _reg.id.max()]
                self.servers_status = self.servers_status[self.servers_status.server != _reg.server.max()]
                
                
            if (_cmd == 'UPDATE') | (_cmd == 'CREATE'):
                self.servers_status  = pd.concat([self.servers_status,_reg], ignore_index=True)

        
        return self.servers_status.reset_index(drop=True).astype({
            'vcpus': int, 
            'vcpus_reserved': int,
            'vcpus_system':int,
            'vcpus_free':int,
            'memory':int,
            'vms':int,
            'server_type':int,
            'id':int})


        
    def get_vms_status(self):
       
        while not self.vms_status_q.empty():
            _cmd, _reg = self.vms_status_q.get()
  
            
            if self.vms_status.empty == False:
                if (_cmd == 'UPDATE') | (_cmd == 'DELETE'):
                    self.vms_status = self.vms_status[self.vms_status.vm != _reg.vm.max()]

                
            if (_cmd == 'UPDATE') | (_cmd == 'CREATE'):
                self.vms_status  = pd.concat([self.vms_status,_reg], ignore_index=True)


            
        return self.vms_status.reset_index(drop=True).astype({
            'vcpus': int, 
            'memory':int
        })

    
    def rank_server_for_vm(self, vm: VM):
        
        if len(self.servers) == 0:
            self.logger.log_msg('RANK_SERVER','INFO',"No Servers to deploy VM: %s "%vm.name)
            return -1, pd.DataFrame([-1], columns=['rank'])

        _status_servers = self.get_servers_status()
        _status_servers['rank'] = 0
        _status_servers['selected'] = False
        
        ###### CHECK AVAILABILITY ZONE##################################

        _status_servers = _status_servers[ _status_servers.avz == vm.avz ] 

        if _status_servers.empty:
            print("AVZ not available:  %s"%vm.avz)
            self.logger.log_msg('RANK_SERVER','NOK',"No AVZ: %s to deploy VM:%s "%(vm.avz,vm.name))
            return -1, _status_servers
        
        
        
        
        _status_vms = self.get_vms_status()
        ###### CHECK ANTIAFFINITY y AFFINITY #########################
        if len(vm.antiaffinity) > 0:
            _aaf_servers = _status_vms.loc[_status_vms['component'].isin(vm.antiaffinity),'server_deployed']
            _status_servers.loc[_status_servers['server'].isin(_aaf_servers),'rank'] -= 100
            
        if len(vm.affinity) > 0:
            _af_servers = _status_vms.loc[_status_vms['component'].isin(vm.affinity),'server_deployed']
            _status_servers.loc[_status_servers['server'].isin(_af_servers),'rank'] += 10
        
        ###### LOAD BALANCER ########################################  
        _lb_servers = _status_vms.loc[_status_vms['component'] == vm.component,'server_deployed']
        _status_servers.loc[_status_servers['server'].isin(_lb_servers),'rank'] -= 5

        ###### NODE SELECTED ###############################
        if vm.node_select != 'NO_NODE_SELECT':
            _status_servers.loc[_status_servers['server'] == vm.node_select,'rank'] += 300
            
        ###### RESOURCES CPU ########################################
        #if vm.node_select == 'p-cllry01-csu-2038':
        #    display(vm.flavor.vcpus)
        #    display(_status_servers.loc[_status_servers['server'] == 'p-cllry01-csu-2038'])
        
        _status_servers.loc[ _status_servers['vcpus_free'].apply(lambda x: x < vm.flavor.vcpus) ,'rank'] -= 100
        _status_servers.loc[ _status_servers['vcpus_free'].apply(lambda x: x >= vm.flavor.vcpus) ,'rank'] += 30

        if _status_servers[_status_servers['rank'] >= -5].empty:
            print("No existen candidatos para desplegar VM")
            display(_status_servers)
            self.logger.log_msg('RANK_SERVER','NOK',"No existen candidatos para desplegar VM:%s "%vm.name)

            return -1, _status_servers
        

        _status_servers = _status_servers[_status_servers['rank'] == _status_servers['rank'].max()] 
        _status_servers = _status_servers[_status_servers['vcpus_free'] == _status_servers['vcpus_free'].max()] 

        _server_selected = _status_servers.loc[_status_servers['rank'].idxmax()] 
        
        _status_servers.loc[_status_servers['server'] == _server_selected['server'], 'selected'] = True
        
        _idx_selected = _server_selected['id']
        self.logger.log_msg('RANK_SERVER','OK',"Ranking server id %s  to deploy VM:%s"%(_idx_selected, vm.name))
        return int(_idx_selected), _status_servers
        
         
    def get_status_by_avz(self):
        _stat_servers = self.get_servers_status()
        _stat_vms = self.get_vms_status()
        
        _map_avz_dc = {v['avz']:v['datacenter'] for k,v in _stat_servers[['avz','datacenter']].drop_duplicates().iterrows()}
        
        
        _status_avz = _stat_servers.groupby(['avz','server','processor']).agg(
            vcpus=('vcpus','max'),
            vcpus_reserved = ('vcpus_reserved','sum'),
            vcpus_system=('vcpus_system','max'),
            vcpus_free=('vcpus_free','sum')
            ).groupby(level=0).sum()
        
        
        _status_avz['number_servers'] = _stat_servers.groupby(['avz','server']).max().groupby(level=0).agg(number_servers=('id','count') )
        _status_avz['number_vms'] = _stat_vms.groupby(['avz']).apply(lambda x: len(x['vm'].unique()))
        _status_avz['number_vms'] = _status_avz['number_vms'].fillna(0).astype(int)
        _status_avz['servers_not_available'] = _stat_servers.groupby(['avz','server']).agg({'virtual':np.all}).groupby(level=[0]).sum().astype(int)       
        _status_avz['utilization'] = _status_avz.apply(lambda x: (x['vcpus_reserved']/x['vcpus']), axis=1)
        _status_avz['datacenter'] = _status_avz.apply(lambda x: _map_avz_dc[x.name], axis=1)
  
        return _status_avz

    def set_default_server(self, server: Server):
        self.default_server = server.get_copy()
        
    def create_default_server(self, avz='default', datacenter='default'):        
        _default_server =  self.default_server.get_copy()
        _default_server.avz = avz
        _default_server.datacenter = datacenter
        _default_server.name = "vserver_auto_"+str(self.get_index())
        _default_server.virtual = True
       
        if not self.deploy_server(_default_server):
            self.logger.log_msg('CREATE_DEFAULT_SERVER','NOK'," Could not deploy default Server:%s"%_default_server.name)
        
        self.logger.log_msg('CREATE_DEFAULT_SERVER','OK',"Deployed default Server:%s"%_default_server.name)
        return _default_server

    
    def get_copy(self, name=''):
        _name = name if name != '' else self.name + "_copy"
        
        _new_sch = Scheduler(_name, self.default_server)
        
        _new_sch.idxs = deepcopy(self.idxs)
        
        for _id,_server in self.servers.items():
            _server_cp = _server.get_copy()
            _server_cp.id = _id
            
            _new_sch.servers[_id] = _server_cp
            
        _new_sch.status_reload()
            
        return _new_sch



# In[ ]:




