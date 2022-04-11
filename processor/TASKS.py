#!/usr/bin/env python3
# coding: utf-8

# In[1]:


from random import randint, choice
import pandas as pd
import numpy as np
import re
import networkx as nx

from VMs import VM, Flavor_VM
from LOG_MSG import Log_msg
from SERVERS import Server, Type_Server
from SCHEDULERS import Scheduler


# In[2]:


class G_Tasks(nx.DiGraph):
    
    def __init__(self, name, tasks_list, servers_tasks:pd.DataFrame, vms_tasks:pd.DataFrame, default_server:Server):
        super().__init__()
        self.logger = Log_msg(name,"TASK")
        self.default_server = default_server
        
        for _t in tasks_list:
            self.add_edge(_t[0] if pd.notnull(_t[0]) else 'ROOT',_t[1], label=_t[2])
            
        for _n in self.nodes():
            nx.set_node_attributes(self,{_n: { "scheduler": Scheduler(_n, self.default_server)}})
            
        self.servers_tasks = servers_tasks
        self.vms_tasks = vms_tasks
    
    
    def get_predecessor(self, node):
        _predecessor = list(self.predecessors(node))
        
        if len(_predecessor) > 0:
            return _predecessor[0]
        
        return None

    
    def get_tasks(self, task_id=None):
        _ret = nx.to_pandas_edgelist(self)
        
        if task_id != None:
            _ret = _ret[_ret['task_id'] == task_id]
            
        return _ret
    
    def get_scheduler(self, task_name):
        
        for _n in self.nodes(data=True):
            if _n[0] == task_name:
                return _n[1]['scheduler']
            
        return None
          

    def execute_servers_task(self, task_name):      
        _predecessor = self.get_predecessor(task_name)
        _sch = self.nodes[_predecessor]['scheduler'].get_copy(task_name) if _predecessor else Scheduler(task_name, DEFAULT_SERVER)

        nx.set_node_attributes(self,{task_name: { "scheduler": _sch}})
        
        _tasks_servers = self.servers_tasks[self.servers_tasks['Task_Name'] == task_name]

        num_servers = len(_tasks_servers)
        for i,(k,cmd_v) in enumerate(_tasks_servers.iterrows()):
            #clear_output(wait=True)
            print("SCHEDULER SERVER: %s SERVER: %s EXC: %s AVZ: %s ..........%d of %d"%(_sch.name, cmd_v.Hostname, cmd_v.Task_Action, cmd_v.Avz, i+1,num_servers))
            
            if cmd_v.Task_Action == 'DELETE_ALL':
           
                for k,v in _sch.get_servers_status().groupby('server').max().iterrows():

                    if _sch.remove_server(k):
                        print("Deleted ALL Servers: %s"%k)
                        self.logger.log_msg('EXEC_SERVER_DELETE_ALL','OK',"Server deleted: %s"%k)
                    else:
                        print("ERROR: Deleted ALL Servers: %s"%k)
                        self.logger.log_msg('EXEC_SERVER_DELETE_ALL','NOK',"Server NOT deleted: %s"%k)
            
            
            if cmd_v.Task_Action == 'SET_DEFAULT_SERVER':
                _server = Server(cmd_v.Hostname, SERVER_TYPES[cmd_v.Server_Type], avz=cmd_v['Avz'], datacenter=cmd_v['Datacenter'])
                _sch.set_default_server(_server)
                self.logger.log_msg('EXEC_SERVER_DEFAULT_SERVER','OK',"Set new Default Server: %s"%cmd_v.Hostname)
            
            
            if cmd_v.Task_Action == 'CREATE':
                _server = Server(cmd_v.Hostname, SERVER_TYPES[cmd_v.Server_Type], avz=cmd_v['Avz'], datacenter=cmd_v['Datacenter'])
                
                if _sch.deploy_server(_server):
                    self.logger.log_msg('EXEC_SERVER_CREATE','OK',"Server  created: %s"%cmd_v.Hostname)
                else:
                    self.logger.log_msg('EXEC_SERVER_CREATE','NOK',"Server not created: %s"%cmd_v.Hostname)
            
            
            if cmd_v.Task_Action == 'DELETE':
                if _sch.remove_server(cmd_v.Hostname):
                    self.logger.log_msg('EXEC_SERVER_DELETE','OK',"Server deleted: %s"%cmd_v.Hostname)
                else:
                    self.logger.log_msg('EXEC_SERVER_DELETE','NOK',"Server NOT deleted: %s"%cmd_v.Hostname)


    def execute_vms_task(self, task_name, tries=2):

        def deploy_vm_try(_scheduler, cmd_v):
            
            _vm = VM(
                cmd_v.Vm_Name,
                cmd_v.Component,
                cmd_v.VNF,
                FLAVORS_VM[cmd_v.Flavor],
                avz=cmd_v.Avz,
                datacenter=cmd_v.Datacenter,
                node_select=cmd_v.Hostname,
                affinity=cmd_v.Affinity, 
                antiaffinity=cmd_v.AntiAffinity
                )
            
            _cluster_id, _status = _scheduler.rank_server_for_vm(_vm)
            
            return _cluster_id, _status ,_vm

        _sch = self.get_scheduler(task_name)
        _tasks_vms = self.vms_tasks[self.vms_tasks['Task_Name'] == task_name]
        
        num_vms = len(_tasks_vms)
        for i,(k,cmd_v) in enumerate(_tasks_vms.iterrows()):
            #clear_output(wait=True)
            print("SCHEDULER VM: %s VM: %s EXC: %s AVZ: %s  NODE_SELECT: %s ..........%d of %d"%(_sch.name, cmd_v.Vm_Name, cmd_v.Task_Action, cmd_v.Avz,cmd_v.Hostname, i+1,num_vms))
            
            if cmd_v.Task_Action == 'DELETE_ALL':
                print("Deleting all VMS....")
                self.logger.log_msg('EXEC_VMS_TASKS_DELETE_ALL','OK',"VM deleted all")
                for k,v in _sch.get_vms_status().iterrows():
                    
                    if _sch.remove_vm(v.vm):
                        print("VM deleted: %s"%(v.vm))
                        self.logger.log_msg('EXEC_VMS_TASKS_DELETE_ALL','OK',"VM deleted %s"%(v.vm))
                    else:
                        print("VM NOT deleted: %s"%(v.vm))
                        self.logger.log_msg('EXEC_VMS_TASKS_DELETE_ALL','NOK',"VM NOT deleted %s"%(v.vm))
                        
                    
            if cmd_v.Task_Action == 'CREATE':
                if _sch.get_vm_by_name(cmd_v.Vm_Name) != None:
                    print("This VM Already exist: %s"%cmd_v.Vm_Name)
                    self.logger.log_msg('EXEC_VMS_CREATE','NOK',"This VM Already exist: %s"%cmd_v.Vm_Name)
                    continue
                    
                _cluster_id, _status, _vm = deploy_vm_try(_sch, cmd_v)
                
                try_deploy = 0
                while (_cluster_id < 0) & (try_deploy < tries):
                    df_server = _sch.create_default_server(avz=cmd_v.Avz, datacenter=cmd_v.Datacenter)
                    print("No enough resources, needed %d, creating Default Server: %s"%(_vm.flavor.vcpus,df_server.name))
                    print("Try %d"%try_deploy)
                    self.logger.log_msg('EXEC_VMS_CREATE','NOK',"No enough resources, needed %d, creating Default Server: %s"%(_vm.flavor.vcpus,df_server.name))
                    
                    _cluster_id, _status, _vm = deploy_vm_try(_sch, cmd_v)

                    try_deploy += 1

                if _cluster_id >= 0:
                    print("Deployed VM: %s in Server: %s \r"%(_vm.name, _sch.servers[_cluster_id].name), end='')
                    self.logger.log_msg('EXEC_VMS_CREATE','OK',"Resources  %d found, in server id: %s"%(_vm.flavor.vcpus,_cluster_id))
                    _log_result = _sch.deploy_vm(_sch.servers[_cluster_id].name,_vm)
                else:
                    print("Could not Deployed VM: %s"%_vm.name)
                    self.logger.log_msg('EXEC_VMS_CREATE','OK',"Resources  %d vcpus not found"%(_vm.flavor.vcpus))
                    
            
            if cmd_v.Task_Action == 'DELETE':   
                if _sch.remove_vm(cmd_v.Vm_Name):
                    print("VM deleted: %s"%(cmd_v.Vm_Name))
                    self.logger.log_msg('EXEC_VMS_DELETE','OK',"VM deleted %s"%(cmd_v.Vm_Name))
                else:
                    print("VM NOT deleted: %s"%(cmd_v.Vm_Name))
                    self.logger.log_msg('EXEC_VMS_DELETE','NOK',"VM NOT deleted %s"%(cmd_v.Vm_Name))
                        

