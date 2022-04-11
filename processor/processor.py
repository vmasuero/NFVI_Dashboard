#!/usr/bin/env python3
# coding: utf-8

# In[1]:


import argparse
from random import randint, choice
import pandas as pd
import numpy as np
import re
import networkx as nx
import os 

import sys


from VMs import VM, Flavor_VM
from LOG_MSG import Log_msg
from SERVERS import Server, Type_Server
from SCHEDULERS import Scheduler


from collections import defaultdict
import json


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
                        
              
            if cmd_v.Task_Action == 'C1REATE':
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
                    if not _sch.deploy_vm(_sch.servers[_cluster_id].name,_vm):
                        print("ERROR:   en %s"%_sch.servers[_cluster_id].name)
                else:
                    print("Could not Deployed VM: %s"%_vm.name)
                    self.logger.log_msg('EXEC_VMS_CREATE','OK',"Resources  %d vcpus not found"%(_vm.flavor.vcpus))
                    
            #### DEV100   
            if cmd_v.Task_Action == 'CREATE':
                try_deploy = 0
                _vm = VM(
                    cmd_v.Vm_Name,
                    cmd_v.Component,
                    cmd_v.VNF,
                    FLAVORS_VM[cmd_v.Flavor],
                    avz=cmd_v.Avz,
                    datacenter=cmd_v.Datacenter,
                    node_select=cmd_v.Hostname,
                    affinity=cmd_v.Affinity, 
                    antiaffinity=cmd_v.AntiAffinity)
                
                
                _id, _status = _sch.rank_server_for_vm(_vm)
                
                while (_id < 0) & (try_deploy < tries):
                    df_server = _sch.create_default_server(avz=cmd_v.Avz, datacenter=cmd_v.Datacenter)
                    print("No enough resources, needed %d, creating Default Server: %s"%(_vm.flavor.vcpus,df_server.name))
                    print("Try %d"%try_deploy)
                    self.logger.log_msg('EXEC_VMS_CREATE','NOK',"No enough resources, needed %d, creating Default Server: %s"%(_vm.flavor.vcpus,df_server.name))
                    
                    _id, _status = _sch.rank_server_for_vm(_vm)
                    try_deploy += 1
                
                if _id >= 0:
                    print("Deployed VM: %s in Server: %s \r"%(_vm.name, _sch.servers[_id].name), end='')
                    self.logger.log_msg('EXEC_VMS_CREATE','OK',"Resources  %d found, in server id: %s"%(_vm.flavor.vcpus,_id))
                    
                    if not _sch.deploy_vm(_sch.servers[_id],_vm):
                        print("ERROR: Incongruencia con server  %s y VM %s"%(_sch.servers[_id].name,_vm.name))
                        print(_status)
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


# In[3]:


def format_server_df(file):
    _sheet_name = 'Servers'
    _columns_strip = ['Task_Name','Task_Action','Hostname','Avz','Datacenter']
    _columns_int = ['Server_Type']
    
    _servers_df = pd.read_excel(file, sheet_name=_sheet_name)
    _servers_df.loc[:,['Avz','Datacenter']] = _servers_df.loc[:,['Avz','Datacenter']].fillna('default')
    _servers_df[_columns_strip] = _servers_df[_columns_strip].fillna('').applymap(lambda x: x.strip())
    _servers_df[_columns_int] = _servers_df[_columns_int].fillna(-1).astype(int)
    
    return _servers_df
  
    
def format_vms_df(file):
    _sheet_name = 'VMs'
    _columns_strip = ['Task_Name','Task_Action','Vm_Name','Hostname','Avz','Component','VNF', 'Datacenter']
    _columns_int = ['Flavor']
    
    _vms_df = pd.read_excel(file, sheet_name='VMs')
    _vms_df.loc[:,['Avz','Datacenter']] = _vms_df.loc[:,['Avz','Datacenter']].fillna('default')
    _vms_df.loc[:,['Vm_Name','Hostname']] = _vms_df.loc[:,['Vm_Name','Hostname']].fillna('')
    _vms_df[_columns_strip] = _vms_df[_columns_strip].fillna('').applymap(lambda x: x.strip())
    _vms_df['Affinity'] = _vms_df['Affinity'].fillna('').apply(lambda x: [y for y in x.split(',') if y != ''] if re.match(r'([a-zA-Z0-9_-]+,)+',x) else [])
    _vms_df['AntiAffinity'] = _vms_df['AntiAffinity'].fillna('').apply(lambda x: [y for y in x.split(',') if y != ''] if re.match(r'([a-zA-Z0-9_-]+,)+',x) else [])
    _vms_df[_columns_int] = _vms_df[_columns_int].fillna(-1).astype(int)
    
    return _vms_df[_columns_strip + _columns_int +['Affinity','AntiAffinity']]
    

def format_flavors_df(file):
    _columns_strip = ['Compute_Type']
    _columns_int = ['Id','Vcpus','RAM']
    
    _flavors_df = pd.read_excel(file, sheet_name='Flavor_Types')
    _flavors_df[_columns_strip] = _flavors_df[_columns_strip].applymap(lambda x: x.strip())
    _flavors_df[_columns_int] = _flavors_df[_columns_int].astype(int)
    _flavors_df = _flavors_df.set_index('Id')
    
    return [Flavor_VM(k, v.Vcpus, v.RAM) for k,v in _flavors_df.iterrows()]


def format_server_types(file):
    _columns_strip = ['Processor_Model','HyperThreading']
    _columns_int = ['Id','Num_Cores','Vcpus_for_System','Ram_memory_GB']
    _server_type_df = pd.read_excel(file, sheet_name='Server_Types')
    _server_type_df[_columns_strip] = _server_type_df[_columns_strip].applymap(lambda x: x.strip())
    _server_type_df[_columns_int] = _server_type_df[_columns_int].astype(int)
    _server_type_df['HyperThreading'] = _server_type_df['HyperThreading'].apply(lambda x: True if x == 'yes' else False)
    _server_type_df = _server_type_df.set_index('Id')
        
    return  [Type_Server(k, 
                         v.Num_Cores, 
                         v.Vcpus_per_Core, 
                         v.Vcpus_for_System, 
                         v.HyperThreading, 
                         v.Ram_memory_GB, 
                         v.Processor_Model) for k,v in _server_type_df.iterrows()]


def format_task_df(file):
    _task_df = pd.read_excel(file, sheet_name='Tasks')
    _task_df[['Previous_Task','Fordward_Task']] = _task_df[['Previous_Task','Fordward_Task']].applymap(lambda x: x.strip() if type(x) == str else x)
    
    return _task_df


def exec_tasks(tasks, task_name):
    print("\n\n############################################################################")
    print("Exceuting TASK:          %s"%task_name)
    tasks.execute_servers_task(task_name)
    tasks.execute_vms_task(task_name)

    for _sucessor in tasks.successors(task_name):
        exec_tasks(tasks, _sucessor) 


# In[4]:


parser = argparse.ArgumentParser(description='Processor of Template - NFVI Report')
parser.add_argument('-f', '--file', type=str, required=True, help='XLS file of the NFVi Template')


DIR_TEMPLATE = '/usr/app/template/'

if __name__ == '__main__':
    
    print(sys.argv)
    args = parser.parse_args()
    
    file_path = DIR_TEMPLATE + args.file
    
    if os.path.isfile(file_path):
        print ("File found .......... OK: %s"%file_path)
    else:
        raise Exception("File not exist: %s"%file_path)
    
    
    print("reading FILE: Servers Types")
    SERVER_TYPES = format_server_types(file_path)

    print("reading FILE: Flavors VMs")
    FLAVORS_VM = format_flavors_df(file_path)

    print("reading FILE: Servers")
    SERVERS_DF = format_server_df(file_path)

    print("reading FILE: VMs")
    VMS_DF = format_vms_df(file_path)


    print("reading FILE: Tasks")
    TASKS_DF = format_task_df(file_path)

    print("Testing files...")
    CHECK_OK = True

    print("Chequeando correspondencia en Servers DB.........", end='')
    _test1 = ~SERVERS_DF.loc[SERVERS_DF['Task_Action'] == 'CREATE','Server_Type'].isin([x.name for x in SERVER_TYPES])

    if np.any(_test1):
        print('NOK')
        print("\t\tERROR SERVERS DB:  No esta definido el Type Server en index: %d "%_test1.idxmax())
        CHECK_OK = False
    else:
        print("OK")


    print("Chequeando correspondencia en VM DB.........", end='')
    _test2 = ~VMS_DF.loc[VMS_DF['Task_Action'] == 'CREATE','Flavor'].isin([x.name for x in FLAVORS_VM])

    if np.any(_test2):
        print('NOK')
        display(VMS_DF[_test2])
        print("\t\tERROR VM DB:  No esta definido el Flavor en index: %d "%_test2.idxmax())
        CHECK_OK = False
    else:
        print("OK")

    DEFAULT_SERVER = Server('default_server',SERVER_TYPES[0],'default', 'default', virtual=True)


    if not CHECK_OK:
        raise Exception('Archivo de INPUT contiene fallas')

    
    TASKS  = G_Tasks('PRI_TASK',TASKS_DF.values, SERVERS_DF, VMS_DF, DEFAULT_SERVER)
    exec_tasks(TASKS, 'INITIALIZATION')
    
    
    
    SAVE_DATA = defaultdict(lambda x: None)

    print("AVZ tables...")
    AVZ_TABLES = {_node:TASKS.get_scheduler(_node).get_status_by_avz()  for _node in TASKS.nodes() if _node != 'ROOT'}
    for k,v in AVZ_TABLES.items():
        v['utilization'] = v['utilization'].apply(lambda x: "%0.0f%%"%(100*x)) 

    SAVE_DATA['AVZ_TABLES'] = {k:v.to_dict() for k,v in AVZ_TABLES.items()}


    print("Creating Server tables...")
    SERVERS_TABLES = {_node:TASKS.get_scheduler(_node).get_servers_status()  for _node in TASKS.nodes() if _node != 'ROOT'}
    for k,v in SERVERS_TABLES.items():
        v.loc[:,['id','memory','server_type','vcpus','vcpus_free','vcpus_reserved','vcpus_system','vms']] = v.loc[:,['id','memory','server_type','vcpus','vcpus_free','vcpus_reserved','vcpus_system','vms']].astype(int)

    SAVE_DATA['SERVERS_TABLES'] = {k:v.to_dict() for k,v in SERVERS_TABLES.items()}  


    print("Creating VMs tables...")
    VMS_TABLES = {_node:TASKS.get_scheduler(_node).get_vms_status()  for _node in TASKS.nodes() if _node != 'ROOT'}
    for k,v in VMS_TABLES.items():
        v.loc[:,['vcpus','memory']] = v.loc[:,['vcpus','memory']].astype(int) 

    SAVE_DATA['VMS_TABLES'] = {k:v.to_dict() for k,v in VMS_TABLES.items()}
    SAVE_DATA['SIMULATIONS_LIST'] = [_ed for _ed in TASKS.edges(data=True) if _ed[0] != 'ROOT'] 
    SAVE_DATA['SERVER_TYPES'] = [_server_type.get_info().to_dict('records')[0] for i,_server_type in  enumerate(SERVER_TYPES)]
    SAVE_DATA['FLAVORS_VM'] = [_flavor_vm.get_info().to_dict('records')[0] for i,_flavor_vm in  enumerate(FLAVORS_VM)]
    SAVE_DATA['STATE_LIST'] = [_node for _node in TASKS.nodes() if _node != 'ROOT']
    
    
    
    _file_out = file_path.replace('.xlsx','.json')
    print("Saving file in: %s"%_file_out)

    with open(_file_out, 'w') as outfile:
        json.dump(dict(SAVE_DATA), outfile)


# In[ ]:




