#!/usr/bin/env python3
# coding: utf-8

# In[1]:


from random import randint, choice
import pandas as pd
import numpy as np
import json
import argparse

from collections import defaultdict


# In[2]:


#from jupyter_dash import JupyterDash

import dash
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from dash import dash
from dash import dcc
from dash import html
from dash import dash_table
from dash import callback_context


import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
import plotly.express as px
import random


import os


from COLORS import COLORS 


# In[3]:


######## PRESENTATION #################################
def create_dashtable_from_df(df):
    
    _dash_table = dash_table.DataTable(
        id='table-virtualization-'+str(randint(1000,9999)),
        data=df.to_dict('records'),
        columns=[{'name': i, 'id': i} for i in df.columns],
        virtualization=False,
        page_action='none',
        page_size=20,
        filter_action="none",
        style_header={
            'backgroundColor': 'white',
            'fontWeight': 'bold'
        },
        style_cell = {
                'font_family': 'cursive',
                'font_size': '11px',
                'text_align': 'center',
                'textAlign': 'left'
            },
    )
    

    return dbc.Row([_dash_table])


def write_title(text, size):
    return html.P("%s"%text, className="lead",style={'font-size': str(size)+'px'})

def create_cards(task_name, datacenter):
    #DEV4
    _avz_data = AVZ_TABLES[task_name]
    _avz_data = _avz_data[ _avz_data.datacenter == datacenter]
    
    _server_data = SERVERS_TABLES[task_name]
    _server_data  = _server_data[_server_data.datacenter == datacenter]
        
    def create_card_1(curr_val, header, bg_color, delta=0, mode="number", suffix=""):

        _fig = go.Figure(go.Indicator(
                mode = mode,
                value = curr_val,
                delta={'reference': delta}, #usando con 'number+delta'
                number = {'suffix': suffix},
                #domain = {'x': [0, 1], 'y': [0, 1]},
                domain= {"row": 0, "column": 0},
                title = {'text': header, "font": { "size": 20 }},
            )
        )
        _fig.update_layout(height=200, paper_bgcolor=bg_color, font= {'color': 'black', 'size':1})

        return _fig
    
 
    CARDS_INIT ={
            'vCPUs reservadas': {
                'value':int(100*(_avz_data.vcpus_reserved.sum() / _avz_data.vcpus.sum())),
                'suffix':" %",
                'bg_color': '#D3DEDC'
            },
            'Servidores':  {
                'value':_avz_data.number_servers.sum() - len(_server_data[_server_data.server.str.match(r'vserver_auto(.*)')].groupby('server').count()),
                'suffix':" units",
                'bg_color':'#92A9BD'
            },
            'Servidores Adicionales':  {
                'value':_server_data.groupby('server').agg(virtual=('virtual',np.all)).sum().values[0],
                'suffix':" units",
                'bg_color':'#CCD1E4'
            }
        }
    
    _row_header = dbc.Row(html.P("Datacenter: %s"%datacenter, className="lead",style={'font-size':'17px'}))
    _row_content = dbc.Row([dbc.Col(  dcc.Graph(figure=create_card_1(v['value'], k, v['bg_color'], suffix=v['suffix'])), width=3)  for k,v in CARDS_INIT.items() ])

    
    return dbc.Row([_row_header,_row_content,dbc.Row([html.Br()])])

def create_table_chart(task_name, datacenter):
    
    _server_data = SERVERS_TABLES[task_name]
    _server_data  = _server_data[_server_data.datacenter == datacenter] 
    
    _data  =_server_data.groupby('avz').agg({
        'vcpus':'sum',
        'vcpus_reserved':'sum',
        'vcpus_free':'sum',
        'vcpus_system':'sum',
        'vms':'sum',
        'server':lambda x: len(np.unique(x))}
    ).rename(columns={"server": "servers"})
    
 
    _data['utilization'] = (_data.vcpus_reserved/_data.vcpus).apply(lambda x: "%0.0f%%"%(100*x))
    _data.reset_index(inplace=True)
    
    
    _row_header = dbc.Row(html.P("Datacenter: %s"%datacenter, className="lead",style={'font-size':'17px'}))
    _row_content = dbc.Row([create_dashtable_from_df(_data)])


    return dbc.Row([_row_header,_row_content,dbc.Row([html.Br()])])


def create_bar_charts(task_name, datacenter):
    _vms_data = VMS_TABLES[task_name]
    _vms_data  = _vms_data[_vms_data.datacenter == datacenter] 
    #DEV2
    
    _bar_chart = _vms_data.groupby(['vnf']).agg({'vcpus':'sum','memory':'sum','server_cluster_id':'max'})
    _bar_chart.reset_index(inplace=True)
    _bar_chart['datacenter'] = datacenter
    _bar_chart.columns = ['VNF', 'vCPUS reserved', 'memory', 'server_cluster_id','datacenter']
    _bar_chart['colors'] = _bar_chart.apply(lambda x: VNF_COLORS[x.VNF], axis=1)
    _bar_chart = _bar_chart.sort_values(by='VNF')
        
    _fig = go.Figure(data=[
            go.Bar(x=_bar_chart.VNF, y=_bar_chart['vCPUS reserved'], marker_color=_bar_chart.colors)
    ])
    
    _fig.update_layout(title_text='Datacenter: %s'%datacenter, barmode='stack')

    
    return dbc.Row([dcc.Graph(id='create_bar_charts_fig_'+str(randint(0,10000)), figure=_fig)])
    
def create_histogram_charts(task_name, datacenter):
    
    _servers_data = SERVERS_TABLES[task_name]
    _servers_data  = _servers_data[_servers_data.datacenter == datacenter] 
    
    _df = _servers_data.groupby('server').sum().apply(lambda x: 100*(x.vcpus_free/x.vcpus), axis=1).to_frame()
    _df.columns = ['% utilización vCPUs per Server']

    _row_header =  html.P("Datacenter %s"%datacenter, className="lead",style={'font-size':'15px'})
    _row_content = dcc.Graph(id='create_histogram_charts_'+str(randint(1,1000)), figure=px.histogram(_df, x="% utilización vCPUs per Server", nbins=7) ) 
    
    return dbc.Row([_row_header,_row_content,dbc.Row([html.Br()])]) 
    
def create_resume_tables(task_name, datacenter):
    _servers_data = SERVERS_TABLES[task_name]
    _servers_data  = _servers_data[_servers_data.datacenter == datacenter] 
    
    _count_servers = _servers_data.server_type.value_counts().to_frame().rename({'server_type':'Num Servers'}, axis=1)
    _row_content_1_df = pd.concat([SERVER_TYPES,_count_servers], axis=1).rename({'name':'Server Types'}, axis=1)
    _row_content_1_df = _row_content_1_df[ pd.notnull(_row_content_1_df['Num Servers']) ]
    
    _row_header_1 =  html.P("Datacenter %s"%datacenter, className="lead",style={'font-size':'15px'})
    _row_content_1 = create_dashtable_from_df(_row_content_1_df)
    
    return dbc.Row([_row_header_1,_row_content_1,dbc.Row([html.Br()])])

def create_file_server():
    _servers_data = pd.DataFrame()
    for k,v in SERVERS_TABLES.items():
        print("Generating info of State: %s"%k)
        _data = v
        _data['state'] = k
        _servers_data = pd.concat([_servers_data, _data])
        
    _servers_data = _servers_data.reset_index(drop=True)
    
    return _servers_data

def create_file_vms():    
    print()
    _vms_data = pd.DataFrame()
    for k,v in VMS_TABLES.items():
        print("Generating info of State: %s"%k)
        _data = v
        _data['state'] = k
        _vms_data = _vms_data.append(_data)

    _vms_data = _vms_data.reset_index(drop=True)
    
    return _vms_data

def create_logs_file():
    _logs = TASKS.logger.get_info()
    for _node in TASKS.nodes():

        if _node == 'ROOT':
            continue

        print("Generating Log info from SCH: %s"%_node)
        _sch = TASKS.get_scheduler(_node)

        _logs = _logs.append(_sch.logger.get_info())
        for idx,_server in _sch.servers.items():
            print("Generating Log info from SERVER: %s \r"%_server.name, end='')
            _logs = _logs.append(_server.logger.get_info())
           #clear_output(wait=True)

    _logs = _logs.sort_values(by='timestamp').reset_index(drop=True)
    
    return _logs




def get_datacenters(task_name):
    return SERVERS_TABLES[task_name].datacenter.unique()

def create_content_home(task_name):

    _datacenters = get_datacenters(task_name)
    CARDS_TOTAL = [html.Hr(className="my-2"), write_title("Utilización Totales por AVZs",22) ] + [create_cards(task_name,dc) for dc in _datacenters] 
    TABLES_AVZ = [html.Hr(className="my-2"), write_title("Tablas",22) ] + [create_table_chart(task_name,dc) for dc in _datacenters] 
    BARCHARTS = [html.Hr(className="my-2"), write_title("Graficos",22) ] + [create_bar_charts(task_name,dc) for dc in _datacenters]
    HISTCHARTS = [html.Hr(className="my-2"), write_title("Historial",22) ] + [create_histogram_charts(task_name,dc) for dc in _datacenters]
    RESUMECHARTS = [html.Hr(className="my-2"), write_title("Types",22) ] + [create_resume_tables(task_name,dc) for dc in _datacenters]
    DOWNLOAD_BTS = [dbc.Row([
        dbc.Col([ html.Button("Download Server Report", id="btn_sever_xlsx"), dcc.Download(id="download_df_server_xlsx"),]),
        dbc.Col([ html.Button("Download VMs Report", id="btn_vms_xlsx"), dcc.Download(id="download_df_vms_xlsx"),]),
        dbc.Col([ html.Button("Download Template", id="btn_template_xlsx"), dcc.Download(id="download_template_xlsx"),]),
        dbc.Col([ html.Button("Download logs Report", id="btn_logs_xlsx"), dcc.Download(id="download_df_logs_xlsx"),])
    ])]
    
    CONTENT = CARDS_TOTAL + TABLES_AVZ + BARCHARTS + HISTCHARTS + RESUMECHARTS + DOWNLOAD_BTS

    return html.Div(CONTENT,className="p-1 bg-light rounded-3")



# In[4]:


######### SIMULATIONS PAGE  #################
def create_dashtable_from_df_2(df):
    
    return dash_table.DataTable(
        id='table-virtualization-'+str(randint(1000,9999)),
        data=df.to_dict('records'),
        columns=[{'name': i, 'id': i} for i in df.columns],
        virtualization=False,
        page_action='none',
        page_size=20,
        filter_action="none",
        style_header={
            'backgroundColor': 'white',
            'fontWeight': 'bold'
        },
        style_cell = {
                'font_family': 'cursive',
                'font_size': '11px',
                'text_align': 'center',
                'textAlign': 'left'
            },
        merge_duplicate_headers=True,
    )
    

def create_comp_table(task_name1, task_name2):
    
    _data_left = AVZ_TABLES[task_name1].copy()[['vcpus','vcpus_reserved','vcpus_free','number_servers','number_vms','servers_not_available']] 
    _data_left_comp = _data_left[['vcpus','vcpus_reserved','vcpus_free','number_servers','number_vms','servers_not_available']].copy()
    _data_left.columns = pd.MultiIndex.from_tuples([(task_name1,'a_'+x) for x in _data_left.columns], names=["State", "Value"])
    
    _data_right = AVZ_TABLES[task_name2].copy()[['vcpus','vcpus_reserved','vcpus_free','number_servers','number_vms','servers_not_available']] 
    _data_right_comp = _data_right[['vcpus','vcpus_reserved','vcpus_free','number_servers','number_vms','servers_not_available']].copy()    
    _data_right.columns = pd.MultiIndex.from_tuples([(task_name2,'b_'+x) for x in _data_right.columns], names=["State", "Value"])
    
    _merge_table  = pd.concat([_data_left, _data_right], axis=1).fillna(0)
    
    
    T1_1 = _merge_table[task_name1]
    T1_1.columns = ['vcpus','vcpus_reserved','vcpus_free','number_servers','number_vms','servers_not_available']
    
    T1_2 = _merge_table[task_name2]
    T1_2.columns = ['vcpus','vcpus_reserved','vcpus_free','number_servers','number_vms','servers_not_available']
    
    _comp_table_red = (T1_1 - T1_2) < 0
    _comp_table_blue = (T1_1 - T1_2) > 0
    _merge_table.reset_index(inplace=True)
    

    _data_values = [{(k1[1] if k1[1] != '' else 'avz' ):v1 for k1,v1 in k.items()} for k in _merge_table.to_dict('records')]
    _data_columns = [{'name':list(x), 'id':(x[1] if x[1] != '' else 'avz')} for x in _merge_table.columns ]


    _conditional_format = []
    for col in _comp_table_red:
        for i,v in enumerate(_comp_table_red[col]):
            if v:
                _cond = {
                        'if': {
                            'column_id': 'b_'+col,
                            'row_index': i
                        },
                        #'backgroundColor': '#85144b',
                        'color': 'red'
                    }

                _conditional_format.append(_cond)
    
    for col in _comp_table_blue:
        for i,v in enumerate(_comp_table_blue[col]):
            if v:
                _cond = {
                        'if': {
                            'column_id': 'b_'+col,
                            'row_index': i
                        },
                        #'backgroundColor': '#85144b',
                        'color': 'blue'
                    }

                _conditional_format.append(_cond)
                
    _dash_table = create_dashtable_from_df_2(_merge_table)
    _dash_table.data = _data_values
    _dash_table.columns = _data_columns
    _dash_table.style_data_conditional = _conditional_format

    return _dash_table



def create_content_simulations():
    SIM_ROWS =  []
    for _sim in SIMULATIONS_LIST:
        SIM_ROWS.append( dbc.Row(html.P(_sim[2]['label'])) )
        SIM_ROWS.append( dbc.Row( create_comp_table(_sim[0], _sim[1]), id='test') )
        SIM_ROWS.append( dbc.Row( html.Hr(className="my-3")) )
        SIM_ROWS.append( dbc.Row( html.Br()) )
    
    return html.Div(SIM_ROWS,className="p-1 bg-light rounded-3")
    
   


# In[5]:


######### TETRIS PAGE  #################
def create_tetris_bar(avz_data, avz_name, state):

    _height_ds = 50
    _component_colors = COMPONENT_COLORS # {x:random.choice(COLORS) for x in avz_data['component'].unique()}
    
    _servers_status = SERVERS_TABLES[state]
    _servers_status = _servers_status[_servers_status['avz'] == avz_name]
    
    _fig = go.Figure()
    
    
    for k,v in avz_data.groupby('component'):
        v = v.sort_values(by='server_deployed')

        _fig.add_trace(
            go.Bar(
                y=[v.server_deployed, v.processor_deployed],
                x=v.vcpus,
                orientation='h',
                name=k,
                marker=dict(color=_component_colors[v.component.values[0]], line=dict(color='black', width=2),  opacity=.2), 
                hovertext= v.vm
            )
        )

    _fig.add_trace(
        go.Bar(
            y=[_servers_status.server, _servers_status.processor],
            x=_servers_status.vcpus_free,
            orientation='h',
            name='free',
            marker=dict(color='white', line=dict(color='black', width=1), opacity=.4),
            hovertext= 'free',
        )
    )
    
    _fig.add_trace(
        go.Bar(
            y=[_servers_status.server, _servers_status.processor],
            x=_servers_status.vcpus_system,
            orientation='h',
            name='system',
            marker=dict(color='red', line=dict(color='black', width=1), opacity=.4),
            hovertext= 'system',
        )
    )
    
    _fig.update_layout(
        xaxis=dict(title_text="vCores per Processor"),
        yaxis=dict(title_text="Servers"),
        title = avz_name,
        showlegend=True,
        barmode="stack",
        width=1000,
        height= _height_ds*len(avz_data.server_deployed.unique()) #900
    )
    
    return _fig

def create_table_tetris(avz_data, avz_name, state):
    
    _servers_status = SERVERS_TABLES[state]
    _servers_status = _servers_status[_servers_status['avz'] == avz_name]

    _aditional_s =_servers_status.loc[_servers_status.virtual,'server']
    
    _rows = []
    _d = defaultdict(lambda x: '')
    _d['Number Servers'] = len(avz_data['server_deployed'].unique()) - len(_aditional_s.unique())
    _d['Aditional Servers'] = len(_aditional_s.unique())
    _d['Name AVZ'] = avz_name
    _d['vCPU Capacity'] = str(_servers_status['vcpus'].sum()) + " vcpus"
    _d['vCPU Reserved'] = str(avz_data['vcpus'].sum()) + " vcpus"
    _d['vCPU System'] = str(_servers_status['vcpus_system'].sum()) + " vcpus"
    _d['vCPU Free'] = str(_servers_status['vcpus_free'].sum()) + " vcpus"
    
    
    return html.Div([html.Br(),dbc.Table([html.Tr([html.Td(_i[0]), html.Td(_i[1])])  for _i in _d.items()], bordered=True)])


def create_tetris(task_name, datacenter):
    _vms_data = VMS_TABLES[task_name]
    _vms_data  = _vms_data[_vms_data.datacenter == datacenter]
    
    _row_header = dbc.Row(html.P("Datacenter: %s"%datacenter, className="lead",style={'font-size':'17px'}))
    _row_content = [
        dbc.Row([
            dbc.Col([ dcc.Graph(id='graph_tetris_'+str(k), figure=create_tetris_bar(v,k, task_name))]),
            dbc.Col([create_table_tetris(v,k, task_name)])
        ])
        
        for k,v in _vms_data.groupby('avz')]
    
    return dbc.Row([_row_header] + _row_content)
    

    
    
def create_content_tetris(task_name):
    _datacenters = get_datacenters(task_name)
    TETRIS_CONTENT = [create_tetris(task_name,dc) for dc in _datacenters] 
    
    return html.Div(TETRIS_CONTENT,className="p-1 bg-light rounded-3")


# In[6]:


def create_header(task_name):
    return dbc.Row([
        dbc.Container([
            dbc.Row([
            dbc.Col(html.P("Reporte NFVI", className="display-4")),
            dbc.Col(html.P("Utilizacion de Recursos de DataCenter dentro del Proyecto OneCore.", className="lead",style={'font-size':'17px'})),
            dbc.Col(SELECT_TAB),
            ]),                     
        ],fluid=True,className="py-1"),
        html.Hr(className="my-2"),
        dbc.Row([dbc.Col(html.P(task_name, className="display-6", id='state_title')),dbc.Col(html.Div(""), width=3)], align="left")
    ])




# In[7]:


parser = argparse.ArgumentParser(description='Processor of Template - NFVI Report')
parser.add_argument('-f', '--file', type=str, required=True, help='JSON file of the NFVi Template')


#app = JupyterDash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
#app.config.suppress_callback_exceptions = True
#app.config['suppress_callback_exceptions'] = True

DIR_TEMPLATE = '/usr/app/template/'
    
if __name__ == '__main__':
    
    args = parser.parse_args()
    
    file_path = DIR_TEMPLATE + args.file
    if os.path.isfile(file_path):
        print ("File found .......... OK: %s"%file_path)
    else:
        raise Exception("File not exist: %s"%file_path)
    
    
    with open(file_path) as json_file:
        JSON_DATA = json.load(json_file)
    
    AVZ_TABLES = {k:pd.DataFrame(v) for k,v in JSON_DATA['AVZ_TABLES'].items()}
    SERVERS_TABLES = {k:pd.DataFrame(v) for k,v in JSON_DATA['SERVERS_TABLES'].items()}
    VMS_TABLES = {k:pd.DataFrame(v) for k,v in JSON_DATA['VMS_TABLES'].items()}

    SIMULATIONS_LIST = JSON_DATA['SIMULATIONS_LIST']
    SERVER_TYPES = pd.DataFrame(JSON_DATA['SERVER_TYPES'])
    FLAVORS_VM = pd.DataFrame(JSON_DATA['FLAVORS_VM'])
    STATE_LIST =  JSON_DATA['STATE_LIST']

    COMPONENTS = pd.Series(dtype=str)
    for k,v  in VMS_TABLES.items():
        COMPONENTS  = pd.concat([COMPONENTS,v['component']])       
    COMPONENTS = COMPONENTS.unique()    

    VNFS = pd.Series(dtype=str)
    for k,v  in VMS_TABLES.items():
        VNFS  = pd.concat([VNFS,v['vnf']])
    VNFS = VNFS.unique()    

    DATACENTERS = pd.Series(dtype=str)
    for k,v  in SERVERS_TABLES.items():
        DATACENTERS  = pd.concat([DATACENTERS,v['datacenter']])
    DATACENTERS = DATACENTERS.unique()
    
    SELECT_TAB = dbc.Row([
        dcc.Dropdown(
        id='select_state',
        options=[{'label': x, 'value': x} for x in STATE_LIST],
        value=STATE_LIST[0]
        ) 
    ]) 

    COMPONENT_COLORS = {x:COLORS.pop() for x in COMPONENTS if x != ''}
    VNF_COLORS = {x:COLORS.pop() for x in VNFS if x != ''}
    TASK_NAME = STATE_LIST[0]
       
    app.layout = html.Div(
    [
        dbc.Tabs(
            [
                dbc.Tab(label="Home", tab_id="tab-0"),
                dbc.Tab(label="Simulations", tab_id="tab-1"),
                dbc.Tab(label="Tetris", tab_id="tab-2"),
            ],
            id="tabs",
            active_tab="tab-0",
        ),
        
        create_header(TASK_NAME),
        html.Div(id="content", style={'height':'100%','width':'100%'}),
    ],
    style={
        'height':'80%',
        'width':'100%'
        }
    )


    @app.callback([
        Output("content", "children"), 
        Output("state_title", "children")
        ],
        [
            Input("tabs", "active_tab"),
            Input('select_state', 'value')
        ])
    def switch_tab(select_tab, select_state):
        clicked = callback_context.triggered[0]['prop_id'].split('.')[0]
        task_name = select_state

        if not clicked:
            return create_content_home(task_name),select_state 

        if select_tab == 'tab-0':
            print("HOME PAGE")
            return create_content_home(task_name),select_state
        if select_tab == 'tab-1':
            print("SIMULATIONS PAGE")
            return create_content_simulations(),select_state
        if select_tab == 'tab-2':
            print("TETRIS PAGE")
            return create_content_tetris(task_name),select_state

    @app.callback(
        [Output("download_df_server_xlsx", "data"),
         Output("download_df_vms_xlsx", "data"),
         Output("download_template_xlsx", "data"),
         Output("download_df_logs_xlsx", "data")],
        [Input("btn_sever_xlsx", "n_clicks"),
        Input("btn_vms_xlsx", "n_clicks"),
        Input("btn_template_xlsx", "n_clicks"),
        Input("btn_logs_xlsx", "n_clicks")]
    )
    def func_server(btn_server, btn_vms, btn_tamplate, btn_logs):
        changed_id = [p['prop_id'] for p in callback_context.triggered][0]
        _res_output = [None,None,None,None]

        if changed_id is None:
            raise PreventUpdate

        if 'btn_sever_xlsx' in changed_id:
            _res_output[0] = dcc.send_data_frame(create_file_server().to_excel, "servers_report_%s.xlsx"%randint(1,1000), sheet_name="Servers")

        if 'btn_vms_xlsx' in changed_id:
            _res_output[1] = dcc.send_data_frame(create_file_vms().to_excel, "vms_report_%s.xlsx"%randint(1,1000), sheet_name="VMs")

        if 'btn_template_xlsx' in changed_id:
            _res_output[2] =  dcc.send_file(file_path.replace('.json','.xlsx'))

        if 'btn_logs_xlsx' in changed_id:
            _res_output[3] = dcc.send_data_frame(create_logs_file().to_excel, "logs_report_%s.xlsx"%randint(1,1000), sheet_name="Logs")        

        return tuple(_res_output) 
    
    
    
    app.run_server(host='0.0.0.0',  port=8151)
    
    
    


# In[ ]:




