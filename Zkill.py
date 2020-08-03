# -*- coding: utf-8 -*-
"""
Created on Sun Jul 26 16:06:43 2020

@author: Harlan Johnson
"""
import os
from airtable import airtable
import json
import time
import pprint
import requests
import pandas as pd
import copy
import sched
from time import time

frame_aggregation = []
victim_aggregation = []
attackers_aggregation = []
last_update = time()
os.environ["AIRTABLE_KEY"] = "keyAyLi47AijQ6KOv"

def retrieve():
  response = requests.get("https://redisq.zkillboard.com/listen.php?queueID=DataGod9674429039")
  data = response.json()
  return data

def full_data(data):
    data_testing = copy.deepcopy(data)
    killmail_full = {}
    data_no_package = data_testing['package']
    data_ID = {
        'killID': data_no_package['killID'],
        }
    data_killmail = data_no_package['killmail']
    data_zkb = {
        'zkb': data_no_package['zkb'],
        }
    killmail_full[data_no_package['killID']] = data_killmail
    killmail_full[data_no_package['killID']].update(data_zkb)
    return killmail_full

def frame_data(data):
    data_frame_testing = copy.deepcopy(data)
    killmail_frame = {}
    killmail_victim_items = {}
    killmail_attackers = {}
    killmail_frame = data_frame_testing['package']
    killmail_remove = killmail_frame.pop('killmail')
    killmail_frame.update(killmail_remove)
    frame_attackers = killmail_frame.pop('attackers')
    frame_victim_items = killmail_frame['victim'].pop('items')
    killmail_attackers_data = []
    for attacker in frame_attackers:
        killmail_attackers = {}
        killmail_attackers.update({'killID': killmail_frame['killID'],
                                   'killmail_time': killmail_frame['killmail_time'],
                                   'solar_system_id':killmail_frame['solar_system_id'],
                                   'zkb':killmail_frame['zkb'],
                                   'total_attackers': len(frame_attackers),})
        killmail_attackers.update(attacker)
        #print(attacker)
        #killmail_attackers['victim'] = 
        killmail_attackers.update({'victim':killmail_frame['victim']})
        killmail_attackers_data.append(killmail_attackers)
        #print("attacker")
        if attacker['final_blow'] == True:
            killmail_frame.update({'attacker':attacker})
        else:
            pass
    killmail_victim_data = []
    for item in frame_victim_items:
        killmail_victim_items = {}
        killmail_victim_items.update({'killID': killmail_frame['killID'],
                                   'killmail_time': killmail_frame['killmail_time'],
                                   'solar_system_id':killmail_frame['solar_system_id'],
                                   'zkb':killmail_frame['zkb'],
                                   'total_items':len(frame_victim_items)})
        killmail_victim_items.update(item)
        killmail_victim_items.update(killmail_frame['victim'])
        killmail_victim_data.append(killmail_victim_items)
        #print("entry")
        #pprint.pprint(killmail_victim_data)
        #print("victim item")
    return killmail_frame, killmail_victim_data, killmail_attackers_data
        
   

def list_to_dict(data_set_list, existing_list):
    data = copy.deepcopy(data_set_list)
    result = copy.deepcopy(existing_list)
    for item in data:
        result.append(copy.deepcopy(item))
    #print("added")
    #print(result)
    return result

def data():
    global frame_aggregation
    global victim_aggregation
    global attackers_aggregation
    global last_update
    data_z = retrieve()
    full_print = full_data(data_z)
    frame_print = frame_data(data_z)
    #pprint.pprint(full_print)
    #pprint.pprint(frame_print)
    with open('full_data.json', "a") as full_w:
        json.dump(full_print, full_w)
    #pprint.pprint(frame_aggregation)
    frame_aggregation.append(frame_print[0])
    #pprint.pprint(frame_aggregation)
    victim_aggregation = list_to_dict(frame_print[1], victim_aggregation)
    attackers_aggregation = list_to_dict(frame_print[2], attackers_aggregation)
    #pprint.pprint(victim_aggregation)
    #attackers_aggregation.update(frame_print[2])
    if time()-last_update > -1:    
        save_data(frame_aggregation, 'frame_data.csv')
        save_data(victim_aggregation, 'item_data.csv')
        save_data(attackers_aggregation, 'attackers_data.csv')
        airtable_export(frame_aggregation,'Frame')
        airtable_export(victim_aggregation,'Victim_Items')
        airtable_export(attackers_aggregation,'Attackers')
        last_update = time()
        frame_aggregation = []
        victim_aggregation = []
        attackers_aggregation = []
    #print("data")
    
def save_data(data_set, location):
    data = copy.deepcopy(data_set)
    #print(location+" : "+str(len(data)))
    df_exist = copy.deepcopy(pd.read_csv(location))
    for item in data:
        #print(item)
        #data_json = json.load(item)
        df_temp = pd.DataFrame(pd.json_normalize(item))
        #print(df_temp)
        df_exist = pd.concat([df_exist, df_temp],ignore_index=True)
    #print(df_exist)
    df_exist.to_csv(location, mode = 'w',index=False)
    last_update = time()
    print("Update "+location+": "+str(time()))
    #print("data saved - "+location)
    
def airtable_export(json_file,table):
    base_key = 'appaTgY78Ycqm3PkP'
    table_name = table
    airtable = airtable(base_key, table_name, api_key=os.environ['AIRTABLE_KEY'])
    file_insert = []
    for item in json_file:
        #pprint.pprint(item)
        typetest = pd.DataFrame(pd.json_normalize(item))
        #print(typetest)
        typetest3 = json.loads(typetest.to_json(orient='records',lines=True))
        file_insert.append(typetest3)
        #pprint.pprint(typetest3)
        #typetest3.update({'typecast': True})
        #print(type(typetest3))
        #airtable.insert(typetest3, typecast=True)
    #pprint.pprint(file_insert)
    airtable.batch_insert(file_insert, typecast=True)
    

 
        
while True:
    data()

def testing():
    for item in json_file:
        pprint.pprint(item)
        typetest = pd.json_normalize(item)
        typetest2 = typetest2.to_json()
        typetest2.update({'typecast': True})
        print(type(typetest2))
        pprint.pprint(typetest2)
        airtable.batch_insert(typetest2,True)
        
#with open(r"C:\Users\Harlan Johnson\Zkill Data Aggregation\frame_data.json", "r") as frame_r:
    #df = pd.json_normalize(frame_r)
    #print(df)
