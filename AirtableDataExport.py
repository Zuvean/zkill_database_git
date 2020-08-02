# -*- coding: utf-8 -*-
"""
Created on Mon Jul 27 13:02:25 2020

@author: Harlan Johnson
"""
import os
from pprint import pprint
from airtable import Airtable
import pandas as pd
import json
import time
import pprint
import requests
import numpy as np
import copy
import sched
from time import time

os.environ["AIRTABLE_KEY"] = "keyAyLi47AijQ6KOv"
base_key = 'appaTgY78Ycqm3PkP'
table_name = 'Frame'
airtable = Airtable(base_key, table_name, api_key=os.environ['AIRTABLE_KEY'])
print(airtable)

df = pd.read_csv('frame_data.csv', 'r')
#data = df.to_json()

print(df)

print(len(df))

for item in df:
    print(item)
#airtable.batch_insert(data)
