import time
from datetime import datetime
from math import sin, cos, sqrt, atan2, radians
from pyspark.sql import Row
import numpy as np
import re

def getMapF(line):
    id_station = str(line[0])   
    timestamp = line[1]
    status = int(line[3])
    
    if status==0:
        status='AlmostFull'
    elif status==1:
        status='Full'
    else:
        status='Normal'
    info = id_station.split('.')[0]+'_'+status
    return (timestamp, info)

def getMapE(line):
    id_station=str(line[0])
    timestamp= line[1]
    status=int(line[3])
    
    if status==0:
        status='AlmostEmpty'
    elif status==1:
        status='Empty'
    else:
        status='Normal'
    info=id_station.split('.')[0]+'_'+status
    return (timestamp,info)

# map tuple of timestamps and states to ordered states
def ordered_state_mapper(l):
    window_ids = []
    states = []
    
    # sort states
    for el in l:
        window_ids.append(el[0])
        states.append(el[1])

    # concatenate the string
    for i, idx in enumerate(np.argsort(window_ids)):
        if i == 0:
            string = states[idx]
        else:
            string = string + '-' + states[idx]
    
    return string

# write T0, T1, ecc.
def reduceKeys(line):   
    lista=[]
    line_split=line[1].split("-")
    
    count=len(line_split)
    tot=[]
    for val in range(count):
        li=[]
        stations=line_split[val].split(',')
        for st in stations:
            all_string_st=st.split('_')[0]+'_'+'T'+str(val)+'_'+st.split('_')[1]
            li.append(all_string_st)
        tot.append(li)
    lista.append((line[0],(tot))) 
    return lista

#function to retrieve distance between 2 stations
def getDistance(station1, station2):
    # approximate radius of earth in km
    R = 6373.0    
    lat_a=float(station1.split(',')[0])
    lat_b=float(station2.split(',')[0])
    long_a=float(station1.split(',')[1])
    long_b=float(station2.split(',')[1])
    
    lat1=radians(lat_a)
    lat2=radians(lat_b)
    lon1=radians(long_a)
    lon2=radians(long_b)
    
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance


def row_seq(line):
    true=line[1]
    string=Row(sequence=true)
    return string

def giveSelected(line):
    seq=line[0]
    found=False   
    for window in seq:
        for el in window:
            if 'T0' in el and '_0' in el:
                found=True
                break
    return found 

def mapValues(line):
    seq=line[0]
    final=''
    
    for i, window in enumerate(seq):
        if i>0:
            final+='-'
        for j, el in enumerate(window):
            if j>0:
                final+=','
            final+=el
    final+=(';'+str(line[1])+';'+str(i))
    return final

def event_has_rule(rule_event, e_window):
    has_it = False
    state = re.sub(r"[\[\]']", '', rule_event.split('_')[0])
    distance = re.sub(r"[\[\]']", '', rule_event.split('_')[2])
    
    for e in e_window:
        if re.match(rf"{state}_T._{distance}", e):
            has_it = True
    
    return has_it

def all_rule_items_match(rule_windows, event_windows):
    '''
    rule_windows  = [["Event_T._distance", ...], [...], ...]
    event_windows = [["Event_T._distance", ...], [...], ...]
    '''
    all_match = True
    
    for rule_window, event_window in zip(rule_windows, event_windows):
        for rule_event in rule_window.split(','):
            if not event_has_rule(rule_event, event_window):
                all_match = False
                break

    return all_match