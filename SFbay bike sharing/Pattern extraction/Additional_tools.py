from datetime import datetime
from math import sin, cos, sqrt, atan2, radians
from pyspark.sql import Row

def stateFunctionF(docks_available, bikes_available):
    if docks_available==0:
        return 1
    elif (docks_available==1 or docks_available==2):
        return 0
    else:
        return 2
    
def stateFunctionE(docks_available,bikes_available):
    if bikes_available==0:
        return 1
    elif (bikes_available==1 or bikes_available==2):
        return 0
    else:
        return 2

def getMapF(line):
    id_station = str(line[0])
    year = int(line[1])
    month = int(line[2])
    day = int(line[3])
    hour = int(line[4])
    minute = int(line[5])   
    timestamp = datetime(year, month, day, hour, minute)  
    status = int(line[6])
    if status==0:
        status='AlmostFull'
    else:
        status='Full'
    info = id_station.split('.')[0]+'_'+status
    return ((timestamp, info))

def getMapE(line):
    id_station=str(line[0])
    year=int(line[1])
    month=int(line[2])
    day=int(line[3])
    hour=int(line[4])
    minute=int(line[5])   
    timestamp= datetime(year,month, day, hour, minute)  
    status=int(line[6])
    if status==0:
        status='AlmostEmpty'
    else:
        status='Empty'
    info=id_station.split('.')[0]+'_'+status
    return ( (timestamp,info))

def reduceKeys(line):   
    lista=[]
    #lista.append(line[0])
    line_split=line[1].split("-")
    #return line_split[0]
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
    #dict[seq]=line[1]
    for i, window in enumerate(seq):
        if i>0:
            final+='-'
        for j, el in enumerate(window):
            if j>0:
                final+=','
            final+=el
    final+=(';'+str(line[1])+';'+str(i))
    return final  