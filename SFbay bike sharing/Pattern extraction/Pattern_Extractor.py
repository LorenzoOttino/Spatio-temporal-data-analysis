from pyspark.ml.fpm import PrefixSpan
import pyspark.sql.functions as F
import pandas as pd
import sys
import matplotlib.pyplot as plt
import os
import time
from datetime import datetime
import ast
import re
from Additional_tools import *

class Pattern_Extractor():
    def __init__(self, interval, maxDelta, th, window_size, status_path, station_path, spark, sc):
        self.interval = interval
        self.maxDelta = maxDelta
        self.th = th
        self.window_size = window_size
        self.status_path = status_path
        self.station_path = station_path
        self.spark = spark
        self.sc = sc        
        
    def get_station_info(self, tot_id_stations):
        #load station file
        stationsDF = self.spark.read.format("csv").option("delimiter", ",").option("header", True).option("inferSchema", True).load(self.station_path)

        #get only the used stations 
        necessary_rows = stationsDF.filter(F.col("id").isin(tot_id_stations)).sort("id").rdd.map(tuple)

        #get info of stations about coordinates and name
        coordinates = necessary_rows.map(lambda line: (line[0],(str(line[2])+','+str(line[3]))))
        names_stations = necessary_rows.map(lambda line: (line[0],line[1]))
        list_coo = coordinates.collect()

        #dict in which the key is a pair of stations and value is the distance
        dict_distances={}

        for i in range(len(list_coo)):
            for j in range(i+1,len(list_coo)):
                station1=list_coo[i][0]
                station2=list_coo[j][0]
                d_i=list_coo[i][1]
                d_j=list_coo[j][1]
                distance=getDistance(d_i,d_j)
                id_stations=str(station1)+' '+str(station2)
                dict_distances[id_stations]=distance

        return dict_distances


    
    def extract_items(self, extraction_type, neighborhood_type='distance', n_neighbors=5, incr_dec_threshold=1, wrap_states=False, state_change=False, for_test=False, importance_path='../../Data/edge_importance.csv'):
        '''
        Extract all items with specified charachteristics
        extraction_type: target extraction type
        neighborhood_type: kind of neighborhood considered
        n_neighbors: number of neighbors to consider if neighborhood_type=='indegree'
        incr_dec_threshold: threshold for considering the state decreasing or increasing
        wrap_states: boolean parameter that considers Full == AlmostFull if extraction_type == 'Full-Decrease'. Similar behaviour for 'Empty-Increase'
        state_change: boolean parameter that considers AlmostFull only if state was not AF in the first timestamp if extraction_type == 'Full-Decrease'. Similar behaviour for 'Empty-Increase'
        for_test: extract also 'Normal' state
        '''
        # parameters checking
        if not (extraction_type=='Full-AlmostFull' or extraction_type=='Empty-AlmostEmpty' or extraction_type == 'Full-Decrease' or extraction_type == 'Empty-Increase' or extraction_type == 'Full-Increase'):
            raise(NameError('Wrong extraction_type name'))        
        if not (neighborhood_type=='distance' or neighborhood_type=='indegree'):
            raise(NameError('Wrong neighborhood_type name'))
        
        # cache neighbors dictionary
        if neighborhood_type=='indegree':
            edge_importance_df = self.sc.broadcast(pd.read_csv(importance_path))
            
        interval = self.interval
        window_size = self.window_size
        maxDelta = self.maxDelta
        th = self.th
        # BEWARE: '2013-08-29 12:06' IS THE BASE TIME FOR OUR PARTICULAR DATASET
        base_time = int(time.mktime(datetime(2013, 8, 29, 12, 6).timetuple())/(interval*60))
        
        # load data
        inputDF = self.spark.read.format("csv").option("delimiter", ",").option("header", True).option("inferSchema", True).load(self.status_path)

        # filter inconsistent data
        inputDF = inputDF.filter("bikes_available is not null")
        filteredDF = inputDF.filter("docks_available<>0 OR bikes_available<>0")

        if extraction_type == 'Full-AlmostFull' or extraction_type == 'Empty-AlmostEmpty':
            if extraction_type == 'Full-AlmostFull':
                
                def stateFunctionF(docks_available, bikes_available):
                    if docks_available==0 and not wrap_states:
                        return 1
                    elif (docks_available==0 or docks_available==1 or docks_available==2):
                        return 0
                    else:
                        return 2
    
                self.spark.udf.register("state", stateFunctionF)
            else:# extraction_type == 'Empty-AlmostEmpty'
                
                def stateFunctionE(docks_available,bikes_available):
                    if bikes_available==0 and not wrap_states:
                        return 1
                    elif (bikes_available==0 or bikes_available==1 or bikes_available==2):
                        return 0
                    else:
                        return 2
                self.spark.udf.register("state", stateFunctionE)

            getStatusDF = filteredDF.selectExpr("station_id", "time", "state(docks_available, bikes_available) as status")

            # filter only full or almost full stations
            if not for_test:
                recordsDF = getStatusDF.filter("status==1  or status==0")

            # map to Unix time and cluster into interval:
            # to have small numbers, the min(time) of the dataset (already calculated) is subtracted in each window
            def mapToUnixTime(line):
                timestamp = line[1]
                unixtime = time.mktime(timestamp.timetuple())

                return line[0], int(unixtime/(interval*60)) - base_time, int(unixtime), line[2]

            if not for_test:
                unixRecords = recordsDF.rdd.map(tuple).map(mapToUnixTime)
            else:
                unixRecords = getStatusDF.rdd.map(tuple).map(mapToUnixTime)
            
        else:# extraction_type == 'Full-Decrease' or extraction_type == 'Empty-Increase'or extraction_type == 'Full-Increase'
            # map (ID, bikes, docks, time) -> ((ID, window), (time, docks, bikes))
            def mapIdWindow_TimeStats(line):
                stationId = line[0]
                timestamp = line[3]
                docks = line[2]
                bikes = line[1]

                unixtime = time.mktime(timestamp.timetuple())
                window = int(unixtime/(interval*60)) - base_time

                return ((stationId, window), (int(unixtime), int(docks), int(bikes)))
            
            station_windowRdd = filteredDF.rdd.map(mapIdWindow_TimeStats)
            groupedSWRdd = station_windowRdd.groupByKey()
            
            # flatMap ((ID, window), (time, docks, bikes)) -> [(ID, window, state),...]
            def increase_decrease_state_mapper(line):
                window_ids = []
                stats = []
                hasCritical = False
                hasAlmostCritical = False
                hasIncrease = False
                hasDecrease = False

                # sort states
                for el in line[1]:
                    window_ids.append(el[0])
                    stats.append((el[1], el[2])) # (docks, bikes)
                
                window_states = []
                
                # find if we have an increase/decrease above threshold
                for i, idx in enumerate(np.argsort(window_ids)):
                    if i == 0:
                        minval = stats[idx][1] # current number of bikes available
                        maxval = stats[idx][1]
                        
                        if state_change:
                            if extraction_type.split("-")[0] == "Empty":
                                if stats[idx][1] < 3: # current number of bikes available
                                    hasAlmostCritical = True
                                if stats[idx][1] == 0:
                                    hasCritical = True
                            else: # extraction_type.split("-")[0] == "Full"
                                if stats[idx][0] < 3: # current number of docks available
                                    hasAlmostCritical = True
                                if stats[idx][0] == 0:
                                    hasCritical = True

                    else:
                        # update variables
                        if stats[idx][1] < minval:
                            minval = stats[idx][1]
                        if stats[idx][1] > maxval:
                            maxval = stats[idx][1]
                        
                        # check increase/decrease
                        if (stats[idx][1] - minval) > incr_dec_threshold:
                            if not hasIncrease:
                                hasIncrease = True
                                window_states.append((line[0][0], line[0][1], 'Increase'))
                        if (maxval - stats[idx][1]) > incr_dec_threshold:
                            if not hasDecrease:
                                hasDecrease = True
                                window_states.append((line[0][0], line[0][1], 'Decrease'))
                        
                        # check critical/almost state
                        if extraction_type.split("-")[0] == "Empty":
                            if (not hasCritical) and stats[idx][1] == 0 and (not wrap_states):
                                hasCritical = True
                                window_states.append((line[0][0], line[0][1], extraction_type.split("-")[0]))
                            elif (not hasAlmostCritical) and stats[idx][1] < 3:
                                hasAlmostCritical = True
                                window_states.append((line[0][0], line[0][1], f'Almost{extraction_type.split("-")[0]}'))
                        
                        else: # extraction_type.split("-")[0] == "Full"
                            if (not hasCritical) and stats[idx][0] == 0 and (not wrap_states):
                                hasCritical = True
                                window_states.append((line[0][0], line[0][1], extraction_type.split("-")[0]))
                            elif (not hasAlmostCritical) and stats[idx][0] < 3:
                                hasAlmostCritical = True
                                window_states.append((line[0][0], line[0][1], f'Almost{extraction_type.split("-")[0]}'))
                
                if not hasAlmostCritical and for_test:
                    window_states.append((line[0][0], line[0][1], 'Normal'))
                
                return window_states

            unixRecords = groupedSWRdd.flatMap(increase_decrease_state_mapper)

        # get distinct stations to calculate distances
        id_stations = unixRecords.map(lambda line: line[0]).distinct()

        tot_id_stations = id_stations.collect() # all distinct station ids
        
        # obtain timestamp and info
        if extraction_type == 'Full-AlmostFull':
            get_map = unixRecords.map(getMapF).distinct()
        elif extraction_type == 'Empty-AlmostEmpty':
            get_map = unixRecords.map(getMapE).distinct()
        else:
            get_map = unixRecords.map(lambda l: (l[1], f"{l[0]}_{l[2]}"))

        
        # for each timestamp obtain info
        reduceK = get_map.reduceByKey(lambda l1, l2 :(l1+','+l2)).sortByKey()
        
        #obtain window, station-status
        def giveSplit(line):   
            id_window = (int(line[0]))
            lista = []
            counter = id_window    
            while counter >= 0:
                lista.append(('Window '+ str(counter), (id_window, (line[1]))))
                counter = counter-1
                if(id_window-counter)==window_size:
                    return lista  
            return lista
        mapData = reduceK.flatMap(giveSplit)

        # for each window get all info
        all_keys = mapData.groupByKey().mapValues(ordered_state_mapper)

        #finestra temporale
        windows = all_keys.flatMap(reduceKeys)
        
        dict_distances = self.get_station_info(tot_id_stations)
        
        #Apply “Spatial Delta”
        def giveSpatialWindow(line):            
            lista=[]    
            time0=line[1][0]

            count_windows=len(line[1])#tot windows

            for station in time0:# only first window
                current_station=int(station.split('_')[0])
                
                list_tmp=[]
                topX_neighborhood = []
                if neighborhood_type=='indegree':
                    if extraction_type=='Full-AlmostFull' or extraction_type=='Full-Decrease'or extraction_type == 'Full-Increase':
                        topX_neighborhood = edge_importance_df.value[edge_importance_df.value['end_id']==current_station][:n_neighbors]['start_id'].values
                    else: # extraction_type=='Empty-AlmostEmpty' or extraction_type=='Empty-Decrease'
                        topX_neighborhood = edge_importance_df.value[edge_importance_df.value['start_id']==current_station]\
                        .sort_values(['start_id','count'], ascending=[True, False])[:n_neighbors]['end_id'].values

                #for each window
                for i, window in enumerate(line[1]):           
                    second_lista=[]
                    second_lista_items = set()
                    #for each element in a window
                    for item in window:
                        second_station = int(item.split('_')[0])
                        state = item.split('_')[2]
                        
                        # we are not interested in 'Decrease' state if extraction_type == 'Full-Increase'
                        if extraction_type == 'Full-Increase' and state == 'Decrease':
                            continue

                        if current_station!=second_station:
                            # we are interested in only one state if 
                            # extraction_type == 'Full-Decrease' or extraction_type == 'Empty-Increase'
                            if (extraction_type == 'Full-Decrease' or extraction_type == 'Empty-Increase') and state != extraction_type.split('-')[1]:
                                continue
                            
                            # check that station is needed or not if neighborhood_type=='indegree'
                            if (not second_station in topX_neighborhood and neighborhood_type=='indegree'):
                                continue
                            if current_station<second_station:
                                key=str(current_station)+' '+str(second_station)
                            else:
                                key=str(second_station)+' '+str(current_station)                    

                            dist=dict_distances[key]
                            if (dist<=maxDelta*th) or (neighborhood_type=='indegree'):
                                d = 1
                                while d*th < dist:
                                    d += 1
                                delta = d

                                label=state+'_'+'T'+str(i)+'_'+str(delta)
                                if label not in second_lista_items:
                                    second_lista.append(label)
                                    second_lista_items.add(label)
                        else:
                            # for the "main" station
                            # we are not interested in 'Decrease' state if extraction_type == 'Full-Decrease'
                            # we are not interested in 'Increase' state if extraction_type == 'Empty-Increase'
                            if  (extraction_type == 'Full-Decrease' or extraction_type == 'Empty-Increase') and state == extraction_type.split('-')[1]:
                                continue
                            label=state+'_'+'T'+str(i)+'_'+str(0)
                            if label not in second_lista_items:
                                second_lista.append(label)
                                second_lista_items.add(label)

                    if len(second_lista)>0:
                        list_tmp.append(second_lista)
                lista.append(((line[0]+'|'+str(current_station)),list_tmp))

            return lista
        
        spatial_app = windows.flatMap(giveSpatialWindow)
        spatial = spatial_app.map(row_seq)
        
        # clear cached file
        if neighborhood_type=='indegree':
            edge_importance_df.unpersist()

        return spatial.toDF()

    
    def extract_frequent_items(self, df, support, mpl=5, mlpdbs=5000):
        '''
        Obtain sequence and frequence of input patterns
        returns a DataFrame
        '''
        prefixSpan = PrefixSpan(minSupport=support, maxPatternLength=mpl, maxLocalProjDBSize=mlpdbs)
        prefix = prefixSpan.findFrequentSequentialPatterns(df)   

        return prefix

    
    def save_stats(self, prefix, support, extraction_type, root="../../Results/Extraction/Undefined/Undefined_"):
        '''
        Save and print some statistics about extracted patterns
        '''
        extr_type_critical = extraction_type.split('-')[0]
        extr_type_almostCritical = extraction_type.split('-')[1]
        #output files
        folder_path = f'{root}{self.interval}_{int(self.th*1000)}_{support}({self.window_size}-{self.maxDelta})'

        output_file = f'{folder_path}/results_{int(self.th*1000)}_{support}_ordered_by_confidence.txt'
        output_file2 = f'{folder_path}/results_{int(self.th*1000)}_{support}_{extraction_type}_ordered_by_confidence.txt'
        output_file3 = f'{folder_path}/results_{int(self.th*1000)}_{support}_diff_delta_ordered_by_confidence.txt'
        output_file4 = f'{folder_path}/results_{int(self.th*1000)}_{support}_ordered_by_support.txt'
        output_file5 = f'{folder_path}/results_{int(self.th*1000)}_{support}_{extraction_type}_ordered_by_support.txt'
        output_file6 = f'{folder_path}/results_{int(self.th*1000)}_{support}_diff_delta_ordered_by_support.txt'
        img_support = f'{folder_path}/{self.window_size}-{self.maxDelta}-{int(self.th*1000)}-{support}-{self.interval}.jpg'

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        pre = prefix.rdd.map(tuple)

        giveT0 = pre.filter(giveSelected)        
        mapDict = giveT0.map(mapValues)

        li = mapDict.collect()
        dic = {}
        for el in li:
            splits = el.split(';')
            dic[splits[0]] = int(splits[1])

        repeated_el_window=0
        for el in dic.keys():
            flag_rep=False
            windows=el.split('-')
            for w in windows:
                tot_items=len(w.split(','))
                set_items=len(set(w.split(',')))
                if tot_items!=set_items:
                    repeated_el_window+=1
                    break

        dic_supports={}
        for el in dic.keys():    
            if len(el.split('-'))>1:        
                num=int(dic[el])       
                string=''
                tot=el.split('-')[:-1]
                for k,station in enumerate(tot):
                    if k>0:
                        string+='-'
                    string+=station
                #print(string)
                den=int(dic[string])
                dic_supports[el]=str(num/den)+' - '+str(dic[el])
        keys = list(dic_supports.keys())
        values = list(dic_supports.values())

        #sort dictionary by decreasing values and sort within each window
        for key in dic_supports:    
            splitted=key.split('-')      
            splitted.sort()
        # dic_supports = dict(sorted(dic_supports.items(), key=operator.itemgetter(1),reverse=True))
        dic_supports = dict(sorted(dic_supports.items(), 
                                   key=lambda v: (float(v[1].split(' - ')[0]), int(v[1].split(' - ')[1])),
                                   reverse=True))

        # output_file='results_..._ordered_by_confidence.txt'
        file = open(output_file, "w")
        list_pattern=[]
        for el in dic_supports:
            key_list=[]
            for e in el.split('-'):
                key_list.append([e])
            list_pattern.append([[key_list], [dic_supports[el]]])
        file.write('Pattern, Confidence-Frequence'+'\n')
        file.write(f'Total number of input patterns: {len(dic_supports)}'+'\n')
        for el in list_pattern:  
            file.write(str(el)+ '\n')    
        file.close()

        # output_file4='results_..._ordered_by_support.txt'
        file4 = open(output_file4, "w")
        #order list fist by support and then by confidence
        list_ordered_by_support = sorted(list_pattern,
                                         key=lambda v: (int(v[1][0].split(" - ")[1]), float(v[1][0].split(" - ")[0])),
                                         reverse=True)
        file4.write('Pattern, Confidence-Frequence'+'\n')
        file4.write(f'Total number of input patterns: {len(dic_supports)}'+'\n')
        for el in list_ordered_by_support:  
            file4.write(str(el)+ '\n')    
        file4.close()

        tot_list=[]
        for i, j in zip(keys,values):
            key_list=[]
            for el in i.split('-'):
                key_list.append([el])
            tot_list.append([[key_list], [j]])
        confidence=[]
        for el in values:
            val=round(float(el.split(' - ')[0]),2)
            confidence.append(val)
        #confidence    
        plt.hist(confidence)
        plt.xlabel('Confidence')
        plt.ylabel('Number of patterns')
        plt.xlim(0.3,1)
        plt.savefig(img_support)
        plt.close()

        #how many min, max, avg items there are
        num_items=0
        num_freq_items=0
        min_items=sys.maxsize
        max_items=0
        avg_items=0

        num_freq_window=0
        tot_window=0
        num_patterns=0
        min_window=sys.maxsize
        max_window=0
        avg_window=0
        flag_piena=False
        flag_quasi_piena=False

        list_piena_quasi_piena=[]

        for key in dic_supports.keys():
            flag_piena=False
            flag_quasi_piena=False    
            string=''

            #statistics for the windows
            if '-' in key:
                patterns=len(key.split('-'))
                tot_window+=patterns      
            else:
                tot_window+=1
                patterns=1
            num_patterns+=1

            #statistics for the ìtems
            if ('-' in key and ',' in key):
                new_key=key       
                new_key=new_key.replace(',', '-')               
                items=new_key.split('-')
                for el in items:
                    string+=el.split('_')[0]
                    if el.split('_')[0].startswith(extr_type_critical) and flag_piena==False:
                        flag_piena=True
                    elif el.split('_')[0].startswith(extr_type_almostCritical) and flag_quasi_piena==False:
                        flag_quasi_piena=True


            elif ('-' in key):
                items=key.split('-')
                for el in items:
                    string+=el.split('_')[0]
                    if el.split('_')[0].startswith(extr_type_critical) and flag_piena==False:
                        flag_piena=True
                    elif el.split('_')[0].startswith(extr_type_almostCritical) and flag_quasi_piena==False:
                        flag_quasi_piena=True

            elif ( ',' in key):
                items=key.split(',')
                for el in items:
                    string+=el.split('_')[0]
                    if el.split('_')[0].startswith(extr_type_critical) and flag_piena==False:
                        flag_piena=True
                    elif el.split('_')[0].startswith(extr_type_almostCritical) and flag_quasi_piena==False:
                        flag_quasi_piena=True

            else:
                items=list(key)
                for el in items:
                    string+=el.split('_')[0]
                    if el.split('_')[0].startswith(extr_type_critical) and flag_piena==False:
                        flag_piena=True
                    elif el.split('_')[0].startswith(extr_type_almostCritical) and flag_quasi_piena==False:
                        flag_quasi_piena=True
            if (flag_quasi_piena==True and flag_piena==True):       
                #list_piena_quasi_piena.append([key])
                key_list=[]            
                for e in key.split('-'):
                    key_list.append([e])
                list_piena_quasi_piena.append([[key_list], [dic_supports[key]]])

            q_items=len(items) 
            if min_items>q_items:
                min_items=q_items
            if max_items<q_items:
                max_items=q_items
            freq=int(dic_supports[key].split('-')[1])
            num_items+=freq 
            num_freq_items+=freq*q_items

            if min_window> patterns:
                min_window=patterns
            if max_window<patterns:
                max_window=patterns    
            num_freq_window+=freq*patterns  

        avg_window=0
        if num_items!=0:
            avg_items=float(num_freq_items)/float(num_items)    
            print(f'The average number of items is: {avg_items}')    
            avg_window=float(num_freq_window)/ float(num_items)
            print(f'The average number of windows is: {avg_window}')
        else:
            print(f'The average number of windows is: {avg_window}')

        print(f'The minimum number of items is: {min_items}')
        print(f'The maximum number of items is: {max_items}')
        print(f'The minimum number of windows is: {min_window}')
        print(f'The maximum number of windows is: {max_window}')

        # output_file2='results_..._QuasiPiena_Piena_ordered_by_confidence.txt'
        file2 = open(output_file2, "w")
        # lung_piena_quasipiena=0
        if len(list_piena_quasi_piena)!=0:
            df_supports=self.sc.parallelize(list_piena_quasi_piena).toDF().withColumnRenamed('_1','sequence')
            df_supports=df_supports.withColumnRenamed('_2','confidence-freq')
            lung_piena_quasipiena=df_supports.count() 
        #     df_supports.show(lung_piena_quasipiena,False)
        #     print(lung_piena_quasipiena)
        #list_piena_quasi_piena
        file2.write('Pattern, Confidence-Frequence'+'\n')
        file2.write(f'Total number of input patterns: {len(list_piena_quasi_piena)}'+'\n')
        if len(list_piena_quasi_piena)!=0:
            for el in list_piena_quasi_piena:
                file2.write(str(el)+'\n')
        file2.close()

        # output_file5='results_..._QuasiPiena_Piena_ordered_by_support.txt'
        file5 = open(output_file5, "w")
        #order list fist by support an then by confidence
        list_piena_quasi_piena_ordered_by_support = sorted(list_piena_quasi_piena, key=lambda v: (int(v[1][0].split(" - ")[1]), float(v[1][0].split(" - ")[0])), reverse=True)
        file5.write('Pattern, Confidence-Frequence'+'\n')
        file5.write(f'Total number of input patterns: {len(list_piena_quasi_piena_ordered_by_support)}'+'\n')
        if len(list_piena_quasi_piena_ordered_by_support)!=0:
            for el in list_piena_quasi_piena_ordered_by_support:
                file5.write(str(el)+'\n')
        file5.close()

        list_influenze=[]
        for el in dic_supports.keys():
            delta_spaziale=False
            if ('-' in el):
                all_windows_list=el.split('-')
                if ('T0_0' in all_windows_list[0] ):
                    for cons_window in all_windows_list[1::]:
                        if ',' in cons_window:
                            for item in cons_window.split(','):                       
                                act_delta=int(item.split('_')[2])
                                if act_delta!=0:
                                    delta_spaziale=True
            if delta_spaziale==True:        
                key_list=[]            
                for e in el.split('-'):
                    key_list.append([e])
                list_influenze.append([[key_list], [dic_supports[el]]])
        # output_file3='results_..._diff_delta_ordered_by_confidence.txt'
        file3 = open(output_file3, "w")
        lung_different_time_space=0
        if len(list_influenze)!=0:
            df_supports=self.sc.parallelize(list_influenze).toDF().withColumnRenamed('_1','sequence')
            df_supports=df_supports.withColumnRenamed('_2','confidence-freq')
            lung_different_time_space=df_supports.count() 
            #df_supports.show(lung_different_time_space,False)
            #print(lung_different_time_space)
        file3.write('Pattern, Confidence-Frequence'+'\n')
        file3.write(f'Total number of input patterns: {len(list_influenze)}'+'\n')
        if len(list_influenze)!=0:
            for el in list_influenze:       
                file3.write(str(el)+'\n')
        file3.close()

        # output_file6='results_..._diff_delta_ordered_by_support.txt'
        file6 = open(output_file6, "w")
        #order list first by support and then by confidence
        list_influenze_ordered_by_support = sorted(list_influenze, key=lambda v: (int(v[1][0].split(" - ")[1]), float(v[1][0].split(" - ")[0])), reverse=True)
        file6.write('Pattern, Confidence-Frequence'+'\n')
        file6.write(f'Total number of input patterns: {len(list_influenze_ordered_by_support)}'+'\n')
        if len(list_influenze_ordered_by_support)!=0:
            for el in list_influenze_ordered_by_support:       
                file6.write(str(el)+'\n')
        file6.close()
        
        len_prefix = prefix.count()
        
        lung_piena_quasipiena=0
        if len(list_piena_quasi_piena)!=0:
            df_supports=self.sc.parallelize(list_piena_quasi_piena).toDF().withColumnRenamed('_1','sequence')
            df_supports=df_supports.withColumnRenamed('_2','confidence-freq')
            lung_piena_quasipiena=df_supports.count() 
            
        print(f'The number of patterns in the pre-filter is: {len_prefix}')
        print(f'The number of items after the filter with at least 2 windows and at least a T0 and delta 0 is: {len(dic_supports)}')
        print('STATISTICS about sequences with at least 2 windows_T0_delta0')
        print(f'The average number of windows is: {avg_window}')
        print(f'The minimum number of windows is: {min_window}')
        print(f'The maximum number of windows is: {max_window}')
        print(f'The average number of items is: {avg_items}')
        print(f'The minimum number of items is: {min_items}')
        print(f'The maximum number of items is: {max_items}')
        print(f'The number of patterns in which there is at least one item that repeats within a window is: {repeated_el_window} ')
        print(f'The number of patterns with at least 1 event {extr_type_almostCritical} and 1 event {extr_type_critical} is: {lung_piena_quasipiena}')
        print(f'The number of patterns with at least 1 T0, DELTA S=0 and at least 1 pattern with at least 1 pattern with DELTA S different from 0 and DELTA T different from 0 is: {lung_different_time_space}')
        

    def save_classification_patterns(self, prefix, output_file):
        '''
        Save patterns for calssification
        '''
        pre = prefix.rdd.map(tuple)
        
        # filter patterns that contain station 0
        giveT0 = pre.filter(giveSelected)
        
        mapDict = giveT0.map(mapValues)
        
        li = mapDict.collect()
        voc={}
        
        for el in li:
            splits=el.split(';')
            voc[splits[0]]=int(splits[1])
            
        repeated_el_window=0
        for el in voc.keys():
            flag_rep=False
            windows=el.split('-')
            for w in windows:
                tot_items=len(w.split(','))
                set_items=len(set(w.split(',')))
                if tot_items!=set_items:
                    repeated_el_window+=1
                    break
                    
        voc_supports={}
        for el in voc.keys():    
            if len(el.split('-'))>1:        
                num=int(voc[el])       
                string=''
                tot=el.split('-')[:-1]
                for k,station in enumerate(tot):
                    if k>0:
                        string+='-'
                    string+=station
                #print(string)
                den=int(voc[string])
                voc_supports[el]=str(num/den)+' - '+str(voc[el])
        keys=list(voc_supports.keys())
        values=list(voc_supports.values())
        
        #sort vocabulary by decreasing values and sort within each window
        tot_frequence=0
        for key in voc_supports:    
            splitted=key.split('-')      
            splitted.sort()
            tmp_frequence=int(voc_supports[key].split(' - ')[1])
            tot_frequence+=tmp_frequence
        for key in voc_supports:
            freq= int(voc_supports[key].split(' - ')[1])
            voc_supports[key]=str(voc_supports[key].split(' - ')[0])+' - ' + str(freq)#+' - '+str(perc_value)+'%'  
        
        voc_supports = dict(sorted(voc_supports.items(), 
                                   key=lambda v: (float(v[1].split(' - ')[0]), int(v[1].split(' - ')[1])),
                                   reverse=True))
        
        file = open(output_file, "w")
        list_pattern=[]
        
        for el in voc_supports:
            key_list=[]
            for e in el.split('-'):
                key_list.append([e])
            list_pattern.append([[key_list], [voc_supports[el]]])
            
        file.write('Pattern, Confidence-Frequence'+'\n')
        file.write(f'Total number of input patterns: {len(voc_supports)}'+'\n')
        for el in list_pattern:  
            file.write(str(el)+ '\n')    
        file.close() 
        print(output_file, " saved successfully.")
        
        
    def filter_patterns_conf_sup(self, patterns_path, conf_threshold, sup_threshold, target='AlmostFull'):
        '''
        Filter input patterns by confidence and support
        patterns_path: file containng patterns
        conf_threshold: max support threshold
        sup_threshold: min support threshold
        target: indicate desired critical state
        returns an RDD
        '''
        #read file
        patterns = self.sc.textFile(patterns_path)

        #remove file headers 
        patterns = patterns.zipWithIndex().filter(lambda kv: kv[1] > 1).keys()

        #filter patterns containing in the last element only elements with "_0" and {target} state
        def filtering(line):
            s_list = ast.literal_eval(line)
            last = s_list[0][0][-1]
            list_of_last = last[0].split(',')
            for el in list_of_last:
                if "_0" not in el or target not in el:
                    return False
            return True

        filtered_patterns = patterns.filter(filtering)

        def conf_sup_filtering(line):
            s_list = ast.literal_eval(line)
            conf_sup = s_list[1][0]
            conf = float(conf_sup.split(' - ')[0])
            sup = int(conf_sup.split(' - ')[1])
            if conf >= conf_threshold and sup >= sup_threshold:
                return True
            else:
                return False

        filtered_patterns = filtered_patterns.filter(conf_sup_filtering)

        if filtered_patterns.isEmpty():
            print("All patterns were filtered.\nPlease choose looser rules.")

        return filtered_patterns


    def print_patterns_stats(self, filtered_patterns):
        '''
        Print some statistics about the filtered patterns
        '''
        #extract confidences list
        def extract_confidences(line):
            s_list = ast.literal_eval(line)
            confidence = s_list[1][0].split(' - ')[0]
            return float(confidence)

        confidences_list = filtered_patterns.map(extract_confidences).collect()

        #extract suppprts list
        def extract_supports(line):
            s_list = ast.literal_eval(line)
            support = s_list[1][0].split(' - ')[1]
            return int(support)

        supports_list = filtered_patterns.map(extract_supports).collect()

        # support-confidence plot
        plt.scatter(supports_list, confidences_list)
        plt.xlabel('support') 
        plt.ylabel('confidence')
        plt.title('Plot img_support_confidence')
        plt.show()

        # plot the number of pattern with certaion confidence
        plt.title('Plot img_confidence')
        plt.hist(confidences_list)
        plt.xlabel('Confidence')
        plt.ylabel('Number of patterns')
        plt.xlim(0,1)
        plt.show()

        # plot number of patterns >= conf_threshold    
        conf_trhesholds = [0.1, 0.2, 0.3, 0.40,0.50,0.60,0.70,0.80,0.90,0.100]
        bincenters = (np.array(conf_trhesholds)[1:] + conf_trhesholds[:-1])/2
        binwidths = np.diff(conf_trhesholds)
        binvals = [np.sum(np.array(confidences_list)>=thresh) for thresh in conf_trhesholds[:-1]]
        fig, ax = plt.subplots()
        plt.title( f'Plot num patterns >= conf_trashold')
        ax.bar(bincenters, binvals, width=binwidths, alpha=0.4,
               edgecolor=['darkblue'])
        ax.set_xlabel('conf_threshold')
        ax.set_ylabel('occurences')
        ax.autoscale('x', tight=True)
        plt.show()
        

    def logga(self, test_items, rules, match_threshold=1):
        '''
        Log matches
        '''
        br_rules = self.sc.broadcast(rules)

        match_count = 0
        # perform the prediction:
        # map (item) -> (y_pred, y_true)
        # rule structure: [patterns, tag, conf-sup]
        # line structure: [[patterns, tag]]
        def predict_item(l):
            line = l[0]
            
            for rule in br_rules.value:
                rule_timeslots = rule.split('], ')
                
                rule_item_offset = len(rule_timeslots[:-2]) - len(line[:-1])
                # rule cannot match if it is longer than the event
                if rule_item_offset > 0:
                    continue

                if all_rule_items_match(rule_timeslots[:-2], line[-rule_item_offset:-1]):
                    match_count += 1
                    
                if match_count >= match_threshold:
                    return (rule_timeslots[:-2], line[-rule_item_offset:-1])
            
        
        predictions = test_items.rdd.filter(lambda t: len(t[0]) > 1).map(predict_item)
        
        return predictions
        

    def test_rules(self, test_items, rules, match_threshold=1):
        '''
        Test filtered associative rules.
        test_items: RDD with all items to test
        rules: list of rules to apply
        match_threshold: number of rules to match in order to make a positive prediction
        '''
        
        br_rules = self.sc.broadcast(rules)

        # perform the prediction:
        # map (item) -> (y_pred, y_true)
        # rule structure: [patterns, tag, conf-sup]
        # line structure: [[patterns, tag]]
        def predict_item(l):
            line = l[0]
            y_true = 'Normal'
            match_count = 0
    
            if re.search(r'AlmostFull_T._0', str(line[-1])):
                y_true = 'AlmostFull'
            
            y_pred = 'Normal'
            
            for rule in br_rules.value:
                rule_timeslots = rule.split('], ')
                
                rule_item_offset = len(rule_timeslots[:-2]) - len(line[:-1])
                # rule cannot match if it is longer than the event
                if rule_item_offset > 0:
                    continue

                if all_rule_items_match(rule_timeslots[:-2], line[-rule_item_offset:-1]):
                    match_count += 1
                    
                if match_count >= match_threshold:
                    y_pred = 'AlmostFull'
                    break
            
            return (y_pred, y_true)
        
        predictions = test_items.rdd.filter(lambda t: len(t[0]) > 1).map(predict_item)
        
        br_rules.unpersist()
        
        # map (y_pred, y_true) -> (TP|TN|FP|FN, 1)
        def map_confusion_data(line):
            if line[0] == line[1]:
                if line[0]=='AlmostFull':
                    return ('TP', 1)
                else:
                    return ('TN', 1)
            else:
                if line[0]=='AlmostFull':
                    return ('FP', 1)
                else:
                    return ('FN', 1)                
        
        confusion_data = predictions.map(map_confusion_data)
        confusion_values = confusion_data.reduceByKey(lambda x1, x2: int(x1)+int(x2))
        
        return confusion_values.collectAsMap()
        
