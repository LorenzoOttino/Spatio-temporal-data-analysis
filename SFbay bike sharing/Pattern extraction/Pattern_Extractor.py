from pyspark.ml.fpm import PrefixSpan
import pyspark.sql.functions as F
import pandas as pd
import sys
import matplotlib.pyplot as plt
import os
import time
from datetime import datetime
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


    def extract_items(self, extraction_type, neighborhood_type='distance', n_neighbors=10, importance_path='../../Data/edge_importance.csv'):
        # parameters checking
        if not (extraction_type=='Full-AlmostFull' or extraction_type=='Empty-AlmostEmpty'):
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
        base_time = int(time.mktime(datetime(2013, 8, 29, 12, 6).timetuple())/(interval*60)) + 1 # windows will start from 1
        
        # load data
        inputDF = self.spark.read.format("csv").option("delimiter", ",").option("header", True).option("inferSchema", True).load(self.status_path)

        # filter inconsistent data
        inputDF = inputDF.filter("bikes_available is not null")
        filteredDF = inputDF.filter("docks_available<>0 OR bikes_available<>0")

        if extraction_type == 'Full-AlmostFull':
            self.spark.udf.register("state", stateFunctionF)
        else:# extraction_type == 'Empty-AlmostEmpty':
            self.spark.udf.register("state", stateFunctionE)
            
        getStatusDF = filteredDF.selectExpr("station_id", "time", "state(docks_available, bikes_available) as status")

        # filter only full or almost full stations
        full_almostFull = getStatusDF.filter("status==1  or status==0")
        full_almostFull.createOrReplaceTempView("readings")

        # select station, year, month, day, hour, minute, status ordered by time
        ss = self.spark.sql("""SELECT  station_id , YEAR(time) as year, MONTH(time) as month, DAY(time) as day, HOUR(time)as hour, MINUTE(time) as minute, status FROM readings GROUP BY station_id, year, month, day, hour, minute, status ORDER BY  station_id, year, month, day, hour, minute""")
        
        # create rdd and group into interval
        my_rdd = ss.rdd.map(tuple)
        
        # map to Unix time and cluster into interval:
        # to have small numbers, the min(time) of the dataset (already calculated) is subtracted in each window
        def mapToUnixTime(line):
            timestamp = datetime(line[1], line[2], line[3], line[4], line[5])
            unixtime = time.mktime(timestamp.timetuple())

            return line[0], int(unixtime/(interval*60)) - base_time, int(unixtime), line[6]
        
        rdd = my_rdd.map(mapToUnixTime)

        # get distinct stations to calculate distances
        id_stations = rdd.map(lambda line: line[0]).distinct()

        tot_id_stations = id_stations.collect() # all distinct station ids
        
        # obtain timestamp and info
        if extraction_type == 'Full-AlmostFull':
            get_map = rdd.map(getMapF).distinct()
        else:# extraction_type == 'Empty-AlmostEmpty':
            get_map = rdd.map(getMapE).distinct()

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
            dic={}

            count_windows=len(line[1])#tot windows

            for station in time0:# only first window
                current_station=int(station.split('_')[0])
                #lista_station=[]
                list_tmp=[]
                topX_neighborhood = []
                if neighborhood_type=='indegree':
                    if extraction_type=='Full-AlmostFull':
                        topX_neighborhood = edge_importance_df.value[edge_importance_df.value['end_id']==current_station][:n_neighbors]['start_id'].values
                    else: # extraction_type=='Empty-AlmostEmpty'
                        topX_neighborhood = edge_importance_df.value[edge_importance_df.value['start_id']==current_station]\
                        .sort_values(['start_id','count'], ascending=[True, False])[:n_neighbors]['end_id'].values

                #for each window
                for i, window in enumerate(line[1]):           
                    second_lista=[]
                    #for each element of a window
                    for item in window :
                        #second_lista=[]
                        second_station=int(item.split('_')[0])
                        state=item.split('_')[2]

                        if current_station!=second_station:
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
                                second_lista.append(label)
                        else:
                            label=state+'_'+'T'+str(i)+'_'+str(0)
                            second_lista.append(label)

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

    #obtain sequence and frequence
    def extract_frequent_items(self, df, support, mpl=5, mlpdbs=5000):
        prefixSpan = PrefixSpan(minSupport=support, maxPatternLength=mpl, maxLocalProjDBSize=mlpdbs)
        prefix = prefixSpan.findFrequentSequentialPatterns(df)   

        return prefix

    def save_stats(self, prefix, support, extraction_type, root="../../Results/Extraction/Undefined/Undefined_"):
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
        #plt.title(f'{self.maxDelta} threshold spaziale, {self.window_size} threshold temporale {self.th*1000} m e supporto {support}')
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
