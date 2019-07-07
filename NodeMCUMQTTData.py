import sys

def progressbar(it, prefix="", size=60, file=sys.stdout):
    count = len(it)
    def show(j):
        x = int(size*j/count)
        file.write("%s[%s%s] %i/%i\r" % (prefix, "#"*x, "."*(size-x), j, count))
        file.flush()        
    show(0)
    for i, item in enumerate(it):
        yield item
        show(i+1)
    file.write("\n")
    file.flush()

import time


# Above Function is used to create Progress bar




#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np


# # 2. Reading WiSenseData


#reading the Outdoor dataset
dff = pd.read_csv('NodeMCUMQTTData.csv' , header = None)


# # 3. Renaming the Columns

df = dff.rename(columns={0: 'timeStamp', 1: 'nodeName' , 2: 'BME280_Temp', 3: 'BME280_Hum' , 4: 'BME280_Pres' , 5 : 'DS18B20_Temp' , 6: 'LM35_Temp' , 7 : 'Battery_Vol' })


#Copying the original dataset ('df') into data1
data1 = df.copy()  



#Converting datatype of 'timeStamp' to datetime type
data1['timeStamp'] = pd.to_datetime(data1['timeStamp'])  


# Now We will create four new columns in our Dataset namely, 'BME280_Temp_changed', 'DS18B20_Temp_changed' , 'LM35_Temp_changed' and 'BME280_Pres_changed'
# These column will contain value '1' if changed else it will contain 0
data1['BME280_Temp_changed'] = 0
data1['DS18B20_Temp_changed'] = 0
data1['LM35_Temp_changed'] = 0
data1['BME280_Pres_changed'] = 0





#Checking Outliers
'''
# Following Scripts will deal with first value of each node if it is outlier

1. We'll just check if first value of each node for a particular column is outlier (i.e temperature > 100 or temperature < 0), if it is outlier then we'll change its value to next row value

'''


#from tqdm import tqdm_notebook

nodes = data1['nodeName'].unique() # this line will create an array having total unique nodes

print('Checking Outlier for BME280_Temp')
for n in progressbar(nodes, "Processing records for Outlier "):
#for n in tqdm_notebook(nodes , desc = 'Processing records for Outlier'):
    for i in range(data1.shape[0] - 1):
        if(data1.loc[i , 'nodeName'] == n):
            val0 = float(data1.loc[i,'BME280_Temp'])
            if(val0 < 0 or val0 > 100):
                data1.loc[i,'BME280_Temp'] = data1.loc[i+1,'BME280_Temp']
                print('Outlier Found at', i , 'for node' , n)
                break
            else:
                break

print('Checking Outlier for DS18B20_Temp')  
for n in progressbar(nodes, "Processing records for Outlier "):              
#for n in tqdm_notebook(nodes , desc = 'Processing records for Outlier'):
    for i in range(data1.shape[0] - 1):
        if(data1.loc[i , 'nodeName'] == n):
            val0 = float(data1.loc[i,'DS18B20_Temp'])
            if(val0 < 0 or val0 > 100):
                data1.loc[i,'DS18B20_Temp'] = data1.loc[i+1,'DS18B20_Temp']
                print('Outlier Found at',i, 'for node' , n)
                break
            else:
                break
                
                
print('Checking Outlier for LM35_Temp') 
for n in progressbar(nodes, "Processing records for Outlier "):               
#for n in tqdm_notebook(nodes , desc = 'Processing records for Outlier'):
    for i in range(data1.shape[0] - 1):
        if(data1.loc[i , 'nodeName'] == n):
            val0 = float(data1.loc[i,'LM35_Temp'])
            if(val0 < 0 or val0 > 100):
                data1.loc[i,'LM35_Temp'] = data1.loc[i+1,'LM35_Temp']
                print('Outlier Found at',i, 'for node' , n)
                break
            else:
                break
                
                
print('Checking Outlier for BME280_Pres')
for n in progressbar(nodes, "Processing records for Outlier "):
#for n in tqdm_notebook(nodes , desc = 'Processing records for Outlier'):
    for i in range(data1.shape[0] - 1):
        if(data1.loc[i , 'nodeName'] == n):
            val0 = float(data1.loc[i,'BME280_Pres'])
            if(val0 < 750 or val0 > 1000):
                data1.loc[i,'BME280_Pres'] = data1.loc[i+1,'BME280_Pres']
                print('Outlier Found at',i, 'for node' , n)
                break
            else:
                break

                






# # Following is the function to clean 'temperature3' and 'pressure'

# # Logic behind cleaning the data
# 
# Example: Cleaning temperature1
# 
# To clean 'temperature1', we'll iterate through this column and select two values(rows) of a particular node and compare it.
# 1. If there absolute difference is more than 10C and timeinterval is less than 30 minutes then we'll replace later value with previous one.
# 2. If later value(row) is showing an Outlier and time interval is more than 30 minutes then will just replace it with 'NaN'.
# 
# Same logic is Implemented for 'temperature3' and pressure.
# 


nodes = data1['nodeName'].unique() # this line will create an array having total unique nodes


#Function to clean 'BME280_Temp'

def BME280_Temp_clean(df):
    for n in progressbar(nodes, "Computing: "):
    #for n in nodes:
        k = 0
        for i in range(k , df.shape[0]-1):
          if(df.loc[i, 'nodeName'] == n):
            val0 = float(df.loc[i,'BME280_Temp'])
            time0 = (df.loc[i,'timeStamp' ])
            for j in range(i+1, df.shape[0]-1):
              if(df.loc[j, 'nodeName'] == n):
                val1 = float(df.loc[j , 'BME280_Temp'])
                time1 = (df.loc[j , 'timeStamp'])
                timedelta = time1 - time0
                minutes = timedelta.total_seconds() / 60
                
                if (abs(val1 - val0) > 10 and minutes < 30.0):
                  df.loc[j,'BME280_Temp'] = val0
                  df.loc[j, 'BME280_Temp_changed'] = 1
                  k = j
                  break
                elif(((val1) > 100 or (val1) < 0 ) and minutes > 30.0):
                  df.loc[j,'BME280_Temp'] = 'NaN'
                  k = j
                  break
                else:
                  k = j
                  break
                    
                    
#Function to clean 'DS18B20_Temp'

def DS18B20_Temp_clean(df):
    for n in progressbar(nodes, "Computing: "):
    #for n in nodes:
        k = 0
        for i in range(k , df.shape[0]-1):
          if(df.loc[i, 'nodeName'] == n):
            val0 = float(df.loc[i,'DS18B20_Temp'])
            time0 = (df.loc[i,'timeStamp' ])
            for j in range(i+1, df.shape[0]-1):
              if(df.loc[j, 'nodeName'] == n):
                val1 = float(df.loc[j , 'DS18B20_Temp'])
                time1 = (df.loc[j , 'timeStamp'])
                timedelta = time1 - time0
                minutes = timedelta.total_seconds() / 60
                
                if (abs(val1 - val0) > 10 and minutes < 30.0):
                  df.loc[j,'DS18B20_Temp'] = val0
                  df.loc[j, 'DS18B20_Temp_changed'] = 1
                  k = j
                  break
                elif(((val1) > 100 or (val1) < 0 ) and minutes > 30.0):
                  df.loc[j,'DS18B20_Temp'] = 'NaN'
                  k = j
                  break
                else:
                  k = j
                  break
                    
                    
#Function to clean 'LM35_Temp'

def LM35_Temp_clean(df):
    for n in progressbar(nodes, "Computing: "):
    #for n in nodes:
        k = 0
        for i in range(k , df.shape[0]-1):
          if(df.loc[i, 'nodeName'] == n):
            val0 = float(df.loc[i,'LM35_Temp'])
            time0 = (df.loc[i,'timeStamp' ])
            for j in range(i+1, df.shape[0]-1):
              if(df.loc[j, 'nodeName'] == n):
                val1 = float(df.loc[j , 'LM35_Temp'])
                time1 = (df.loc[j , 'timeStamp'])
                timedelta = time1 - time0
                minutes = timedelta.total_seconds() / 60
                
                if (abs(val1 - val0) > 10 and minutes < 30.0):
                  df.loc[j,'LM35_Temp'] = val0
                  df.loc[j, 'LM35_Temp_changed'] = 1
                  k = j
                  break
                elif(((val1) > 100 or (val1) < 0 ) and minutes > 30.0):
                  df.loc[j,'LM35_Temp'] = 'NaN'
                  k = j
                  break
                else:
                  k = j
                  break                    
                    
                    
                    
# Function to clean 'pressure'

def BME280_Pres_clean(df):
    for n in progressbar(nodes, "Computing: "):
    #for n in nodes:
        k = 0
        for i in range(k , df.shape[0]-1):
          if(df.loc[i, 'nodeName'] == n):
            val0 = float(df.loc[i,'BME280_Pres'])
            time0 = (df.loc[i,'timeStamp' ])
            for j in range(i+1, df.shape[0]-1):
              if(df.loc[j, 'nodeName'] == n):
                val1 = float(df.loc[j , 'BME280_Pres'])
                time1 = (df.loc[j , 'timeStamp'])
                timedelta = time1 - time0
                minutes = timedelta.total_seconds() / 60
                
                if (abs(val1 - val0) > 10 and minutes < 30.0):
                  df.loc[j,'BME280_Pres'] = val0
                  df.loc[j, 'BME280_Pres_changed'] = 1
                  k = j
                  break
                elif(((val1) > 1000 or (val1) < 750 ) and minutes > 30.0):
                  df.loc[j,'BME280_Pres'] = 'NaN'
                  k = j
                  break
                else:
                  k = j
                  break


# In[35]:


#Call Above functions to clean the dataset

print('Cleaning BME280_Temp')
BME280_Temp_clean(data1)

print('Cleaning DS18B20_Temp')
DS18B20_Temp_clean(data1)

print('Cleaning LM35_Temp')
LM35_Temp_clean(data1)

print('Cleaning BME280_Pres')
BME280_Pres_clean(data1)

data1['BME280_Temp'] = data1['BME280_Temp'].astype('float64')
data1['DS18B20_Temp'] = data1['DS18B20_Temp'].astype('float64')


data1.to_csv('kaish.csv')
