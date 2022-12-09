#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  6 20:21:17 2022

@author: harrypope
"""

# loading / prepping datasets for analysis

import numpy as np
import pandas as pd

## ATTENTION!!! - THINGS TO CHANGE BEFORE YOU RUN - IMPORTANT!!!!! ##

acs_avg= 5.4 # set the national unemployment average from the ACS
luas_avg= 5.3 # set the national unemployment average from the LUAS

# input data paths #

dp03path='/Users/powerfalker/Desktop/Geo/ACSDP5Y2020.DP03_2022-12-06T212554/ACSDP5Y2020.DP03-Data.csv'
dp05path='/Users/powerfalker/Desktop/Geo/ACSDP5Y2020.DP05_2022-12-06T213650/ACSDP5Y2020.DP05-Data.csv'
list1path='/Users/powerfalker/Desktop/Geo/list1_2020.xls'

# where is the spatial file

shp_path = "/Users/powerfalker/Desktop/Geo/cb_2021_us_tract_500k/cb_2021_us_tract_500k.shp"
adj_path= '/Users/powerfalker/Desktop/Geo/neighbor_list.csv'

## output files ##

dat_out='/Users/powerfalker/Desktop/Geo/Py_TEA_Out.csv'
adj_out='/Users/powerfalker/Desktop/Geo/Py_NULL_Out.csv'

# get econ data

dp03raw=pd.read_csv(dp03path)

cols_to_include={'GEO_ID','NAME','DP03_0003E','DP03_0005E'}
renaming = {'NAME':'NAME_TRACT'}

dp03 = dp03raw[cols_to_include].rename(renaming, axis=1)
dp03b = dp03.drop(axis=0, index=0)

# get pop data

dp05raw=pd.read_csv(dp05path)

cols_to_include={'GEO_ID','NAME','DP05_0001E'}
renaming = {'NAME':'NAME_COUNTY'}

dp05 = dp05raw[cols_to_include].rename(renaming, axis=1)
dp05b = dp05.drop(axis=0, index=0)

type(dp05.GEO_ID)

## MAKE NEW VARS ##

df=dp03b.merge(dp05b, on='GEO_ID', how='inner')

df = pd.DataFrame(df)

df['GEO_STATE']=df['GEO_ID'].str[9:11]
df['GEO_COUNTY']=df['GEO_ID'].str[9:14]
df['GEO_PLACE']=df['GEO_ID'].str[9:16]
df['GEO_TRACT']=df['GEO_ID'].str[9:20]

### get msa data ###

list1raw=pd.read_excel(list1path, skiprows=2)
cols_to_include={'Metropolitan/Micropolitan Statistical Area','FIPS State Code','FIPS County Code'}
renaming = {'Metropolitan/Micropolitan Statistical Area':'MSA','FIPS State Code':'FIPS_STATE','FIPS County Code':'FIPS_COUNTY'}

list1 = list1raw[cols_to_include].rename(renaming, axis=1)

list1 = list1.applymap(str)

list1.FIPS_STATE = list1.FIPS_STATE.astype(str).str.zfill(4)

list1.FIPS_COUNTY = list1.FIPS_COUNTY.astype(str).str.zfill(5)

list1.FIPS_STATE = list1.FIPS_STATE.str.rstrip('0')
list1.FIPS_COUNTY = list1.FIPS_COUNTY.str.rstrip('0')

list1.FIPS_STATE = list1.FIPS_STATE.str.rstrip('.')
list1.FIPS_COUNTY = list1.FIPS_COUNTY.str.rstrip('.')

list1.FIPS_COUNTY=list1.FIPS_STATE+list1.FIPS_COUNTY

msa=list1.rename({'FIPS_COUNTY':'GEO_COUNTY','FIPS_STATE':'GEO_STATE'},axis=1)

df2=df.merge(msa[{'GEO_COUNTY','MSA'}], on='GEO_COUNTY', how='outer')

df2.DP03_0005E=df2.DP03_0005E.fillna(0).astype(int)
df2.DP03_0003E=df2.DP03_0003E.fillna(0).astype(int)
df2.DP05_0001E=df2.DP05_0001E.fillna(0).astype(int)

df2['Rural']= np.where(df2.DP05_0001E >= 2000, 'YES', 'NO')
df2['Unemp_Rate']= round((df2.DP03_0005E/df2.DP03_0003E)*100,2)
df2['HUA_ACS']= np.where(df2.Unemp_Rate*1.5 >= acs_avg, 'YES', 'NO')
df2['HUA_LUAS']= np.where(df2.Unemp_Rate*1.5 >= luas_avg, 'YES', 'NO')
df2['MSA_Check']= np.where(df2.MSA == 'Metropolitan Statistical Area','YES','NO')

## get neighbors ##

from libpysal.weights import Queen
import geopandas as gpd
 
# read it into a geopandas geoDataFrame
adjdf = gpd.read_file(shp_path)
 
# use a named ID Variable (GEOID is the unique ID in my test file)
w_queen_id = Queen.from_dataframe(adjdf, idVariable='GEOID')
 
# run through some results and make a table!
# maybe cleaner ways to do this, but this was fast for me
rows = []
for key in w_queen_id.neighbors:
    for id in w_queen_id.neighbors[key]:
       rows.append([key, id])
 
# write the results to a csv
adj_dat = gpd.GeoDataFrame(rows, columns=["originID", "neighID"])
adj_dat.to_csv(adj_path)

## LOAD NEIGHBOR FILE ##

neighbor=adj_dat
renaming = {'NAME':'NAME_COUNTY'}
neighbor=neighbor[{'originID','neighID'}].rename({'originID':'OriginTract','neighID':'NeighborTract'},axis=1)

neighbor.OriginTract = neighbor.OriginTract.astype(str).str.zfill(11)
neighbor.NeighborTract = neighbor.NeighborTract.astype(str).str.zfill(11)

adj1=df2.merge(neighbor,left_on='GEO_TRACT', right_on='OriginTract', how='inner')
adj1=adj1[{'OriginTract','NeighborTract','Unemp_Rate'}].rename({'Unemp_Rate':'Origin_Rate'},axis=1)

adj2=df2.merge(adj1,left_on='GEO_TRACT', right_on='NeighborTract', how='inner')
adj2=adj2[{'OriginTract','NeighborTract','Origin_Rate','Unemp_Rate'}].rename({'Unemp_Rate':'Neighbor_Rate'},axis=1)

adj2['ADJ_HUA_ACS']= np.where(adj2.Neighbor_Rate >= acs_avg, 'YES', 'NO')
adj2['ADJ_HUA_LUAS']= np.where(adj2.Neighbor_Rate >= luas_avg, 'YES', 'NO')

## TEST ##

adj3y = adj2[(adj2['ADJ_HUA_ACS']=='YES') | (adj2['ADJ_HUA_LUAS']=='YES')]
adj3n = adj2[(adj2['ADJ_HUA_ACS']=='NO') & (adj2['ADJ_HUA_LUAS']=='NO')]

adj3yy=adj3y[{'OriginTract','ADJ_HUA_ACS','ADJ_HUA_LUAS'}]
adj_yes=adj3yy.drop_duplicates()

adj3nn=adj3n[{'OriginTract','ADJ_HUA_ACS','ADJ_HUA_LUAS'}]
adj_no=adj3nn.drop_duplicates()

adj_check=adj_yes.merge(adj_no, on='OriginTract', how='outer').fillna('NO')

adj_check['QUALIFIED_ADJ_HUA']=np.where( (adj_check.ADJ_HUA_ACS_x=='YES')|(adj_check.ADJ_HUA_LUAS_x=='YES'), 'YES', 'NO')

adj_check=adj_check[{'OriginTract','QUALIFIED_ADJ_HUA','ADJ_HUA_ACS_x','ADJ_HUA_LUAS_x'}].rename({'OriginTract':'GEO_TRACT','ADJ_HUA_ACS_x':'ADJ_HUA_ACS','ADJ_HUA_LUAS_x':'ADJ_HUA_LUAS'},axis=1)

## merge neighbor with main file ##

df3=df2.merge(adj_check, on='GEO_TRACT', how='left')

## finish up ##

df3['QUALIFIED_HUA']=np.where( (df3.HUA_ACS=='YES')|(df3.HUA_LUAS=='YES')|(df3.ADJ_HUA_ACS=='YES')|(df3.ADJ_HUA_LUAS=='YES'), 'YES', 'NO')

df3['QUALIFIED_RURAL']=np.where( (df3.MSA_Check=='NO')&(df3.Rural=='YES'), 'YES', 'NO')

neighbor_null=df3[(df3['QUALIFIED_ADJ_HUA'].isnull())]

neighbor_check=neighbor[(neighbor.OriginTract=='01003010600')]
adj2_check=adj2[(adj2.OriginTract=='01003010600')]
adj_no_check=adj_no[(adj_no.OriginTract=='01003010600')]
adj_check_check=adj_check[(adj_check.GEO_TRACT=='01003010600')]
df3_check=df3[(df3.GEO_TRACT=='01003010600')]

## OUTPUT DATA FILE ##

from pathlib import Path  
filepath = Path(dat_out)  
df3.to_csv(filepath)  

## OUTPUT NULL NEIGHBORS ##

from pathlib import Path  
filepath = Path(adj_out)  
neighbor_null.to_csv(filepath) 
