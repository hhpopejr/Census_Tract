# Census_Tract
Public Census and ACS Data

### get msa data ###

# get crosswalks from https://mcdc.missouri.edu/applications/geocorr2022.html#

cw1=pd.read_csv(r'geocorr2022_2300402904.csv') #tract to place - also has 2020 census data
cw1b=cw1.drop(axis=0, index=0) 

cw2=pd.read_csv(r'geocorr2022_2300404182.csv') #tract to msa
cw2b=cw2.drop(axis=0, index=0)

crosswalk=cw2b.merge(cw1b, on=['county','tract'], how='inner')

crosswalk=crosswalk.applymap(str)

crosswalk['CENSUS_TRACT']=crosswalk.tract.str.replace(".", "", 1)

crosswalk['CENSUS_TRACT']=crosswalk.county+crosswalk.CENSUS_TRACT

# merge crosswalk with econ data

dfcw=df.merge(crosswalk, left_on='GEO_TRACT', right_on='CENSUS_TRACT', how='left')
