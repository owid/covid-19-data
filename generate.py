#!/usr/bin/env python
# coding: utf-8

# # Generate CSVs from COVID-19 WHO data

# ## Dependencies

# In[26]:


import pandas as pd
import os
from datetime import datetime

INPUT_PATH = 'input/'
OUTPUT_PATH = 'public/data/'

REGIONS_CSV_PATH = os.path.join(INPUT_PATH, 'regions.csv')
DEATHS_CSV_PATH = os.path.join(INPUT_PATH, 'deaths.csv')
CASES_CSV_PATH = os.path.join(INPUT_PATH, 'cases.csv')


# In[27]:


get_ipython().system('mkdir -p $OUTPUT_PATH')


# ## Load the data

# In[28]:


df_regions = pd.read_csv(REGIONS_CSV_PATH)


# Transform the "wide" format into "long" format, which is easier to work with.

# In[29]:


def melt_csv(df, var_name):
    return df.melt(
        id_vars=df.columns[0], 
        value_vars=df.columns[1:], 
        var_name='location', 
        value_name=var_name
    ).dropna()


# In[30]:


df_deaths = melt_csv(pd.read_csv(DEATHS_CSV_PATH, header=1).rename(columns={ 'Date': 'date' }), 'total_deaths')
df_cases = melt_csv(pd.read_csv(CASES_CSV_PATH, header=1).rename(columns={ 'Date': 'date' }), 'total_cases')


# Convert all numbers from floating point to integers:

# In[31]:


df_deaths['total_deaths'] = df_deaths['total_deaths'].astype('Int64')
df_cases['total_cases'] = df_cases['total_cases'].astype('Int64')


# ## Calculations

# Join cases & deaths into one dataframe

# In[32]:


df_merged = df_cases.merge(
    df_deaths, 
    how='outer', 
    left_on=['date', 'location'], 
    right_on=['date', 'location']
).sort_values(by=['location', 'date'])


# Standardize names to OWID names

# In[33]:


df_regions_merged = df_regions.merge(
    df_merged[['location']].drop_duplicates(),
    how="outer",
    left_on="WHO Country Name",
    right_on="location"
)


# In[34]:


assert(df_regions_merged['OWID Country Name'].isnull().any() == False)


# In[35]:


who_name_replace_map = { r['WHO Country Name']: r['OWID Country Name'] for r in df_regions_merged.to_dict('records') }


# In[36]:


df_merged['location'] = df_merged['location'].replace(who_name_replace_map)


# Calculate daily cases & deaths

# In[37]:


df_merged['new_cases'] = df_merged.groupby('location')['total_cases'].diff().astype('Int64')
df_merged['new_deaths'] = df_merged.groupby('location')['total_deaths'].diff().astype('Int64')


# Create a `Worldwide` aggregate

# In[38]:


df_global = df_merged.groupby('date').sum().reset_index()
df_global['location'] = 'Worldwide'


# In[39]:


df_merged = pd.concat([df_merged, df_global], sort=True)


# Calculate doubling rates

# In[40]:


def get_days_to_double(df, col_name):
    try:
        # verbose because being very careful not to modify original data with dates
        latest = df.loc[pd.to_datetime(df['date']).idxmax()]
        df_lt_half = df[df[col_name] <= (latest[col_name] / 2)]
        half = df_lt_half.loc[pd.to_datetime(df_lt_half['date']).idxmax()]
        return (pd.to_datetime(latest['date']) - pd.to_datetime(half['date'])).days
    except:
        return None


# In[41]:


days_to_double_cases = pd.DataFrame([
    (loc, get_days_to_double(df, 'total_cases')) 
    for loc, df in df_merged.groupby('location')
], columns=['location', 'days_to_double_cases'])
days_to_double_cases['days_to_double_cases'] = days_to_double_cases['days_to_double_cases'].astype('Int64')


# ## Inspect the results

# In[42]:


df_merged


# In[43]:


df_merged[df_merged['location'] == 'Worldwide']


# ## Write output files

# In[44]:


df_merged.to_csv(os.path.join(OUTPUT_PATH, 'full_data.csv'), index=False)


# In[45]:


for col_name in ['total_cases', 'total_deaths', 'new_cases', 'new_deaths']:
    df_pivot = df_merged.pivot(index='date', columns='location', values=col_name)
    # move Worldwide to first column
    cols = df_pivot.columns.tolist()
    cols.insert(0, cols.pop(cols.index('Worldwide')))
    df_pivot[cols].to_csv(os.path.join(OUTPUT_PATH, '%s.csv' % col_name))


# In[46]:


days_to_double_cases.to_csv(os.path.join(OUTPUT_PATH, 'days_to_double_cases.csv'), index=False)


# In[47]:


df_regions.to_csv(os.path.join(OUTPUT_PATH, 'regions.csv'), index=False)


# Create `grapher.csv`

# In[48]:


df_grapher = df_merged.copy()
df_grapher['date'] = pd.to_datetime(df_grapher['date']).map(lambda date: (date - datetime(2020, 1, 21)).days)


# In[49]:


df_grapher[['location', 'date', 'new_cases', 'new_deaths', 'total_cases', 'total_deaths']]     .rename(columns={
        'location': 'country',
        'date': 'year',
        'new_cases': 'Daily new confirmed cases of COVID-19',
        'new_deaths': 'Daily new confirmed deaths due to COVID-19',
        'total_cases': 'Total confirmed cases of COVID-19', 
        'total_deaths': 'Total confirmed deaths due to COVID-19'
    }) \
    .to_csv(os.path.join(OUTPUT_PATH, 'grapher.csv'), index=False)


# In[ ]:




