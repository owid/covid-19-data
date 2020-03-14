#!/usr/bin/env python
# coding: utf-8

# # Generate CSVs from COVID-19 WHO data

# ## Dependencies

# In[1]:


import pandas as pd
import os
from datetime import datetime

INPUT_PATH = 'input/'
OUTPUT_PATH = 'public/data/'

REGIONS_CSV_PATH = os.path.join(INPUT_PATH, 'regions.csv')
DEATHS_CSV_PATH = os.path.join(INPUT_PATH, 'deaths.csv')
CASES_CSV_PATH = os.path.join(INPUT_PATH, 'cases.csv')


# In[2]:


get_ipython().system('mkdir -p $OUTPUT_PATH')


# ## Load the data

# In[3]:


df_regions = pd.read_csv(REGIONS_CSV_PATH)


# Transform the "wide" format into "long" format, which is easier to work with.

# In[4]:


def melt_csv(df, var_name):
    return df.melt(
        id_vars=df.columns[0], 
        value_vars=df.columns[1:], 
        var_name='location', 
        value_name=var_name
    ).dropna()


# In[5]:


df_deaths = melt_csv(pd.read_csv(DEATHS_CSV_PATH, header=1).rename(columns={ 'Date': 'date' }), 'total_deaths')
df_cases = melt_csv(pd.read_csv(CASES_CSV_PATH, header=1).rename(columns={ 'Date': 'date' }), 'total_cases')


# Convert all numbers from floating point to integers:

# In[6]:


df_deaths['total_deaths'] = df_deaths['total_deaths'].astype('Int64')
df_cases['total_cases'] = df_cases['total_cases'].astype('Int64')


# ## Calculations

# Join cases & deaths into one dataframe

# In[7]:


df_merged = df_cases.merge(
    df_deaths, 
    how='outer', 
    left_on=['date', 'location'], 
    right_on=['date', 'location']
).sort_values(by=['location', 'date'])


# Standardize names to OWID names

# In[8]:


df_regions_merged = df_regions.merge(
    df_merged[['location']].drop_duplicates(),
    how="outer",
    left_on="WHO Country Name",
    right_on="location"
)


# In[9]:


assert(df_regions_merged['OWID Country Name'].isnull().any() == False)


# In[10]:


who_name_replace_map = { r['WHO Country Name']: r['OWID Country Name'] for r in df_regions_merged.to_dict('records') }


# In[11]:


df_merged['location'] = df_merged['location'].replace(who_name_replace_map)


# Calculate daily cases & deaths

# In[12]:


# Convert to Int64 to handle <NA>
df_merged['new_cases'] = df_merged.groupby('location')['total_cases'].diff().astype('Int64')
df_merged['new_deaths'] = df_merged.groupby('location')['total_deaths'].diff().astype('Int64')


# Create a `World` aggregate

# In[13]:


df_global = df_merged.groupby('date').sum().reset_index()
df_global['location'] = 'World'


# In[14]:


df_merged = pd.concat([df_merged, df_global], sort=True)


# Calculate days since 100th case

# In[15]:


THRESHOLD = 100
DAYS_SINCE_COL_NAME = 'days_since_%sth_case' % THRESHOLD
DAYS_SINCE_COL_NAME_POSITIVE = 'days_since_%sth_case_positive' % THRESHOLD


# In[16]:


def get_date_of_nth_case(df, nth):
    try:
        df_gt_nth = df[df['total_cases'] >= nth]
        earliest = df.loc[pd.to_datetime(df_gt_nth['date']).idxmin()]
        return earliest['date']
    except:
        return None


# In[17]:


date_of_nth_case = pd.DataFrame([
    (loc, get_date_of_nth_case(df, THRESHOLD)) 
    for loc, df in df_merged.groupby('location')
], columns=['location', 'date_of_nth_case']).dropna()


# In[18]:


def inject_days_since(df, ref_date):
    df = df[['date', 'location']].copy()
    df[DAYS_SINCE_COL_NAME] = df['date'].map(lambda date: (pd.to_datetime(date) - pd.to_datetime(ref_date)).days)
    return df


# In[19]:


df_grouped = df_merged.groupby('location')
df_days_since_nth_case = pd.concat([
    inject_days_since(df_grouped.get_group(row['location']), row['date_of_nth_case']) 
    for _, row in date_of_nth_case.iterrows()
])


# In[20]:


df_merged = df_merged.merge(
    df_days_since_nth_case,
    how='outer',
    on=['date', 'location'],
)
df_merged[DAYS_SINCE_COL_NAME] = df_merged[DAYS_SINCE_COL_NAME].astype('Int64')
df_merged[DAYS_SINCE_COL_NAME_POSITIVE] = df_merged[DAYS_SINCE_COL_NAME]     .map(lambda x: x if (pd.notna(x) and x >= 0) else None).astype('Int64')


# Calculate doubling rates

# In[21]:


def get_days_to_double(df, col_name):
    try:
        # verbose because being very careful not to modify original data with dates
        latest = df.loc[pd.to_datetime(df['date']).idxmax()]
        df_lt_half = df[df[col_name] <= (latest[col_name] / 2)]
        half = df_lt_half.loc[pd.to_datetime(df_lt_half['date']).idxmax()]
        return (pd.to_datetime(latest['date']) - pd.to_datetime(half['date'])).days
    except:
        return None


# In[22]:


days_to_double_cases = pd.DataFrame([
    (loc, get_days_to_double(df, 'total_cases')) 
    for loc, df in df_merged.groupby('location')
], columns=['location', 'days_to_double_cases'])
days_to_double_cases['days_to_double_cases'] = days_to_double_cases['days_to_double_cases'].astype('Int64')


# ### Grapher data extract

# In[23]:


df_grapher = df_merged.copy()
df_grapher['date'] = pd.to_datetime(df_grapher['date']).map(lambda date: (date - datetime(2020, 1, 21)).days)
df_grapher = df_grapher[['location', 'date', 'new_cases', 'new_deaths', 'total_cases', 'total_deaths', DAYS_SINCE_COL_NAME, DAYS_SINCE_COL_NAME_POSITIVE]]     .rename(columns={
        'location': 'country',
        'date': 'year',
        'new_cases': 'Daily new confirmed cases of COVID-19',
        'new_deaths': 'Daily new confirmed deaths due to COVID-19',
        'total_cases': 'Total confirmed cases of COVID-19', 
        'total_deaths': 'Total confirmed deaths due to COVID-19',
        DAYS_SINCE_COL_NAME: 'Days since the total confirmed cases of COVID-19 reached %s' % THRESHOLD,
        DAYS_SINCE_COL_NAME_POSITIVE: 'Days since the total confirmed cases of COVID-19 reached %s (positive only)' % THRESHOLD
    })


# ## Inspect the results

# In[24]:


# df_merged


# In[25]:


# df_merged[df_merged['location'] == 'World']


# ## Write output files

# In[26]:


df_merged[].to_csv(os.path.join(OUTPUT_PATH, 'full_data.csv'), index=False)


# In[27]:


for col_name in ['total_cases', 'total_deaths', 'new_cases', 'new_deaths']:
    df_pivot = df_merged.pivot(index='date', columns='location', values=col_name)
    # move World to first column
    cols = df_pivot.columns.tolist()
    cols.insert(0, cols.pop(cols.index('World')))
    df_pivot[cols].to_csv(os.path.join(OUTPUT_PATH, '%s.csv' % col_name))


# In[28]:


days_to_double_cases.to_csv(os.path.join(OUTPUT_PATH, 'days_to_double_cases.csv'), index=False)


# In[29]:


df_regions.to_csv(os.path.join(OUTPUT_PATH, 'regions.csv'), index=False)


# In[30]:


df_grapher.to_csv(os.path.join(OUTPUT_PATH, 'grapher.csv'), index=False)


# In[ ]:




