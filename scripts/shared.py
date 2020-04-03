import pandas as pd
import os
from datetime import datetime

CURRENT_DIR = os.path.dirname(__file__)
POPULATION_CSV_PATH = os.path.join(CURRENT_DIR, '../input/un/population_2020.csv')
CONTINENTS_CSV_PATH = os.path.join(CURRENT_DIR, '../input/owid/continents.csv')

# Per population calculations

def _find_closest_year_row(df, year=2020):
    """Returns the row which is closest to the year specified (in either direction)"""
    df = df.copy()
    df['year'] = df['year'].sort_values(ascending=True)
    return df.loc[df['year'].map(lambda x: abs(x - 2020)).idxmin()]

def load_population(year=2020):
    df = pd.read_csv(
        POPULATION_CSV_PATH,
        keep_default_na=False
    )
    return pd.DataFrame([
        _find_closest_year_row(df_group, year)
        for loc, df_group in df.groupby('entity')
    ]) \
    .dropna() \
    .rename(columns={'entity': 'location', 'year': 'population_year'})

def load_owid_continents():
    df = pd.read_csv(
        CONTINENTS_CSV_PATH,
        keep_default_na=False,
        header=0,
        names=['location', 'code', 'year', 'continent'],
        usecols=['location', 'continent']
    )
    return df

# Useful for adding it to regions.csv and
def inject_population(df):
    return df.merge(
        load_population(),
        how='left',
        on='location'
    )

def inject_per_million(df, measures):
    df = inject_population(df)
    for measure in measures:
        pop_measure = measure + '_per_million'
        df[pop_measure] = df[measure] / (df['population'] / 1e6)
    return df.drop(columns=['population_year', 'population'])

# OWID continents + custom aggregates

aggregates_spec = {
    'World': {
        'include': None,
        'exclude': None
    },
    'World excl. China': {
        'exclude': ['China']
    },
    'World excl. China and South Korea': {
        'exclude': ['China', 'South Korea']
    },
    'World excl. China, South Korea, Japan and Singapore': {
        'exclude': ['China', 'South Korea', 'Japan', 'Singapore']
    },
    # OWID continents
    **{
        continent: { 'include': locations, 'exclude': None }
        for continent, locations in load_owid_continents() \
            .groupby('continent')['location'].apply(list) \
            .to_dict().items()
    }
    # European Union
    # TODO
    # World Bank income groups
    # TODO
}

def _sum_aggregate(df, name, include=None, exclude=None):
    df = df.copy()
    if include:
        df = df[df['location'].isin(include)]
    if exclude:
        df = df[~df['location'].isin(exclude)]
    df = df.groupby('date') \
        .sum() \
        .reset_index()
    df['location'] = name
    return df

def inject_owid_aggregates(df):
    return pd.concat([
        df,
        *[
            _sum_aggregate(df, name, **params)
            for name, params in aggregates_spec.items()
        ]
    ], sort=True, ignore_index=True)

# Total/daily calculation

def inject_total_daily_cols(df, measures):
    # must sort in order to have the cumsum() and diff() in the right direction
    df = df.copy().sort_values(by=['location', 'date'])
    for measure in measures:
        total_col = 'total_%s' % measure
        daily_col = 'new_%s' % measure
        if total_col not in df.columns and daily_col in df.columns:
            df[total_col] = df.groupby('location')[daily_col].cumsum().astype('Int64')
        elif daily_col not in df.columns and total_col in df.columns:
            df[daily_col] = df.groupby('location')[total_col].diff().astype('Int64')
    return df


days_since_spec = {
    'days_since_100th_case': {
        'col': 'total_cases',
        'threshold': 100
    },
    'days_since_5th_death': {
        'col': 'total_deaths',
        'threshold': 5
    },
    'days_since_1_per_million_cases': {
        'col': 'total_cases_per_million',
        'threshold': 1
    },
    'days_since_0_1_per_million_deaths': {
        'col': 'total_deaths_per_million',
        'threshold': 0.1
    },
    # temporary definitions, figure out later
    'days_since_30_new_cases': {
        'col': 'new_cases',
        'threshold': 30
    },
    'days_since_50_new_cases': {
        'col': 'new_cases',
        'threshold': 50
    },
    'days_since_10_new_deaths': {
        'col': 'new_deaths',
        'threshold': 10
    },
    'days_since_5_new_deaths': {
        'col': 'new_deaths',
        'threshold': 5
    },
    'days_since_3_new_deaths': {
        'col': 'new_deaths',
        'threshold': 3
    },
    'days_since_5_new_deaths_7_day_avg': {
        'col': 'new_deaths_7_day_avg',
        'threshold': 5
    },
}

def _get_date_of_nth(df, col, nth):
    try:
        df_gt_nth = df[df[col] >= nth]
        earliest = df.loc[pd.to_datetime(df_gt_nth['date']).idxmin()]
        return earliest['date']
    except:
        return None

def _positive_date_diff(a, b):
    diff = (a - b).days
    return diff if diff >= 0 else None

def _inject_days_since(df, col, ref_date):
    df[col] = df['date'].map(lambda date: _positive_date_diff(pd.to_datetime(date), pd.to_datetime(ref_date))).astype('Int64')
    df = df
    return df

def _inject_days_since_group(df):
    df = df.copy()
    for col, spec in days_since_spec.items():
        date = _get_date_of_nth(df, spec['col'], spec['threshold'])
        if date is not None:
            df = _inject_days_since(df, col, date)
    return df

def inject_days_since_all(df):
    return pd.concat([
        _inject_days_since_group(df_group)
        for loc, df_group in df.groupby('location')
    ])

def _apply_row_cfr_100(row):
    if pd.notnull(row['total_cases']) and row['total_cases'] >= 100:
        return row['cfr']
    return pd.NA

def inject_cfr(df):
    df = df.copy()
    df['cfr'] = (df['total_deaths'] / df['total_cases']) * 100
    df['cfr_100_cases'] = df.apply(_apply_row_cfr_100, axis=1)
    return df

rolling_avg_spec = {
    'new_cases_3_day_avg': {
        'col': 'new_cases',
        'window': 3,
        'min_periods': 1,
        'center': True
    },
    'new_deaths_3_day_avg': {
        'col': 'new_deaths',
        'window': 3,
        'min_periods': 1,
        'center': True
    },
    'new_cases_7_day_avg': {
        'col': 'new_cases',
        'window': 7,
        'min_periods': 3,
        'center': True
    },
    'new_deaths_7_day_avg': {
        'col': 'new_deaths',
        'window': 7,
        'min_periods': 3,
        'center': True
    },
}

def inject_rolling_avg(df):
    df = df.copy().sort_values(by='date')
    for col, spec in rolling_avg_spec.items():
        df[col] = df[spec['col']].astype('float')
        df[col] = df.groupby('location', as_index=False)[col] \
            .rolling(window=spec['window'], min_periods=spec['min_periods'], center=spec['center']) \
            .mean().reset_index(level=0, drop=True)
    return df


# Export logic

KEYS = ['date', 'location']

BASE_MEASURES = [
    'new_cases', 'new_deaths',
    'total_cases', 'total_deaths'
]

PER_MILLION_MEASURES = [
    '%s_per_million' % m for m in BASE_MEASURES
]

DAYS_SINCE_MEASURES = list(days_since_spec.keys())

# Should keep these append-only in case someone external depends on the order
FULL_DATA_COLS = [
    *KEYS,
    *BASE_MEASURES
]

GRAPHER_COL_NAMES = {
    'location': 'Country',
    'date': 'Year',
    'new_cases': 'Daily new confirmed cases of COVID-19',
    'new_deaths': 'Daily new confirmed deaths due to COVID-19',
    'total_cases': 'Total confirmed cases of COVID-19',
    'total_deaths': 'Total confirmed deaths due to COVID-19',
    'new_cases_per_million': 'Daily new confirmed cases of COVID-19 per million people',
    'new_deaths_per_million': 'Daily new confirmed deaths due to COVID-19 per million people',
    'total_cases_per_million': 'Total confirmed cases of COVID-19 per million people',
    'total_deaths_per_million': 'Total confirmed deaths due to COVID-19 per million people',
    'days_since_100th_case': 'Days since the total confirmed cases of COVID-19 reached 100',
    'days_since_5th_death': 'Days since the total confirmed deaths of COVID-19 reached 5',
    'days_since_1_per_million_cases': 'Days since the total confirmed cases of COVID-19 per million people reached 1',
    'days_since_0_1_per_million_deaths': 'Days since the total confirmed deaths of COVID-19 per million people reached 0.1',
    'days_since_30_new_cases': 'Days since 30 daily new confirmed cases recorded',
    'days_since_50_new_cases': 'Days since 50 daily new confirmed cases recorded',
    'days_since_10_new_deaths': 'Days since 10 daily new confirmed deaths recorded',
    'days_since_5_new_deaths': 'Days since 5 daily new confirmed deaths recorded',
    'days_since_3_new_deaths': 'Days since 3 daily new confirmed deaths recorded',
    'days_since_5_new_deaths_7_day_avg': 'Days since daily new confirmed deaths (rolling 7-day average) reached 5',
    'cfr': 'Case fatality rate of COVID-19 (%)',
    'cfr_100_cases': 'Case fatality rate of COVID-19 (%) (Only observations with ≥100 cases)',
    'new_cases_3_day_avg': 'Daily new confirmed cases of COVID-19 (rolling 3-day average)',
    'new_cases_7_day_avg': 'Daily new confirmed cases of COVID-19 (rolling 7-day average)',
    'new_deaths_3_day_avg': 'Daily new confirmed deaths due to COVID-19 (rolling 3-day average)',
    'new_deaths_7_day_avg': 'Daily new confirmed deaths due to COVID-19 (rolling 7-day average)'
}

def existsin(l1, l2):
    return [x for x in l1 if x in l2]

def standard_export(df, output_path, grapher_name):
    # Grapher
    df_grapher = df.copy()
    df_grapher['date'] = pd.to_datetime(df_grapher['date']).map(lambda date: (date - datetime(2020, 1, 21)).days)
    df_grapher = df_grapher[GRAPHER_COL_NAMES.keys()] \
        .rename(columns=GRAPHER_COL_NAMES) \
        .to_csv(os.path.join(output_path, '%s.csv' % grapher_name), index=False)

    # Table & public extracts for external users
    # Excludes most aggregates
    excluded_aggregates = list(set(aggregates_spec.keys()) - set(['World']))
    df_table = df[~df['location'].isin(excluded_aggregates)]
    # full_data.csv
    full_data_cols = existsin(FULL_DATA_COLS, df_table.columns)
    df_table[full_data_cols] \
        .dropna(subset=BASE_MEASURES, how='all') \
        .to_csv(
            os.path.join(output_path, 'full_data.csv'),
            index=False
        )
    # Pivot variables (wide format)
    for col_name in [*BASE_MEASURES, *PER_MILLION_MEASURES]:
        df_pivot = df_table.pivot(index='date', columns='location', values=col_name)
        # move World to first column
        cols = df_pivot.columns.tolist()
        cols.insert(0, cols.pop(cols.index('World')))
        df_pivot[cols].to_csv(os.path.join(output_path, '%s.csv' % col_name))
    return True
