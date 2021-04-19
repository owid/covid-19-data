from datetime import datetime
import pandas as pd

VACCINES_ACCEPTED = [
    "Pfizer/BioNTech", "Moderna", "Oxford/AstraZeneca", "Sputnik V", "Sinopharm/Beijing",
    "Sinopharm/Wuhan", "Johnson&Johnson", "Sinovac", "Covaxin", "EpiVacCorona", "CanSino"
]

def country_df_sanity_checks(df: pd.DataFrame, allow_extra_cols: bool = True) -> pd.DataFrame:
    location = df.loc[:, "location"].unique()
    # Ensure required columns are present
    cols = ["total_vaccinations", "vaccine", "date", "location", "source_url"]
    cols_extra = cols + ["people_vaccinated", "people_fully_vaccinated"]
    cols_missing = [col for col in cols if col not in df.columns]
    if cols_missing:
        raise ValueError(f"df missing column(s): {cols_missing}.")
    # Ensure validity of column names in df
    if not allow_extra_cols:
        cols_wrong = [col for col in df.columns if col not in cols_extra]
        if cols_wrong:
            raise ValueError(f"df contains invalid column(s): {cols_wrong}.")
    # Source url consistency
    if df.source_url.isnull().any():
        raise ValueError(f"{location} -- Invalid source_url! NaN values found.")
    # Vaccine consistency
    if df.vaccine.isnull().any():
        raise ValueError(f"{location} -- Invalid vaccine! NaN values found.")
    vaccines_used = set([xx for x in df.vaccine.tolist() for xx in x.split(', ')])
    if not all([vac in VACCINES_ACCEPTED for vac in vaccines_used]):
        raise ValueError(f"{location} -- Invalid vaccine detected! Check {df.vaccine.unique()}.")
    # Date consistency
    
    if df.date.isnull().any():
        raise ValueError(f"{location} -- Invalid dates! NaN values found.")
    if (df.date.min() < datetime(2020, 12, 1)) or (df.date.max() > datetime.now().date()):
        raise ValueError(f"{location} -- Invalid dates! Check {df.date.min()} and {df.date.max()}")
    if df.date.nunique() != len(df):
        raise ValueError(f"{location} -- Missmatch between number of rows {len(df)} and number of different dates "
                            f"{df.date.nunique()}. Check {df.date.unique()}.")
    # Location consistency
    if df.location.isnull().any():
        raise ValueError(f"{location} -- Invalid location! NaN values found. Check {df.location}.")
    if df.location.nunique() != 1:
        raise ValueError(f"{location} -- Invalid location! More than one location found. Check {df.location}.")
    # Metrics consistency: TODO: 1) metrics increscendo, 2) inequalities between them
    cols = ["total_vaccinations"]
    if "people_vaccinated" in df.columns:
        cols.append("people_vaccinated")
    if "people_fully_vaccinated" in df.columns:
        cols.append("people_fully_vaccinated")
    df_ = df.sort_values(by="date")[cols].dropna()
    # TODO: makes sense?
    #for col in cols: 
    #    if not df_[col].is_monotonic:
    #        raise ValueError(f"{location} -- Column {col} must be monotnically increasing!")
    if "people_vaccinated" in df_.columns:
        if (df_["total_vaccinations"] < df_["people_vaccinated"]).any():
            raise ValueError(f"{location} -- total_vaccinations can't be < people_vaccinated!")
        if "people_fully_vaccinated" in df_.columns:
            if (df_["people_vaccinated"] < df_["people_fully_vaccinated"]).any():
                raise ValueError(f"{location} -- people_vaccinated can't be < people_fully_vaccinated!")
    if "people_fully_vaccinated" in df_.columns:
        if (df_["total_vaccinations"] < df_["people_fully_vaccinated"]).any():
            raise ValueError(f"{location} -- people_fully_vaccinated can't be < people_vaccinated!")