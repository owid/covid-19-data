from datetime import datetime
import pandas as pd

VACCINES_ACCEPTED = [
    "Pfizer/BioNTech", "Moderna", "Oxford/AstraZeneca", "Sputnik V", "Sinopharm/Beijing",
    "Sinopharm/Wuhan", "Johnson&Johnson", "Sinovac", "Covaxin", "EpiVacCorona", "CanSino"
]

def country_df_sanity_checks(
        df: pd.DataFrame, allow_extra_cols: bool = True, monotonic_check: bool = True) -> pd.DataFrame:
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
        vaccines_wrong = [vac for vac in vaccines_used if vac not in VACCINES_ACCEPTED]
        raise ValueError(f"{location} -- Invalid vaccine detected! Check {vaccines_wrong}.")
    # Date consistency
    if df.date.isnull().any():
        raise ValueError(f"{location} -- Invalid dates! NaN values found.")
    if (df.date.min() < datetime(2020, 12, 1)) or (df.date.max() > datetime.now().date()):
        raise ValueError(f"{location} -- Invalid dates! Check {df.date.min()} and {df.date.max()}")
    if df.date.nunique() != len(df):
        raise ValueError(f"{location} -- Mismatch between number of rows {len(df)} and number of different dates "
                            f"{df.date.nunique()}. Check {df.date.unique()}.")
    # Location consistency
    if df.location.isnull().any():
        raise ValueError(f"{location} -- Invalid location! NaN values found. Check {df.location}.")
    if df.location.nunique() != 1:
        raise ValueError(f"{location} -- Invalid location! More than one location found. Check {df.location}.")
    # Metrics consistency: TODO: 1) metrics monotonically increasing by date, 2) inequalities between them valid
    cols = ["total_vaccinations"]
    if "people_vaccinated" in df.columns:
        cols.append("people_vaccinated")
    if "people_fully_vaccinated" in df.columns:
        cols.append("people_fully_vaccinated")
    # Monotonically
    _df = df.sort_values(by="date")[cols]
    if monotonic_check:
        for col in cols:
            _x = _df[col].dropna()
            if not _x.is_monotonic:
                idx_wrong = _x.diff() < 0
                wrong = _x.loc[idx_wrong]
                raise ValueError(f"{location} -- Column {col} must be monotonically increasing! Check:\n{wrong}")
    # Inequalities
    _df = _df.dropna()
    if ("total_vaccinations" in _df.columns) and ("people_vaccinated" in _df.columns):
        _df = _df[["people_vaccinated", "total_vaccinations"]].dropna()
        if (_df["total_vaccinations"] < _df["people_vaccinated"]).any():
            raise ValueError(f"{location} -- total_vaccinations can't be < people_vaccinated!")
    if ("people_vaccinated" in _df.columns) and ("people_fully_vaccinated" in _df.columns):
        _df = _df[["people_vaccinated", "people_fully_vaccinated"]].dropna()
        if (_df["people_vaccinated"] < _df["people_fully_vaccinated"]).any():
            raise ValueError(f"{location} -- people_vaccinated can't be < people_fully_vaccinated!")
    if ("total_vaccinations" in _df.columns) and ("people_fully_vaccinated" in _df.columns):
        _df = _df[["people_fully_vaccinated", "total_vaccinations"]].dropna()
        if (_df["total_vaccinations"] < _df["people_fully_vaccinated"]).any():
            raise ValueError(f"{location} -- people_fully_vaccinated can't be < people_vaccinated!")
