from datetime import datetime
import pandas as pd


def process_location(df: pd.DataFrame) -> pd.DataFrame:
    # Only report up to previous day to avoid partial reporting
    df = df.assign(date=pd.to_datetime(df.date, dayfirst=True))
    df = df[df.date.dt.date < datetime.now().date()] 
    # Default columns for second doses
    if "people_vaccinated" not in df:
        df = df.assign(people_vaccinated=pd.NA)
        df.people_vaccinated = df.people_vaccinated.astype("Int64")
    if "people_fully_vaccinated" not in df:
        df = df.assign(people_fully_vaccinated=pd.NA)
        df.people_fully_vaccinated = df.people_fully_vaccinated.astype("Int64")
    # Avoid decimals
    cols = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]
    df[cols] = df[cols].astype(float).astype("Int64").fillna(pd.NA)
    # Order columns and rows
    usecols = [
        "location", "date", "vaccine", "source_url", "total_vaccinations", "people_vaccinated",
        "people_fully_vaccinated"
    ]
    df = df[usecols]
    df = df.sort_values(by="date")
    # Sanity checks
    _sanity_checks(df)
    # Strip
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # Date format
    df = df.assign(date=df.date.dt.strftime("%Y-%m-%d"))

    return df


def _sanity_checks(df: pd.DataFrame) -> pd.DataFrame:
    location = df.loc[:, "location"].unique()
    vaccines_accepted = [
        "Pfizer/BioNTech", "Moderna", "Oxford/AstraZeneca", "Sputnik V", "Sinopharm/Beijing",
        "Sinopharm/Wuhan", "Johnson&Johnson", "Sinovac", "Covaxin", "EpiVacCorona"
    ]
    df_ = df[["people_vaccinated", "people_fully_vaccinated", "total_vaccinations"]].dropna()
    vaccines_used = set([xx for x in df.vaccine.tolist() for xx in x.split(', ')])
    if not all([vac in vaccines_accepted for vac in vaccines_used]):
        raise ValueError(f"{location} -- Invalid vaccine detected! Check {df.vaccine.unique()}")
    if (df.date.min() < datetime(2020, 12, 1)) or (df.date.max() > datetime.now().date()):
        raise ValueError(f"{location} -- Invalid dates! Check {df.date.min()} and {df.date.max()}")
    if any(df.location.isnull()) or df.location.nunique() != 1:
        raise ValueError(f"{location} -- Invalid location! Check {df.location}")
    if df.date.nunique() != len(df):
        raise ValueError(f"{location} -- Missmatch between number of rows {len(df)} and number of different dates"
                            f"{df.date.nunique()}. Check {df.date.unique()}")
    if any(df_.people_fully_vaccinated > df_.people_vaccinated) or any(df_.people_vaccinated > df_.total_vaccinations):
        raise ValueError(f"{location} -- Logic not valid! Check columns ['people_vaccinated', "
                          "'people_fully_vaccinated', 'total_vaccinations']")
