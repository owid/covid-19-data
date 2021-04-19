from datetime import datetime
import pandas as pd

from vax.utils.checks import country_df_sanity_checks


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
    country_df_sanity_checks(df)
    # Strip
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # Date format
    df = df.assign(date=df.date.dt.strftime("%Y-%m-%d"))

    return df
