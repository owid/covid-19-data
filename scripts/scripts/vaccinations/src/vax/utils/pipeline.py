import pandas as pd


def enrich_total_vaccinations(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated
    )
