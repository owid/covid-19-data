import pandas as pd


def enrich_total_vaccinations(input: pd.DataFrame) -> pd.DataFrame:
    return input.assign(
        total_vaccinations=input.people_vaccinated + input.people_fully_vaccinated
    )
