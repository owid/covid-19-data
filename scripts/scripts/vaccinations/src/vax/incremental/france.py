import pandas as pd
from vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    data = pd.read_csv(source, sep=";", usecols=["fra", "jour", "n_tot_dose1", "n_tot_dose2"])
    return data.set_index(data.columns[0]).T.squeeze()


def translate_index(ds: pd.Series) -> pd.Series:
    return ds.rename({
        'n_tot_dose1': 'people_vaccinated',
        'n_tot_dose2': 'people_fully_vaccinated',
        'jour': 'date',
    })


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = int(ds['people_vaccinated']) + int(ds['people_fully_vaccinated'])
    return enrich_data(ds, 'total_vaccinations', total_vaccinations)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'location', "France")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'vaccine', "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, 'source_url',
                                "https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-personnes-vaccinees-contre-la-covid-19-1/")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(translate_index)
            .pipe(add_totals)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://www.data.gouv.fr/fr/datasets/r/131c6b39-51b5-40a7-beaa-0eafc4b88466"
    data = read(source).pipe(pipeline)
    increment(
        location=data['location'],
        total_vaccinations=int(data['total_vaccinations']),
        people_vaccinated=int(data['people_vaccinated']),
        people_fully_vaccinated=int(data['people_fully_vaccinated']),
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )


if __name__ == "__main__":
    main()
