import re
import dateparser
import vaxutils
import pandas as pd


def read(source: str) -> pd.Series:
    return pd.read_json(source).pipe(lambda ds: pd.DataFrame.from_records(ds["stats"]).iloc[0])


def translate_index(input: pd.Series) -> pd.Series:
    return input.rename({
        'first_vaccine_number': 'people_vaccinated',
        'second_vaccine_number': 'people_fully_vaccinated',
    })


def add_totals(input: pd.Series) -> pd.Series:
    total_vaccinations = int(input['people_vaccinated']) + int(input['people_fully_vaccinated'])
    return vaxutils.enrich_data(input, 'total_vaccinations', total_vaccinations)


def format_date(input: pd.Series) -> pd.Series:
    date = input["vaccine_last_update"]
    date = re.search(r"\d+\. \w+", date).group(0)
    date = str(dateparser.parse(date, languages=["da"]).date())
    date = vaxutils.clean_date(date, "%Y-%m-%d")
    return vaxutils.enrich_data(input, 'date', date)


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Faeroe Islands")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Pfizer/BioNTech")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url', "https://corona.fo/api")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(translate_index)
            .pipe(add_totals)
            .pipe(format_date)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://corona.fo/json/stats"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=str(data['location']),
        total_vaccinations=int(data['total_vaccinations']),
        people_vaccinated=int(data['people_vaccinated']),
        people_fully_vaccinated=int(data['people_fully_vaccinated']),
        date=str(data['date']),
        source_url=str(data['source_url']),
        vaccine=str(data['vaccine'])
    )


if __name__ == "__main__":
    main()
