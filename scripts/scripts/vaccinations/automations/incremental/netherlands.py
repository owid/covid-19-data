import vaxutils
import pandas as pd


def read(source: str) -> pd.Series:
    data = pd.read_json(source)["vaccinaties"]["data"]["kpi_total"]
    assert data["title"] == "Number of doses administered"
    return parse_data(data)


def parse_data(data: pd.DataFrame) -> pd.Series:
    keys = ("date", "total_vaccinations")
    values = (parse_date(data), parse_total_vaccinations(data))
    data = dict(zip(keys, values))
    return pd.Series(data=data)


def parse_date(data: dict) -> str:
    date = data["date_of_report_unix"]
    date = str(pd.to_datetime(date, unit="s").date())
    return date


def parse_total_vaccinations(data: dict) -> int:
    total_vaccinations = data["value"]
    return vaxutils.clean_count(total_vaccinations)


def translate_index(input: pd.Series) -> pd.Series:
    return input.rename({
        'value': 'total_vaccinations',
        'date_of_report_unix': 'date',
    })


def enrich_location(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'location', "Netherlands")


def enrich_vaccine(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'vaccine', "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(input: pd.Series) -> pd.Series:
    return vaxutils.enrich_data(input, 'source_url', "https://coronadashboard.rijksoverheid.nl/landelijk/vaccinaties")


def pipeline(input: pd.Series) -> pd.Series:
    return (
        input.pipe(translate_index)
            .pipe(enrich_location)
            .pipe(enrich_vaccine)
            .pipe(enrich_source)
    )


def main():
    source = "https://raw.githubusercontent.com/minvws/nl-covid19-data-dashboard/master/packages/app/src/locale/en.json"
    data = read(source).pipe(pipeline)
    vaxutils.increment(
        location=data['location'],
        total_vaccinations=data['total_vaccinations'],
        date=data['date'],
        source_url=data['source_url'],
        vaccine=data['vaccine']
    )


if __name__ == "__main__":
    main()
