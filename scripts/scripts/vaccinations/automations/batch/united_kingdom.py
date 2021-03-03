import pandas as pd

from utils.pipeline import enrich_total_vaccinations


def build_url(area: str) -> pd.DataFrame:
    metrics = "".join(
        f"&metric={m}"
        for m in [
            "cumPeopleVaccinatedFirstDoseByPublishDate",
            "cumPeopleVaccinatedFirstDoseByVaccinationDate",
            "cumPeopleVaccinatedSecondDoseByPublishDate",
            "cumPeopleVaccinatedSecondDoseByVaccinationDate",
        ]
    )
    return f"https://api.coronavirus.data.gov.uk/v2/data?areaType={area}&format=csv{metrics}"


def read(area: str) -> pd.DataFrame:
    uk = pd.read_csv(build_url("overview"))
    subnational = pd.read_csv(build_url("nation"))
    return (
        pd.concat([uk, subnational])
        .reset_index(drop=True)
        .rename(columns={"areaName": "location"})
    )


def enrich_people_vaccinated(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        people_vaccinated=df.cumPeopleVaccinatedFirstDoseByPublishDate.fillna(
            df.cumPeopleVaccinatedFirstDoseByVaccinationDate
        )
    )


def enrich_people_fully_vaccinated(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        people_fully_vaccinated=df.cumPeopleVaccinatedSecondDoseByPublishDate.fillna(
            df.cumPeopleVaccinatedSecondDoseByVaccinationDate
        )
    )


def aggregate_first_date(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(
            [
                "location",
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
            ],
            as_index=False,
        )[["date"]]
        .min()
        .replace(0, pd.NA)
    )


def enrich_source_url(df: pd.DataFrame, source_url: str) -> pd.DataFrame:
    return df.assign(source_url=source_url)


def enrich_vaccine(df: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine(date: str) -> str:
        if date >= "2021-01-04":
            return "Oxford/AstraZeneca, Pfizer/BioNTech"
        return "Pfizer/BioNTech"

    return df.assign(vaccine=df.date.apply(_enrich_vaccine))


def pipeline(df: pd.DataFrame, source_url: str) -> pd.DataFrame:
    return (
        df.pipe(enrich_people_vaccinated)
        .pipe(enrich_people_fully_vaccinated)
        .pipe(enrich_total_vaccinations)
        .pipe(aggregate_first_date)
        .pipe(enrich_source_url, source_url)
        .pipe(enrich_vaccine)
    )


def filter_location(df: pd.DataFrame, location: str) -> pd.DataFrame:
    return df[df.location == location].assign(location=location)


def main():
    source = "https://coronavirus.data.gov.uk/details/healthcare"
    result = read("overview").pipe(pipeline, source)

    for location in set(result.location):
        result.pipe(filter_location, location).to_csv(
            f"automations/output/{location}.csv", index=False
        )


if __name__ == "__main__":
    main()
