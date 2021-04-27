import re
from datetime import datetime, timedelta

import pandas as pd

from vax.utils.incremental import enrich_data, increment


vaccine_mapping = {
    "Pfizer": "Pfizer/BioNTech",
    "Moderna": "Moderna",
    "AstraZeneca": "Oxford/AstraZeneca",
    "Janssen": "Johnson&Johnson",
}


def read() -> pd.Series:
    source = _get_source_url()
    df = pd.read_excel(source, index_col=0)
    _check_vaccine_names(df)
    return parse_data(df, source)


def parse_data(df: pd.DataFrame, source: str) -> pd.Series:
    return pd.Series(data={
        "total_vaccinations": df.loc["Totales", "Dosis administradas (2)"].item(),
        "people_vaccinated": df.loc["Totales", "Nº Personas con al menos 1 dosis"].item(),
        "people_fully_vaccinated": df.loc["Totales", "Nº Personas vacunadas(pauta completada)"].item(),
        "date": df["Fecha de la última vacuna registrada (2)"].max().strftime("%Y-%m-%d"),
        "source_url": source,
        "vaccine": ", ".join(_get_vaccine_names(df, translate=True)),
    })


def _get_source_url():
    dt_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    return (
        f"https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/documentos/"
        f"Informe_Comunicacion_{dt_str}.ods"
    )


def _get_vaccine_names(df: pd.DataFrame, translate: bool = False):
    regex_vaccines = r'Dosis entregadas ([a-zA-Z]*) \(1\)'
    if translate:
        return sorted([
            vaccine_mapping[re.search(regex_vaccines, col).group(1)]
            for col in df.columns if re.match(regex_vaccines, col)
        ])
    else:
        return sorted([
            re.search(regex_vaccines, col).group(1) for col in df.columns if re.match(regex_vaccines, col)
        ])


def _check_vaccine_names(df: pd.DataFrame) -> pd.Series:
    vaccines = _get_vaccine_names(df)
    unknown_vaccines = set(vaccines).difference(vaccine_mapping.keys())
    if unknown_vaccines:
        raise ValueError("Found unknown vaccines: {}".format(unknown_vaccines))


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Spain")


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds
        .pipe(enrich_location)
    )


def main():
    data = read().pipe(pipeline)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"]
    )


if __name__ == "__main__":
    main()
