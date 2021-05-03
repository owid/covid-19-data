import os
import re
from datetime import datetime, timedelta
from urllib.error import HTTPError

import pandas as pd

from vax.utils.incremental import enrich_data, increment


vaccine_mapping = {
    "Pfizer": "Pfizer/BioNTech",
    "Moderna": "Moderna",
    "AstraZeneca": "Oxford/AstraZeneca",
    "Janssen": "Johnson&Johnson",
}


def read(last_update: str) -> pd.Series:
    return parse_data(last_update)


def parse_data(last_update: str, max_iter: int = 10):
    records = []
    for days in range(10):
        date_it = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        # print(date_it)
        # print(f"{date_it} > {last_update}?")
        if date_it > last_update:
            source = _get_source_url(date_it.replace("-", ""))
            try:
                df_ = pd.read_excel(source, index_col=0)
            except HTTPError:
                print("No available!")
            else:
                # print("Adding!")
                _check_vaccine_names(df_)
                ds = _parse_ds_data(df_, source)
                records.append(ds)
        else:
            # print("End!")
            break
        # print(max_iter)
    if len(records) > 0:
        return pd.DataFrame(records)
    # print("No data being added to Spain")
    return None


def _parse_ds_data(df: pd.DataFrame, source: str) -> pd.Series:
    return pd.Series(data={
        "total_vaccinations": df.loc["Totales", "Dosis administradas (2)"].item(),
        "people_vaccinated": df.loc["Totales", "Nº Personas con al menos 1 dosis"].item(),
        "people_fully_vaccinated": df.loc["Totales", "Nº Personas vacunadas(pauta completada)"].item(),
        "date": df["Fecha de la última vacuna registrada (2)"].max().strftime("%Y-%m-%d"),
        "source_url": source,
        "vaccine": ", ".join(_get_vaccine_names(df, translate=True)),
    })


def _get_source_url(dt_str):
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


def _check_vaccine_names(df: pd.DataFrame):
    vaccines = _get_vaccine_names(df)
    unknown_vaccines = set(vaccines).difference(vaccine_mapping.keys())
    if unknown_vaccines:
        raise ValueError("Found unknown vaccines: {}".format(unknown_vaccines))


def enrich_location(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(location="Spain")


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .pipe(enrich_location)
    )


def merge_with_current_data(df: pd.DataFrame, filepath: str) -> pd.DataFrame:
    col_ints = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]
    # Load current data
    if os.path.isfile(filepath):
        df_current = pd.read_csv(filepath)
        # Merge
        df_current = df_current[~df_current.date.isin(df.date)]
        df = pd.concat([df, df_current]).sort_values(by="date")[df_current.columns]
        # Int values
    df[col_ints] = df[col_ints].astype("Int64").fillna(pd.NA)
    return df


def main():
    output_file = "output/Spain.csv"
    last_update = pd.read_csv(output_file).date.astype(str).max()
    df = read(last_update)
    if df is not None:
        df = df.pipe(pipeline)
        df = merge_with_current_data(df, output_file)
        df.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()
