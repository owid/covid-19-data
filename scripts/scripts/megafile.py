"""
Merges the main COVID-19 testing dataset with each of the COVID-19 ECDC datasets into a 'megafile';
- Follows a long format of 1 row per country & date, and variables as columns;
- Published in CSV, XLSX, and JSON formats;
- Includes derived variables that can't be easily calculated, such as X per capita;
- Includes country ISO codes in a column next to country names.
"""

import json
import os
from datetime import datetime
from functools import reduce
import pandas as pd
import numpy as np


CURRENT_DIR = os.path.dirname(__file__)
INPUT_DIR = os.path.join(CURRENT_DIR, "../input/")
DATA_DIR = os.path.join(CURRENT_DIR, "../../public/data/")


def get_testing():
    """
    Reads the main COVID-19 testing dataset located in /public/data/testing/
    Rearranges the Entity column to separate location from testing units
    Checks for duplicated location/date couples, as we can have more than 1 time series per country

    Returns:
        testing {dataframe}
    """

    testing = pd.read_csv(
        os.path.join(DATA_DIR, "testing/covid-testing-all-observations.csv"),
        usecols=[
            "Entity",
            "Date",
            "Cumulative total",
            "Daily change in cumulative total",
            "7-day smoothed daily change",
            "Cumulative total per thousand",
            "Daily change in cumulative total per thousand",
            "7-day smoothed daily change per thousand",
            "Short-term positive rate",
            "Short-term tests per case"
        ]
    )

    testing = testing.rename(columns={
        "Entity": "location",
        "Date": "date",
        "Cumulative total": "total_tests",
        "Daily change in cumulative total": "new_tests",
        "7-day smoothed daily change": "new_tests_smoothed",
        "Cumulative total per thousand": "total_tests_per_thousand",
        "Daily change in cumulative total per thousand": "new_tests_per_thousand",
        "7-day smoothed daily change per thousand": "new_tests_smoothed_per_thousand",
        "Short-term positive rate": "positive_rate",
        "Short-term tests per case": "tests_per_case"
    })

    testing[
        ["total_tests_per_thousand", "new_tests_per_thousand", "new_tests_smoothed_per_thousand"]
    ] = testing[
        ["total_tests_per_thousand", "new_tests_per_thousand", "new_tests_smoothed_per_thousand"]
    ].round(3)

    # Split the original entity into location and testing units
    testing[["location", "tests_units"]] = testing.location.str.split(" - ", expand=True)

    # For locations with >1 series, choose a series
    to_remove = pd.read_csv(os.path.join(INPUT_DIR, "owid/secondary_testing_series.csv"))
    for loc, unit in to_remove.itertuples(index=False, name=None):
        testing = testing[-((testing["location"] == loc) & (testing["tests_units"] == unit))]

    # Check for remaining duplicates of location/date
    duplicates = testing.groupby(["location", "date"]).size().to_frame("n")
    duplicates = duplicates[duplicates["n"] > 1]
    if duplicates.shape[0] > 0:
        print(duplicates)
        raise Exception("Multiple rows for the same location and date")

    return testing


def get_ecdc():
    """
    Reads each COVID-19 ECDC dataset located in /public/data/ecdc/
    Melts the dataframe to vertical format (1 row per country and date)
    Merges all ECDC dataframes into one with outer joins

    Returns:
        ecdc {dataframe}
    """

    ecdc_variables = [
        "total_cases",
        "new_cases",
        "weekly_cases",
        "total_deaths",
        "new_deaths",
        "weekly_deaths",
        "total_cases_per_million",
        "new_cases_per_million",
        "weekly_cases_per_million",
        "total_deaths_per_million",
        "new_deaths_per_million",
        "weekly_deaths_per_million"
    ]

    data_frames = []

    # Process each file and melt it to vertical format
    for ecdc_var in ecdc_variables:
        tmp = pd.read_csv(os.path.join(DATA_DIR, f"../../public/data/ecdc/{ecdc_var}.csv"))
        country_cols = list(tmp.columns)
        country_cols.remove("date")

        # Carrying last observation forward for International totals to avoid discrepancies
        if ecdc_var[:5] == "total":
            tmp = tmp.sort_values("date")
            tmp["International"] = tmp["International"].ffill()

        tmp = (
            pd.melt(tmp, id_vars="date", value_vars=country_cols)
            .rename(columns={"value": ecdc_var, "variable": "location"})
            .dropna()
        )
        if ecdc_var[:7] == "weekly_":
            tmp[ecdc_var] = tmp[ecdc_var].div(7).round(3)
            tmp = tmp.rename(errors="ignore", columns={
                "weekly_cases": "new_cases_smoothed",
                "weekly_deaths": "new_deaths_smoothed",
                "weekly_cases_per_million": "new_cases_smoothed_per_million",
                "weekly_deaths_per_million": "new_deaths_smoothed_per_million"
            })
        else:
            tmp[ecdc_var] = tmp[ecdc_var].round(3)
        data_frames.append(tmp)
    print()

    # Outer join between all ECDC files
    ecdc = reduce(
        lambda left, right: pd.merge(left, right, on=["date", "location"], how="outer"),
        data_frames
    )

    return ecdc


def add_macro_variables(complete_dataset, macro_variables):
    """
    Appends a list of 'macro' (non-directly COVID related) variables to the dataset
    The data is denormalized, i.e. each yearly value (for example GDP per capita)
    is added to each row of the complete dataset. This is meant to facilitate the use
    of our dataset by non-experts.
    """

    original_shape = complete_dataset.shape

    for var, file in macro_variables.items():
        var_df = pd.read_csv(os.path.join(INPUT_DIR, file), usecols=["iso_code", var])
        var_df = var_df[-var_df["iso_code"].isnull()]
        var_df[var] = var_df[var].round(3)
        complete_dataset = complete_dataset.merge(var_df, on="iso_code", how="left")

    assert complete_dataset.shape[0] == original_shape[0]
    assert complete_dataset.shape[1] == original_shape[1] + len(macro_variables)

    return complete_dataset


def get_cgrt():
    """
    Downloads the latest OxCGRT dataset from BSG's GitHub repository
    Remaps BSG country names to OWID country names

    Returns:
        cgrt {dataframe}
    """

    cgrt = pd.read_csv(
        "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv",
        low_memory=False
    )

    if "RegionCode" in cgrt.columns:
        cgrt = cgrt[cgrt.RegionCode.isnull()]

    cgrt = cgrt[["CountryName", "Date", "StringencyIndex"]]

    cgrt.loc[:, "Date"] = pd.to_datetime(cgrt["Date"], format="%Y%m%d").dt.date.astype(str)

    country_mapping = pd.read_csv(os.path.join(INPUT_DIR, "bsg/bsg_country_standardised.csv"))

    cgrt = country_mapping.merge(cgrt, on="CountryName", how="right")

    missing_from_mapping = cgrt[cgrt["Country"].isna()]["CountryName"].unique()
    if len(missing_from_mapping) > 0:
        raise Exception(f"Missing countries in OxCGRT mapping: {missing_from_mapping}")

    cgrt = cgrt.drop(columns=["CountryName"])

    rename_dict = {
        "Country": "location",
        "Date": "date",
        "StringencyIndex": "stringency_index"
    }

    cgrt = cgrt.rename(columns=rename_dict)

    return cgrt


def df_to_json(complete_dataset, output_path, static_columns):
    """
    Writes a JSON version of the complete dataset, with the ISO code at the root.
    NA values are dropped from the output.
    Macro variables are normalized by appearing only once, at the root of each ISO code.
    """
    megajson = {}

    static_columns = ["continent", "location"] + list(static_columns)

    complete_dataset = complete_dataset.dropna(axis="rows", subset=["iso_code"])

    for _, row in complete_dataset.iterrows():

        row_iso = row["iso_code"]
        row_dict_static = row.drop("iso_code")[static_columns].dropna().to_dict()
        row_dict_dynamic = row.drop("iso_code").drop(static_columns).dropna().to_dict()

        if row_iso not in megajson:
            megajson[row_iso] = row_dict_static
            megajson[row_iso]["data"] = [row_dict_dynamic]
        else:
            megajson[row_iso]["data"].append(row_dict_dynamic)

    with open(output_path, "w") as file:
        file.write(json.dumps(megajson, indent=4))


def generate_megafile():
    """
    Main function of this script, run if __main__
    Imports and processes the testing data
    Imports and processes the ECDC data
    Imports and processes the OxCGRT data
    Merges testing and ECDC dataframes with an outer join
    Imports ISO 3166-1 alpha-3 codes
    Checks for missing ISO codes in the lookup file compared to OWID files
    Writes the 'megafile' to CSV and XLSX in /public/data/
    """

    print("\nFetching testing dataset…")
    testing = get_testing()

    print("\nFetching ECDC dataset…")
    ecdc = get_ecdc()

    location_mismatch = set(testing.location).difference(set(ecdc.location))
    for loc in location_mismatch:
        print(f"<!> Location '{loc}' has testing data but is absent from ECDC data")

    print("\nFetching OxCGRT dataset…")
    cgrt = get_cgrt()

    all_covid = (
        ecdc
        .merge(testing, on=["date", "location"], how="outer")
        .merge(cgrt, on=["date", "location"], how="left")
        .sort_values(["location", "date"])
    )

    # Add ISO codes
    print("Adding ISO codes…")
    iso_codes = pd.read_csv(os.path.join(INPUT_DIR, "iso/iso3166_1_alpha_3_codes.csv"))

    missing_iso = set(all_covid.location).difference(set(iso_codes.location))
    if len(missing_iso) > 0:
        print(missing_iso)
        raise Exception("Missing ISO code for some locations")

    all_covid = iso_codes.merge(all_covid, on="location")

    # Add continents
    print("Adding continents…")
    continents = pd.read_csv(
        os.path.join(INPUT_DIR, "owid/continents.csv"),
        names=["_1", "iso_code", "_2", "continent"],
        usecols=["iso_code", "continent"],
        header=0
    )

    all_covid = continents.merge(all_covid, on="iso_code", how="right")

    # Add macro variables
    # - the key is the name of the variable of interest
    # - the value is the path to the corresponding file
    macro_variables = {
        "population": "un/population_2020.csv",
        "population_density": "wb/population_density.csv",
        "median_age": "un/median_age.csv",
        "aged_65_older": "wb/aged_65_older.csv",
        "aged_70_older": "un/aged_70_older.csv",
        "gdp_per_capita": "wb/gdp_per_capita.csv",
        "extreme_poverty": "wb/extreme_poverty.csv",
        "cardiovasc_death_rate": "gbd/cardiovasc_death_rate.csv",
        "diabetes_prevalence": "wb/diabetes_prevalence.csv",
        "female_smokers": "wb/female_smokers.csv",
        "male_smokers": "wb/male_smokers.csv",
        "handwashing_facilities": "un/handwashing_facilities.csv",
        "hospital_beds_per_thousand": "owid/hospital_beds.csv",
        "life_expectancy": "owid/life_expectancy.csv"
    }
    all_covid = add_macro_variables(all_covid, macro_variables)

    print("Writing to CSV…")
    all_covid.to_csv(os.path.join(DATA_DIR, "owid-covid-data.csv"), index=False)

    print("Writing to XLSX…")
    all_covid.to_excel(os.path.join(DATA_DIR, "owid-covid-data.xlsx"), index=False)

    print("Writing to JSON…")
    df_to_json(all_covid, os.path.join(DATA_DIR, "owid-covid-data.json"), macro_variables.keys())

    # Store the last updated time
    timestamp_filename = os.path.join(DATA_DIR, "owid-covid-data-last-updated-timestamp.txt")
    with open(timestamp_filename, "w") as timestamp_file:
        timestamp_file.write(datetime.utcnow().replace(microsecond=0).isoformat())

    print("All done!")


if __name__ == '__main__':
    generate_megafile()
