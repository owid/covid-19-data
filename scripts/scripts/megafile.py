"""
Merges the main COVID-19 testing dataset with each of the COVID-19 ECDC datasets into a 'megafile';
- Follows a long format of 1 row per country & date, and variables as columns;
- Published in CSV and XLSX formats;
- Includes derived variables that can't be easily calculated, such as X per capita;
- Includes country ISO codes in a column next to country names.
"""

import os
from datetime import datetime
from functools import reduce
import pandas as pd

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
            "Cumulative total per thousand",
            "Daily change in cumulative total per thousand"
        ]
    )

    testing.columns = [
        "location",
        "date",
        "total_tests",
        "new_tests",
        "total_tests_per_thousand",
        "new_tests_per_thousand"
    ]

    testing[["total_tests_per_thousand", "new_tests_per_thousand"]] = testing[
        ["total_tests_per_thousand", "new_tests_per_thousand"]
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
        "total_deaths",
        "new_deaths",
        "total_cases_per_million",
        "new_cases_per_million",
        "total_deaths_per_million",
        "new_deaths_per_million"
    ]

    data_frames = []

    # Process each file and melt it to vertical format
    print()
    for ecdc_var in ecdc_variables:

        tmp = pd.read_csv(os.path.join(DATA_DIR, "../../public/data/ecdc/{}.csv".format(ecdc_var)))

        for country in tmp.columns[2:]:
            if tmp[country].values[-2] > 0 and tmp[country].values[-1] > 100 \
                and tmp[country].values[-1] / tmp[country].values[-2] > 3:
                print("<!> Sudden increase in {}: {} from {} to {}".format(
                    ecdc_var,
                    country,
                    int(tmp[country].values[-2]),
                    int(tmp[country].values[-1])
                ))

        country_cols = list(tmp.columns)
        country_cols.remove("date")
        tmp = (
            pd.melt(tmp, id_vars="date", value_vars=country_cols)
            .rename(columns={"value": ecdc_var, "variable": "location"})
            .dropna()
        )
        tmp[ecdc_var] = tmp[ecdc_var].round(3)
        data_frames.append(tmp)
    print()

    # Outer join between all ECDC files
    ecdc = reduce(
        lambda left, right: pd.merge(left, right, on=["date", "location"], how="outer"),
        data_frames
    )

    return ecdc


def add_macro_variables(complete_dataset):
    """
    Appends a list of 'macro' (non-directly COVID related) variables to the dataset
    The data is denormalized, i.e. each yearly value (for example GDP per capita)
    is added to each row of the complete dataset. This is meant to facilitate the use
    of our dataset by non-experts.
    """

    original_shape = complete_dataset.shape

    # For each macro variable:
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
        "cvd_death_rate": "gbd/cvd_death_rate.csv",
        "diabetes_prevalence": "wb/diabetes_prevalence.csv",
        "female_smokers": "wb/female_smokers.csv",
        "male_smokers": "wb/male_smokers.csv",
        "handwashing_facilities": "un/handwashing_facilities.csv",
        "hospital_beds_per_100k": "owid/hospital_beds.csv"
    }

    for var, file in macro_variables.items():
        var_df = pd.read_csv(os.path.join(INPUT_DIR, file), usecols=["iso_code", var])
        var_df = var_df[-var_df["iso_code"].isnull()]
        var_df[var] = var_df[var].round(3)
        complete_dataset = complete_dataset.merge(var_df, on="iso_code", how="left")

    assert complete_dataset.shape[0] == original_shape[0]
    assert complete_dataset.shape[1] == original_shape[1] + len(macro_variables)

    return complete_dataset

def generate_megafile():
    """
    Main function of this script, run if __main__
    Imports and processes the testing data
    Imports and processes the ECDC data
    Merges testing and ECDC dataframes with an outer join
    Imports ISO 3166-1 alpha-3 codes
    Checks for missing ISO codes in the lookup file compared to OWID files
    Writes the 'megafile' to CSV and XLSX in /public/data/
    """

    testing = get_testing()

    ecdc = get_ecdc()

    location_mismatch = set(testing.location).difference(set(ecdc.location))
    for loc in location_mismatch:
        print(f"<!> Location '{loc}' has testing data but is absent from ECDC data")

    all_covid = (
        ecdc.merge(testing, on=["date", "location"], how="outer")
        .sort_values(["location", "date"])
    )

    iso_codes = pd.read_csv(os.path.join(INPUT_DIR, "iso/iso3166_1_alpha_3_codes.csv"))

    missing_iso = set(all_covid.location).difference(set(iso_codes.location))
    if len(missing_iso) > 0:
        print(missing_iso)
        raise Exception("Missing ISO code for some locations")

    all_covid = iso_codes.merge(all_covid, on="location")

    # Add macro variables
    all_covid = add_macro_variables(all_covid)

    # Convert some variables to int in the final output, if and only if their NAs mean "zero"
    integer_vars = ["total_cases", "new_cases", "total_deaths", "new_deaths"]
    all_covid[integer_vars] = all_covid[integer_vars].fillna(0).astype(int)

    all_covid.to_csv(os.path.join(DATA_DIR, "owid-covid-data.csv"), index=False)
    all_covid.to_excel(os.path.join(DATA_DIR, "owid-covid-data.xlsx"), index=False)

    # Store the last updated time
    timestamp_filename = os.path.join(DATA_DIR, "owid-covid-data-last-updated-timestamp.txt")
    with open(timestamp_filename, "w") as timestamp_file:
        timestamp_file.write(datetime.utcnow().replace(microsecond=0).isoformat())


if __name__ == '__main__':
    generate_megafile()
