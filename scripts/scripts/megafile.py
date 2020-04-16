"""
Merges the main COVID-19 testing dataset with each of the COVID-19 ECDC datasets into a 'megafile';
- Follows a long format of 1 row per country & date, and variables as columns;
- Published in CSV and XLSX formats;
- Includes derived variables that can't be easily calculated, such as X per capita;
- Includes country ISO codes in a column next to country names.
"""


from functools import reduce
import pandas as pd


def get_testing():
    """
    Reads the main COVID-19 testing dataset located in /public/data/testing/
    Rearranges the Entity column to separate location from testing units
    Checks for duplicated location/date couples, as we can have more than 1 time series per country

    Returns:
    	testing {dataframe}
    """

    testing = pd.read_csv("../../public/data/testing/covid-testing-all-observations.csv", usecols=[
        "Entity",
        "Date",
        "Cumulative total",
        "Daily change in cumulative total",
        "Cumulative total per thousand",
        "Daily change in cumulative total per thousand"
    ])

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
    to_remove = [
        ("India", "people tested"),
        ("Japan", "tests performed"),
        ("United States", "specimens tested (CDC)"),
        ("Singapore", "swabs tested")
    ]
    for loc, unit in to_remove:
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
    for ecdc_var in ecdc_variables:

        tmp = pd.read_csv("../../public/data/ecdc/{}.csv".format(ecdc_var))
        tmp = tmp.drop(columns=["World", "International"])
        country_cols = list(tmp.columns)
        country_cols.remove("date")
        tmp = (
            pd.melt(tmp, id_vars="date", value_vars=country_cols)
            .rename(columns={"value": ecdc_var, "variable": "location"})
            .dropna()
        )
        tmp[ecdc_var] = tmp[ecdc_var].round(3)
        data_frames.append(tmp)

    # Outer join between all ECDC files
    ecdc = reduce(
        lambda left, right: pd.merge(left, right, on=["date", "location"], how="outer"),
        data_frames
    )

    return ecdc


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

    all_covid = (
        ecdc.merge(testing, on=["date", "location"], how="outer")
        .sort_values(["location", "date"])
    )

    iso_codes = pd.read_csv("../input/iso/iso3166_1_alpha_3_codes.csv")

    missing_iso = set(all_covid.location).difference(set(iso_codes.location))
    if len(missing_iso) > 0:
        print(missing_iso)
        raise Exception("Missing ISO code for some locations")

    all_covid = iso_codes.merge(all_covid, on="location")
    
    all_covid.to_csv("../../public/data/owid-covid-data.csv", index=False)
    all_covid.to_excel("../../public/data/owid-covid-data.xlsx", index=False)


if __name__ == '__main__':
    generate_megafile()
