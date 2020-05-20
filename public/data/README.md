# Data on COVID-19 (coronavirus) by _Our World in Data_

Our complete COVID-19 dataset is a collection of the COVID-19 data maintained by [_Our World in Data_](https://ourworldindata.org/coronavirus). It is updated daily and includes data on confirmed cases, deaths, and testing, as well as other variables of potential interest.

🗂️ [Download our complete COVID-19 dataset (CSV)](https://covid.ourworldindata.org/data/owid-covid-data.csv)

🗂️ [Download our complete COVID-19 dataset (XLSX)](https://covid.ourworldindata.org/data/owid-covid-data.xlsx)

**We will continue to publish up-to-date data on confirmed cases, deaths, and testing, throughout the duration of the COVID-19 pandemic.**


## Our data sources

- **Confirmed cases and deaths:** our data comes from the [European Centre for Disease Prevention and Control](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide) (ECDC). We discuss how and when the ECDC collects and publishes this data [here](https://ourworldindata.org/coronavirus-source-data). The cases & deaths dataset is updated daily.
- **Testing for COVID-19:** this data is collected by the _Our World in Data_ team from official reports; you can find the source information for every country and further details [in our post on COVID-19 testing](https://ourworldindata.org/covid-testing). The testing dataset is updated around twice a week.
- **Other variables:** this data is collected from a variety of sources (United Nations, World Bank, Global Burden of Disease, etc.). More information is available in our codebook ([`owid-covid-data-codebook.md`](https://github.com/owid/covid-19-data/tree/master/public/data/owid-covid-data-codebook.md)).


## The complete _Our World in Data_ COVID-19 dataset

**Our complete COVID-19 dataset is available in [CSV](https://covid.ourworldindata.org/data/owid-covid-data.csv) and [XLSX](https://covid.ourworldindata.org/data/owid-covid-data.xlsx) format, and includes all of our historical data on the pandemic up to the date of publication.**

The dataset follows a format of 1 row per location and date. The other columns represent all of our main variables related to confirmed cases, deaths, and testing, as well as other variables of potential interest.

As of 19 May 2020, the columns are: `iso_code`, `location`, `date`, `total_cases`, `new_cases`, `total_deaths`, `new_deaths`, `total_cases_per_million`, `new_cases_per_million`, `total_deaths_per_million`, `new_deaths_per_million`, `total_tests`, `new_tests`, `new_tests_smoothed`, `total_tests_per_thousand`, `new_tests_per_thousand`, `new_tests_smoothed_per_thousand`, `tests_units`, `stringency_index`, `population`, `population_density`, `median_age`, `aged_65_older`, `aged_70_older`, `gdp_per_capita`, `extreme_poverty`, `cvd_death_rate`, `diabetes_prevalence`, `female_smokers`, `male_smokers`, `handwashing_facilities`, `hospital_beds_per_100k`

A full codebook is made available ([`owid-covid-data-codebook.md`](https://github.com/owid/covid-19-data/tree/master/public/data/owid-covid-data-codebook.md)), with a description and source for each variable in the dataset.


## Additional files and information

If you are interested in the individual files that make up the complete dataset, or more detailed information, other files can be found in the subfolders:

- [`ecdc`](https://github.com/owid/covid-19-data/tree/master/public/data/ecdc): data from the European Centre for Disease Prevention and Control, related to confirmed cases and deaths;
- [`testing`](https://github.com/owid/covid-19-data/tree/master/public/data/testing): data from various official sources, related to COVID-19 tests performed in each country. This folder contains two files with more detailed information:
  - [`covid-testing-all-observations.csv`](https://github.com/owid/covid-19-data/blob/master/public/data/testing/covid-testing-all-observations.csv) includes, for each historical observation, the source of the individual data point, and sometimes notes on data collection;
  - [`covid-testing-latest-data-source-details.csv`](https://github.com/owid/covid-19-data/blob/master/public/data/testing/covid-testing-latest-data-source-details.csv) includes, for each country in our testing dataset, the latest figures and a detailed description of how the country’s data is collected.
- [`who`](https://github.com/owid/covid-19-data/tree/master/public/data/who): data from the World Health Organization, related to confirmed cases and deaths—_we have stopped using and updating this data since 18 March 2020_.


## Changelog

- Up until 17 March 2020, we were using WHO data manually extracted from their daily [situation report PDFs](https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports).
- From 19 March 2020, we started relying on data published by the [European CDC](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide). We wrote about [why we decided to switch sources](https://ourworldindata.org/covid-sources-comparison).
- On 3 April 2020, we added country-level time series on COVID-19 tests.
- On 16 April 2020, we made available a [complete dataset of all of our main variables](https://github.com/owid/covid-19-data/tree/master/public/data) related to confirmed cases, deaths, and tests.
- On 25 April 2020, we added rows for "World" and "International" to our complete dataset. The `iso_code` column for "International" is blank, and for "World" we use `OWID_WRL`.


## Data alterations

- **We standardize names of countries and regions.** Since the names of countries and regions are different in different data sources, we standardize all names to the [_Our World in Data_ standard entity names](https://github.com/owid/covid-19-data/blob/master/public/data/ecdc/locations.csv).
- We may correct or discard inconsistencies that we detect in the original data.
- Testing data is collected from many different sources. A detailed documentation for each country is available in [our post on COVID-19 testing](https://ourworldindata.org/coronavirus-testing#source-information-country-by-country).
- Where we collect multiple time series for a given country in our testing data (for example: for the United States, we collect data from both the CDC, and the COVID Tracking Project), our complete COVID-19 dataset only includes the most complete, or, if equally complete, data on the number of people tested rather than the number of tests/samples/swabs processed. The list of 'secondary' test series (those removed) is located in [`scripts/input/owid/secondary_testing_series.csv`](https://github.com/owid/covid-19-data/blob/master/scripts/input/owid/secondary_testing_series.csv).


## Stable URLs

The `/public` path of this repository is hosted at `https://covid.ourworldindata.org/`. For example, you can access the CSV for the complete dataset at `https://covid.ourworldindata.org/data/owid-covid-data.csv`.

We have the goal to keep all stable URLs working, even when we have to restructure this repository. If you need regular updates, please consider using the `covid.ourworldindata.org` URLs rather than pointing to GitHub.


## License

All of _Our World in Data_ is completely open access and all work is licensed under the [Creative Commons BY license](https://creativecommons.org/licenses/by/4.0/). You have the permission to use, distribute, and reproduce in any medium, provided the source and authors are credited.


## Authors

This data has been collected, aggregated, and documented by Diana Beltekian, Daniel Gavrilov, Charlie Giattino, Joe Hasell, Bobbie Macdonald, Edouard Mathieu, Esteban Ortiz-Ospina, Hannah Ritchie, Max Roser.

The mission of _Our World in Data_ is to make data and research on the world’s largest problems understandable and accessible. [Read more about our mission](https://ourworldindata.org/about).
