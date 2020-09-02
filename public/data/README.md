# Data on COVID-19 (coronavirus) by _Our World in Data_

Our complete COVID-19 dataset is a collection of the COVID-19 data maintained by [_Our World in Data_](https://ourworldindata.org/coronavirus). It is updated daily and includes data on confirmed cases, deaths, and testing, as well as other variables of potential interest.

### 🗂️ Download our complete COVID-19 dataset : [CSV](https://covid.ourworldindata.org/data/owid-covid-data.csv) | [XLSX](https://covid.ourworldindata.org/data/owid-covid-data.xlsx) | [JSON](https://covid.ourworldindata.org/data/owid-covid-data.json)

We will continue to publish up-to-date data on confirmed cases, deaths, and testing, throughout the duration of the COVID-19 pandemic.


## Our data sources

- **Confirmed cases and deaths:** our data comes from the [European Centre for Disease Prevention and Control](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide) (ECDC). We discuss how and when the ECDC collects and publishes this data [here](https://ourworldindata.org/coronavirus-source-data). The cases & deaths dataset is updated daily. *Note: the number of cases or deaths reported by any institution—including the ECDC, the WHO, Johns Hopkins and others—on a given day does not necessarily represent the actual number on that date. This is because of the long reporting chain that exists between a new case/death and its inclusion in statistics. **This also means that negative values in cases and deaths can sometimes appear when a country sends a correction to the ECDC, because it had previously overestimated the number of cases/deaths. Alternatively, large changes can sometimes (although rarely) be made to a country's entire time series if the ECDC decides (and has access to the necessary data) to correct values retrospectively.***
- **Testing for COVID-19:** this data is collected by the _Our World in Data_ team from official reports; you can find further details in our post on COVID-19 testing, including our [checklist of questions to understand testing data](https://ourworldindata.org/coronavirus-testing#our-checklist-for-covid-19-testing-data), information on [geographical and temporal coverage](https://ourworldindata.org/coronavirus-testing#which-countries-do-we-have-testing-data-for), and [detailed country-by-country source information](https://ourworldindata.org/coronavirus-testing#our-checklist-for-covid-19-testing-data). The testing dataset is updated around twice a week.
- **Other variables:** this data is collected from a variety of sources (United Nations, World Bank, Global Burden of Disease, Blavatnik School of Government, etc.). More information is available in [our codebook](https://github.com/owid/covid-19-data/tree/master/public/data/owid-covid-codebook.csv).


## The complete _Our World in Data_ COVID-19 dataset

**Our complete COVID-19 dataset is available in [CSV](https://covid.ourworldindata.org/data/owid-covid-data.csv), [XLSX](https://covid.ourworldindata.org/data/owid-covid-data.xlsx), and [JSON](https://covid.ourworldindata.org/data/owid-covid-data.json) formats, and includes all of our historical data on the pandemic up to the date of publication.**

The CSV and XLSX files follow a format of 1 row per location and date. The JSON version is split by country ISO code, with static variables and an array of daily records.

The variables represent all of our main data related to confirmed cases, deaths, and testing, as well as other variables of potential interest.

As of 17 August 2020, the columns are: `iso_code`, `continent`, `location`, `date`, `total_cases`, `new_cases`, `new_cases_smoothed`, `total_deaths`, `new_deaths`, `new_deaths_smoothed`, `total_cases_per_million`, `new_cases_per_million`, `new_cases_smoothed_per_million`, `total_deaths_per_million`, `new_deaths_per_million`, `new_deaths_smoothed_per_million`, `total_tests`, `new_tests`, `new_tests_smoothed`, `total_tests_per_thousand`, `new_tests_per_thousand`, `new_tests_smoothed_per_thousand`, `tests_per_case`, `positive_rate`, `tests_units`, `stringency_index`, `population`, `population_density`, `median_age`, `aged_65_older`, `aged_70_older`, `gdp_per_capita`, `extreme_poverty`, `cardiovasc_death_rate`, `diabetes_prevalence`, `female_smokers`, `male_smokers`, `handwashing_facilities`, `hospital_beds_per_thousand`, `life_expectancy`

A [full codebook](https://github.com/owid/covid-19-data/tree/master/public/data/owid-covid-codebook.csv) is made available, with a description and source for each variable in the dataset.


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
- On 9 May 2020, we added new variables related to demographic, economic, and public health data to our complete dataset.
- On 19 May 2020, we added 2 variables related to testing: `new_tests_smoothed` and `new_tests_smoothed_per_thousand`. To generate them we assume that testing changed equally on a daily basis over any periods in which no data was reported (as not all countries report testing data on a daily basis). This produces a complete series of daily figures, which is then averaged over a rolling 7-day window.
- On 23 May 2020, we added a JSON version of our complete dataset.
- On 4 June 2020, we added a `continent` column to our complete dataset.
- On 1 July 2020, we changed the format of the JSON version of our complete dataset to normalize the data and reduce file size.
- On 4 August 2020, we added the `positive_rate` and `tests_per_case` columns to our complete dataset.
- On 7 August 2020, we transformed our markdown codebook to a CSV file to allow easier merging with the complete dataset.
- On 17 August 2020, we added 4 variables related to cases and deaths: `new_cases_smoothed`, `new_deaths_smoothed`, `new_cases_smoothed_per_million`, and `new_deaths_smoothed_per_million`. These metrics are averaged versions (over a rolling 7-day window) of the daily variables.


## Data alterations

- **We standardize names of countries and regions.** Since the names of countries and regions are different in different data sources, we standardize all names to the [_Our World in Data_ standard entity names](https://github.com/owid/covid-19-data/blob/master/public/data/ecdc/locations.csv).
- We may correct or discard inconsistencies that we detect in the original data.
- Testing data is collected from many different sources. A detailed documentation for each country is available in [our post on COVID-19 testing](https://ourworldindata.org/coronavirus-testing#source-information-country-by-country).
- Where we collect multiple time series for a given country in our testing data (for example: for the United States, we collect data from both the CDC, and the COVID Tracking Project), our complete COVID-19 dataset only includes the most complete, or, if equally complete, data on the number of people tested rather than the number of tests/samples/swabs processed. The list of 'secondary' test series (those removed) is located in [`scripts/input/owid/secondary_testing_series.csv`](https://github.com/owid/covid-19-data/blob/master/scripts/input/owid/secondary_testing_series.csv).


## Stable URLs

The `/public` path of this repository is hosted at `https://covid.ourworldindata.org/`. For example, you can access the CSV for the complete dataset at `https://covid.ourworldindata.org/data/owid-covid-data.csv`.

We have the goal to keep all stable URLs working, even when we have to restructure this repository. If you need regular updates, please consider using the `covid.ourworldindata.org` URLs rather than pointing to GitHub.


## License

All visualizations, data, and code produced by _Our World in Data_ are completely open access under the [Creative Commons BY license](https://creativecommons.org/licenses/by/4.0/). You have the permission to use, distribute, and reproduce these in any medium, provided the source and authors are credited.

The data produced by third parties and made available by _Our World in Data_ is subject to the license terms from the original third-party authors. We will always indicate the original source of the data in our database, and you should always check the license of any such third-party data before use.


## Authors

This data has been collected, aggregated, and documented by Diana Beltekian, Daniel Gavrilov, Charlie Giattino, Joe Hasell, Bobbie Macdonald, Edouard Mathieu, Esteban Ortiz-Ospina, Hannah Ritchie, Max Roser.

The mission of _Our World in Data_ is to make data and research on the world’s largest problems understandable and accessible. [Read more about our mission](https://ourworldindata.org/about).
