# Data on COVID-19 (coronavirus) by _Our World in Data_

Our complete COVID-19 dataset is a collection of the COVID-19 data maintained by [_Our World in Data_](https://ourworldindata.org/coronavirus). It is updated daily and includes data on confirmed cases, deaths, hospitalizations, testing, and vaccinations as well as other variables of potential interest.

### 🗂️ Download our complete COVID-19 dataset : [CSV](https://covid.ourworldindata.org/data/owid-covid-data.csv) | [XLSX](https://covid.ourworldindata.org/data/owid-covid-data.xlsx) | [JSON](https://covid.ourworldindata.org/data/owid-covid-data.json)

We will continue to publish up-to-date data on confirmed cases, deaths, hospitalizations, testing, and vaccinations, throughout the duration of the COVID-19 pandemic.


## The data you find here and our data sources

- **Confirmed cases and deaths:** our data comes from the [COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University](https://github.com/CSSEGISandData/COVID-19) (JHU). We discuss how and when JHU collects and publishes this data [here](https://ourworldindata.org/coronavirus-source-data). The cases & deaths dataset is updated daily. *Note: the number of cases or deaths reported by any institution—including JHU, the WHO, the ECDC and others—on a given day does not necessarily represent the actual number on that date. This is because of the long reporting chain that exists between a new case/death and its inclusion in statistics. **This also means that negative values in cases and deaths can sometimes appear when a country corrects historical data, because it had previously overestimated the number of cases/deaths. Alternatively, large changes can sometimes (although rarely) be made to a country's entire time series if JHU decides (and has access to the necessary data) to correct values retrospectively.***
- **Hospitalizations and intensive care unit (ICU) admissions:** our data comes from the [European Centre for Disease Prevention and Control](https://www.ecdc.europa.eu/en/publications-data/download-data-hospital-and-icu-admission-rates-and-current-occupancy-covid-19) (ECDC) for a select number of European countries; the [government of the United Kingdom](https://coronavirus.data.gov.uk/details/healthcare); the [Department of Health & Human Services](https://healthdata.gov/dataset/covid-19-reported-patient-impact-and-hospital-capacity-state-timeseries) for the United States; the [COVID-19 Tracker](https://covid19tracker.ca/) for Canada. Unfortunately, we are unable to provide data on hospitalizations for other countries: there is currently no global, aggregated database on COVID-19 hospitalization, and our team at _Our World in Data_ does not have the capacity to build such a dataset.
- **Testing for COVID-19:** this data is collected by the _Our World in Data_ team from official reports; you can find further details in our post on COVID-19 testing, including our [checklist of questions to understand testing data](https://ourworldindata.org/coronavirus-testing#our-checklist-for-covid-19-testing-data), information on [geographical and temporal coverage](https://ourworldindata.org/coronavirus-testing#which-countries-do-we-have-testing-data-for), and [detailed country-by-country source information](https://ourworldindata.org/coronavirus-testing#our-checklist-for-covid-19-testing-data). The testing dataset is updated around twice a week.
- **Vaccinations against COVID-19:** this data is collected by the _Our World in Data_ team from official reports.
- **Other variables:** this data is collected from a variety of sources (United Nations, World Bank, Global Burden of Disease, Blavatnik School of Government, etc.). More information is available in [our codebook](https://github.com/owid/covid-19-data/tree/master/public/data/owid-covid-codebook.csv).


## The complete _Our World in Data_ COVID-19 dataset

**Our complete COVID-19 dataset is available in [CSV](https://covid.ourworldindata.org/data/owid-covid-data.csv), [XLSX](https://covid.ourworldindata.org/data/owid-covid-data.xlsx), and [JSON](https://covid.ourworldindata.org/data/owid-covid-data.json) formats, and includes all of our historical data on the pandemic up to the date of publication.**

The CSV and XLSX files follow a format of 1 row per location and date. The JSON version is split by country ISO code, with static variables and an array of daily records.

The variables represent all of our main data related to confirmed cases, deaths, hospitalizations, and testing, as well as other variables of potential interest.

As of 26 January 2021, the columns are: `iso_code`, `continent`, `location`, `date`, `total_cases`, `new_cases`, `new_cases_smoothed`, `total_deaths`, `new_deaths`, `new_deaths_smoothed`, `total_cases_per_million`, `new_cases_per_million`, `new_cases_smoothed_per_million`, `total_deaths_per_million`, `new_deaths_per_million`, `new_deaths_smoothed_per_million`, `reproduction_rate`, `icu_patients`, `icu_patients_per_million`, `hosp_patients`, `hosp_patients_per_million`, `weekly_icu_admissions`, `weekly_icu_admissions_per_million`, `weekly_hosp_admissions`, `weekly_hosp_admissions_per_million`, `total_tests`, `new_tests`, `total_tests_per_thousand`, `new_tests_per_thousand`, `new_tests_smoothed`, `new_tests_smoothed_per_thousand`, `positive_rate`, `tests_per_case`, `tests_units`, `total_vaccinations`, `people_vaccinated`, `people_fully_vaccinated`, `new_vaccinations`, `new_vaccinations_smoothed`, `total_vaccinations_per_hundred`, `people_vaccinated_per_hundred`, `people_fully_vaccinated_per_hundred`, `new_vaccinations_smoothed_per_million`, `stringency_index`, `population`, `population_density`, `median_age`, `aged_65_older`, `aged_70_older`, `gdp_per_capita`, `extreme_poverty`, `cardiovasc_death_rate`, `diabetes_prevalence`, `female_smokers`, `male_smokers`, `handwashing_facilities`, `hospital_beds_per_thousand`, `life_expectancy`, `human_development_index`

A [full codebook](https://github.com/owid/covid-19-data/tree/master/public/data/owid-covid-codebook.csv) is made available, with a description and source for each variable in the dataset.


## Additional files and information

If you are interested in the individual files that make up the complete dataset, or more detailed information, other files can be found in the subfolders:

- [`latest`](https://github.com/owid/covid-19-data/tree/master/public/data/latest): shortened version of our complete dataset with only the latest value for each location and metric (within a limit of 2 weeks in the past). This file is available in CSV, XLSX, and JSON formats.
- [`jhu`](https://github.com/owid/covid-19-data/tree/master/public/data/jhu): data from the COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University, related to confirmed cases and deaths;
- [`testing`](https://github.com/owid/covid-19-data/tree/master/public/data/testing): data from various official sources, related to COVID-19 tests performed in each country. This folder contains two files with more detailed information:
  - [`covid-testing-all-observations.csv`](https://github.com/owid/covid-19-data/blob/master/public/data/testing/covid-testing-all-observations.csv) includes, for each historical observation, the source of the individual data point, and sometimes notes on data collection;
  - [`covid-testing-latest-data-source-details.csv`](https://github.com/owid/covid-19-data/blob/master/public/data/testing/covid-testing-latest-data-source-details.csv) includes, for each country in our testing dataset, the latest figures and a detailed description of how the country’s data is collected;
- [`excess_mortality`](https://github.com/owid/covid-19-data/tree/master/public/data/excess_mortality): data on excess mortality during the pandemic, sourced from [the Human Mortality Database](https://www.mortality.org/) and [the UK Office for National Statistics](https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/articles/comparisonsofallcausemortalitybetweeneuropeancountriesandregions/januarytojune2020);
- [`vaccinations`](https://github.com/owid/covid-19-data/tree/master/public/data/vaccinations): data from various official sources, related to COVID-19 vaccinations in each country;
- [`who`](https://github.com/owid/covid-19-data/tree/master/public/data/who): data from the World Health Organization, related to confirmed cases and deaths—_we have stopped using and updating this data since 18 March 2020_;
- [`ecdc`](https://github.com/owid/covid-19-data/tree/master/public/data/ecdc): data from the European Centre for Disease Prevention and Control, related to confirmed cases and deaths—_we have stopped using and updating this data since 30 November 2020_.
- [`internal`](https://github.com/owid/covid-19-data/tree/master/public/data/internal): data extracts intended for internal use at _Our World in Data_. They may change or be deleted without notice so we discourage using them.


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
- On 10 September 2020, we added the `human_development_index` column to our complete dataset.
- On 14 October 2020, we added data on excess mortality during the pandemic in the `excess_mortality` folder, sourced from [the Human Mortality Database](https://www.mortality.org/) and [the UK Office for National Statistics](https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/articles/comparisonsofallcausemortalitybetweeneuropeancountriesandregions/januarytojune2020).
- On 29 October 2020, we added data on hospitalizations and intensive care unit (ICU) admissions, sourced from the [European Centre for Disease Prevention and Control](https://www.ecdc.europa.eu/en/publications-data/download-data-hospital-and-icu-admission-rates-and-current-occupancy-covid-19) (ECDC), who provide these statistics only for a select number of European countries, and update it on a weekly basis.
- On 10 November 2020, we added data on hospitalizations and intensive care unit (ICU) admissions for the United States, sourced from the [COVID Tracking Project](https://covidtracking.com/).
- On 13 November 2020, we added real-time estimates of the effective reproduction rate (R) of the virus, sourced from [Arroyo Marioli et al. (2020)](https://doi.org/10.2139/ssrn.3581633).
- On 30 November 2020, we changed our source for confirmed cases and deaths to the [COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University](https://github.com/CSSEGISandData/COVID-19). Our previous source for confirmed cases and deaths, the European Centre for Disease Prevention and Control (ECDC), [had announced in November 2020](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide) that it would switch from a daily to a weekly reporting schedule from December. _Our World in Data_ therefore had to transition away from the ECDC as a source to continue to provide daily updates of confirmed cases and deaths. The data last sourced from the ECDC remains available as an archive in the [`ecdc`](https://github.com/owid/covid-19-data/tree/master/public/data/ecdc) folder. The format (variable names and types) of our complete COVID-19 dataset remains the same.
- On 9 December 2020, we changed the names of three countries in our files to match their recently-changed official names. `Czech Republic` has become `Czechia`, `Macedonia` has become `North Macedonia`, and `Swaziland` has become `Eswatini`.
- On 16 December 2020, we started collecting country-level time series on COVID-19 vaccinations.
- On 18 December 2020, we added in the [`latest`](https://github.com/owid/covid-19-data/tree/master/public/data/latest) folder a shortened version of our complete dataset with only the latest value for each location and metric (within a limit of 2 weeks in the past). This file is available in CSV, XLSX, and JSON formats.
- On 19 December 2020, we added data on hospitalizations and intensive care unit (ICU) admissions for Canada, sourced from the [COVID-19 Tracker](https://covid19tracker.ca/).
- On 6 January 2021, we added two variables for daily vaccinations to our complete dataset.
- On 7 January 2021, we replaced the United Kingdom's hospital and ICU data previously gathered by the European CDC with [the official data published by the British government](https://coronavirus.data.gov.uk/details/healthcare).
- On 26 January 2021, we added 4 variables on people vaccinated & people fully vaccinated to our complete dataset.
- On 4 February 2021, we added rows for Africa, Asia, Europe, European Union, North America, Oceania, and South America to our complete dataset. The `iso_code` column for these rows starts with `OWID_`.
- On 5 March 2021, due to [the COVID Tracking Project's announcement](https://covidtracking.com/analysis-updates/covid-tracking-project-end-march-7) that their data collection effort would stop in March 2021, we transitioned to the [Department of Health & Human Services](https://healthdata.gov/dataset/covid-19-reported-patient-impact-and-hospital-capacity-state-timeseries) as our source for data on hospitalizations and ICU admissions in the United States.


## Data alterations

- The population estimates we use to calculate per-capita metrics are all based on the last revision of the [United Nations World Population Prospects](https://population.un.org/wpp/). The exact values can be viewed [here](https://github.com/owid/covid-19-data/blob/master/scripts/input/un/population_2020.csv).
- We standardize names of countries and regions. Since the names of countries and regions are different in different data sources, we standardize all names to the [_Our World in Data_ standard entity names](https://github.com/owid/covid-19-data/blob/master/public/data/jhu/locations.csv).
- We may correct or discard inconsistencies that we detect in the original data.
- Testing data is collected from many different sources. A detailed documentation for each country is available in [our post on COVID-19 testing](https://ourworldindata.org/coronavirus-testing#source-information-country-by-country).
- Where we collect multiple time series for a given country in our testing data (for example: for the United States, we collect data from both the CDC, and the COVID Tracking Project), our complete COVID-19 dataset only includes the most complete, or, if equally complete, data on the number of people tested rather than the number of tests/samples/swabs processed. The list of 'secondary' test series (those removed) is located in [`scripts/input/owid/secondary_testing_series.csv`](https://github.com/owid/covid-19-data/blob/master/scripts/input/owid/secondary_testing_series.csv).


## Stable URLs

The `/public` path of this repository is hosted at `https://covid.ourworldindata.org/`. For example, you can access the CSV for the complete dataset at `https://covid.ourworldindata.org/data/owid-covid-data.csv`.

We have the goal to keep all stable URLs working, even when we have to restructure this repository. If you need regular updates, please consider using the `covid.ourworldindata.org` URLs rather than pointing to GitHub.


## License

All visualizations, data, and code produced by _Our World in Data_ are completely open access under the [Creative Commons BY license](https://creativecommons.org/licenses/by/4.0/). You have the permission to use, distribute, and reproduce these in any medium, provided the source and authors are credited.

In the case of our testing dataset, please give the following citation:
> Hasell, J., Mathieu, E., Beltekian, D. _et al._ A cross-country database of COVID-19 testing. _Sci Data_ **7**, 345 (2020). [https://doi.org/10.1038/s41597-020-00688-8](https://doi.org/10.1038/s41597-020-00688-8)

The data produced by third parties and made available by _Our World in Data_ is subject to the license terms from the original third-party authors. We will always indicate the original source of the data in our database, and you should always check the license of any such third-party data before use.


## Authors

This data has been collected, aggregated, and documented by Cameron Appel, Diana Beltekian, Daniel Gavrilov, Charlie Giattino, Joe Hasell, Bobbie Macdonald, Edouard Mathieu, Esteban Ortiz-Ospina, Hannah Ritchie, Lucas Rodés-Guirao, Max Roser.

The mission of _Our World in Data_ is to make data and research on the world's largest problems understandable and accessible. [Read more about our mission](https://ourworldindata.org/about).
