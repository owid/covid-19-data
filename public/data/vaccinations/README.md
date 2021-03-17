# Data on COVID-19 (coronavirus) vaccinations by _Our World in Data_

For more general information on our COVID-19 data, see our main README file in [`/public/data`](https://github.com/owid/covid-19-data/tree/master/public/data).


## Global vaccination data

### Country metadata

Stored in [`locations.csv`](locations.csv)

* `location`: name of the country (or region within a country).
* `iso_code`: ISO 3166-1 alpha-3 – three-letter country codes.
* `vaccines`: list of vaccines administered in the country up to the current date.
* `last_observation_date`: date of the last observation in our data.
* `source_name`: name of our source for data collection.
* `source_website`: web location of our source. It can be a standard URL if numbers are consistently reported on a given page; otherwise it will be the source for the last data point.

### Vaccination data

Stored in [`vaccinations.csv`](vaccinations.csv) and [`vaccinations.json`](vaccinations.json). Country-by-country data on global COVID-19 vaccinations. We only rely on figures that are verifiable based on public official sources.

This dataset includes some subnational locations (England, Northern Ireland, Scotland, Wales, Northern Cyprus…) and international aggregates (World, continents, European Union…). They can be identified by their `iso_code` that starts with `OWID_`.

The population estimates we use to calculate per-capita metrics are all based on the last revision of the [United Nations World Population Prospects](https://population.un.org/wpp/). The exact values can be viewed [here](https://github.com/owid/covid-19-data/blob/master/scripts/input/un/population_2020.csv).

* `location`: name of the country (or region within a country).
* `iso_code`: ISO 3166-1 alpha-3 – three-letter country codes.
* `date`: date of the observation.
* `total_vaccinations`: total number of doses administered. This is counted as a single dose, and may not equal the total number of people vaccinated, depending on the specific dose regime (e.g. people receive multiple doses). If a person receives one dose of the vaccine, this metric goes up by 1. If they receive a second dose, it goes up by 1 again.
* `total_vaccinations_per_hundred`: `total_vaccinations` per 100 people in the total population of the country.
* `daily_vaccinations_raw`: daily change in the total number of doses administered. It is only calculated for consecutive days. This is a raw measure provided for data checks and transparency, but we strongly recommend that any analysis on daily vaccination rates be conducted using `daily_vaccinations` instead.
* `daily_vaccinations`: new doses administered per day (7-day smoothed). For countries that don't report data on a daily basis, we assume that doses changed equally on a daily basis over any periods in which no data was reported. This produces a complete series of daily figures, which is then averaged over a rolling 7-day window. An example of how we perform this calculation can be found [here](https://github.com/owid/covid-19-data/issues/333#issuecomment-763015298).
* `daily_vaccinations_per_million`: `daily_vaccinations` per 1,000,000 people in the total population of the country.
* `people_vaccinated`: total number of people who received at least one vaccine dose. If a person receives the first dose of a 2-dose vaccine, this metric goes up by 1. If they receive the second dose, the metric stays the same.
* `people_vaccinated_per_hundred`: `people_vaccinated` per 100 people in the total population of the country.
* `people_fully_vaccinated`: total number of people who received all doses prescribed by the vaccination protocol. If a person receives the first dose of a 2-dose vaccine, this metric stays the same. If they receive the second dose, the metric goes up by 1.
* `people_fully_vaccinated_per_hundred`: `people_fully_vaccinated` per 100 people in the total population of the country.

Note: for `people_vaccinated` and `people_fully_vaccinated` we are dependent on the necessary data being made available, so we may not be able to make these metrics available for some countries.



## United States vaccination data

Stored in [`us_state_vaccinations.csv`](us_state_vaccinations.csv). State-by-state data on United States COVID-19 vaccinations. We rely on the data updated daily by the [United States Centers for Disease Control and Prevention](https://covid.cdc.gov/covid-data-tracker/#vaccinations).

* `location`: name of the state or federal entity.
* `date`: date of the observation.
* `total_vaccinations`: total number of doses administered. This is counted as a single dose, and may not equal the total number of people vaccinated, depending on the specific dose regime (e.g. people receive multiple doses). If a person receives one dose of the vaccine, this metric goes up by 1. If they receive a second dose, it goes up by 1 again.
* `total_vaccinations_per_hundred`: `total_vaccinations` per 100 people in the total population of the state.
* `daily_vaccinations_raw`: daily change in the total number of doses administered. It is only calculated for consecutive days. This is a raw measure provided for data checks and transparency, but we strongly recommend that any analysis on daily vaccination rates be conducted using `daily_vaccinations` instead.
* `daily_vaccinations`: new doses administered per day (7-day smoothed). For countries that don't report data on a daily basis, we assume that doses changed equally on a daily basis over any periods in which no data was reported. This produces a complete series of daily figures, which is then averaged over a rolling 7-day window. An example of how we perform this calculation can be found [here](https://github.com/owid/covid-19-data/issues/333#issuecomment-763015298).
* `daily_vaccinations_per_million`: `daily_vaccinations` per 1,000,000 people in the total population of the state.
* `people_vaccinated`: total number of people who received at least one vaccine dose. If a person receives the first dose of a 2-dose vaccine, this metric goes up by 1. If they receive the second dose, the metric stays the same.
* `people_vaccinated_per_hundred`: `people_vaccinated` per 100 people in the total population of the state.
* `people_fully_vaccinated`: total number of people who received all doses prescribed by the vaccination protocol. If a person receives the first dose of a 2-dose vaccine, this metric stays the same. If they receive the second dose, the metric goes up by 1.
* `people_fully_vaccinated_per_hundred`: `people_fully_vaccinated` per 100 people in the total population of the state.
* `total_distributed`: cumulative counts of COVID-19 vaccine doses recorded as shipped in CDC's Vaccine Tracking System.
* `total_distributed_per_hundred`: cumulative counts of COVID-19 vaccine doses recorded as shipped in CDC's Vaccine Tracking System per 100 people in the total population of the state.
* `share_doses_used`: share of vaccination doses administered among those recorded as shipped in CDC's Vaccine Tracking System.


## An example of how we calculate our metrics

4 people take part in a vaccination program, to be given a vaccine that requires 2 doses to be effective against the disease.

* Dina has received 2 doses;
* Joel has received 1 dose;
* Tommy has received 1 dose;
* Ellie has not received any dose.

In our data:

* The total number of doses administered (`total_vaccinations`) will be equal to `4` (2 + 1 + 1);
* The total number of people vaccinated (`people_vaccinated`) will be equal to `3` (Dina, Joel, Tommy);
* The total number of people fully vaccinated (`people_fully_vaccinated`) will be equal to `1` (Dina).
