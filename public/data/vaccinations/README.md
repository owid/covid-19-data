# Data on COVID-19 (coronavirus) vaccinations by _Our World in Data_

For more general information on our COVID-19 data, see our main README file in [`/public/data`](https://github.com/owid/covid-19-data/tree/master/public/data).

#### [`locations.csv`](locations.csv)

* `location`: name of the country (or region within a country).
* `iso_code`: ISO 3166-1 alpha-3 – three-letter country codes.
* `vaccines`: list of vaccines administered in the country up to the current date.
* `last_observation_date`: date of the last observation in our data.
* `source_name`: name of our source for data collection.
* `source_website`: web location of our source. It can be a standard URL if numbers are consistently reported on a given page; otherwise it will be the source for the last data point.

#### [`vaccinations.csv`](vaccinations.csv)

* `location`: name of the country (or region within a country).
* `iso_code`: ISO 3166-1 alpha-3 – three-letter country codes.
* `date`: date of the observation.
* `total_vaccinations`: total number of vaccination doses administered. This is counted as a single dose, and may not equal the total number of people vaccinated, depending on the specific dose regime (e.g. people receive multiple doses). If a person receives one dose of the vaccine, this metric goes up by 1. If they receive a second dose, it goes up by 1 again.
* `total_vaccinations_per_hundred`: `total_vaccinations` per 100 people in the total population of the country.
* `daily_vaccinations`: new vaccinations per day (7-day smoothed). For countries that don't report data on a daily basis, we assume that vaccinations changed equally on a daily basis over any periods in which no data was reported. This produces a complete series of daily figures, which is then averaged over a rolling 7-day window.
* `daily_vaccinations_per_million`: `daily_vaccinations` per 1,000,000 people in the total population of the country.
