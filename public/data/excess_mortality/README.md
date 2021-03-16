# Data on excess mortality during the COVID-19 pandemic by Our World in Data

For more general information on our COVID-19 data, see our main README file in [`/public/data`](https://github.com/owid/covid-19-data/tree/master/public/data).

## Data sources

The all-cause mortality data is from the [Human Mortality Database](https://www.mortality.org/) (HMD) Short-term Mortality Fluctuations project and the [World Mortality Dataset](https://github.com/akarlinsky/world_mortality) (WMD). Both sources are updated weekly.

WMD sources some of its data from HMD, but we use the data from HMD directly. We do not use the data from some countries in WMD because they fail to meet the following data quality criteria: 1) at least four years of historical data; and 2) data published either weekly or monthly. The full list of excluded countries and reasons for exclusion can be found [in this spreadsheet](https://docs.google.com/spreadsheets/d/1JPMtzsx-smO3_K4ReK_HMeuVLEzVZ71qHghSuAfG788/edit?usp=sharing).

We calculate the number of weekly deaths for the United Kingdom by summing the weekly deaths from England & Wales, Scotland, and Northern Ireland.

For a more detailed description of the HMD data, including week date definitions, the coverage (of individuals, locations, and time), whether dates are for death occurrence or registration, the original national source information, and important caveats, [see the HMD metadata file](https://www.mortality.org/Public/STMF_DOC/STMFmetadata.pdf).

For a more detailed description of the WMD data, including original source information, [see their GitHub page](https://github.com/akarlinsky/world_mortality).

## Excess mortality data

Stored in [`excess_mortality.csv`](https://github.com/owid/covid-19-data/blob/master/public/data/excess_mortality/excess_mortality.csv).

As of 20 February 2021, the data columns are:

- `location`: name of the country or region
- `date`: date on which a month or week ended (week dates according to [ISO 8601](https://en.wikipedia.org/wiki/ISO_week_date))
- `p_scores_all_ages`: P-scores for all ages; see note below for the definition of the P-score and how we calculate it
- `p_scores_15_64`: P-scores for ages 15–64
- `p_scores_65_74`: P-scores for ages 65–74
- `p_scores_75_84`: P-scores for ages 75–84
- `p_scores_85plus`: P-scores for ages 85 and above
- `deaths_2021_all_ages`: number of weekly or monthly deaths from all causes for all ages in 2021
- `deaths_2020_all_ages`: number of weekly or monthly deaths from all causes for all ages in 2020
- `avg_deaths_2015_2019`: average number of weekly or monthly deaths from all causes for all ages over the years 2015–2019
- `deaths_2019_all_ages`: number of weekly or monthly deaths from all causes for all ages in 2019
- `deaths_2018_all_ages`: number of weekly or monthly deaths from all causes for all ages in 2018
- `deaths_2017_all_ages`: number of weekly or monthly deaths from all causes for all ages in 2017
- `deaths_2016_all_ages`: number of weekly or monthly deaths from all causes for all ages in 2016
- `deaths_2015_all_ages`: number of weekly or monthly deaths from all causes for all ages in 2015
- `deaths_2014_all_ages`: number of weekly or monthly deaths from all causes for all ages in 2014
- `deaths_2013_all_ages`: number of weekly or monthly deaths from all causes for all ages in 2013
- `deaths_2012_all_ages`: number of weekly or monthly deaths from all causes for all ages in 2012
- `deaths_2011_all_ages`: number of weekly or monthly deaths from all causes for all ages in 2011
- `deaths_2010_all_ages`: number of weekly or monthly deaths from all causes for all ages in 2010
- `time`: week or month number in the year
- `time_unit`: denotes whether the “time” column values are weekly or monthly

## How P-scores are defined and calculated

We used the raw weekly or monthly death data from HMD and WMD to calculate P-scores. The P-score is the percentage difference between the number of weekly or monthly deaths in 2020–2021 and the average number of deaths in the same period over the years 2015–2019 (for a small minority of countries only 2016–2019 are available). For Week 53 2020, which ended on 3 January 2021, we compare the number of deaths to the average deaths in Week 52 over the years 2015–2019, because only one previous year (2015) had a Week 53.

## Important points about the data

For more details see our page on [Excess mortality during the Coronavirus pandemic (COVID-19)](https://ourworldindata.org/excess-mortality-covid).

**The reported number of deaths might not count all deaths that occurred.** This is the case for two reasons:

- First, not all countries have the infrastructure and capacity to register and report all deaths. In richer countries with high-quality mortality reporting systems nearly 100% of deaths are registered, but in many low- and middle-income countries undercounting of mortality is a serious issue. The [UN estimates](https://unstats.un.org/unsd/demographic-social/crvs/#coverage) that only two-thirds of countries register at least 90% of all deaths that occur, and some countries register less than 50% — or [even under 10%](https://www.bbc.com/news/world-africa-55674139) — of deaths.
- Second, there are delays in death reporting that make mortality data provisional and incomplete in the weeks, months, and even years after a death occurs — even in richer countries with high-quality mortality reporting systems. The extent of the delay varies by country. For some, the most recent data points are clearly very incomplete and therefore inaccurate — we do not show these clearly incomplete data points. (For a detailed list of the data we exclude for each country [see this spreadsheet](https://docs.google.com/spreadsheets/d/1Z_mnVOvI9GVLiJRG1_3ond-Vs1GTseHVv1w-pF2o6Bs/edit?usp=sharing).)

**The date associated with a death might refer to when the death _occurred_ or to when it was _registered_.** This varies by country. Death counts by date of registration can vary significantly irrespectively of any actual variation in deaths, such as from registration delays or the closure of registration offices on weekends and holidays. It can also happen that deaths are registered, but the date of death is unknown — those deaths are not included in the weekly or monthly data here.

**The dates of any particular reporting week might differ slightly between countries.** This is because countries that report weekly data define the start and end days of the week differently. Most follow international standard [ISO 8601](https://en.wikipedia.org/wiki/ISO_week_date), which defines the week as from Monday to Sunday, but not all countries follow this standard. We use the ISO 8601 week end dates from 2020-2021.

**Deaths reported weekly might not be directly comparable to deaths reported monthly.** For instance, because excess mortality calculated from monthly data tends to be lower than the excess calculated from weekly data.
