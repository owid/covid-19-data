# Data on excess mortality during the COVID-19 pandemic by Our World in Data

For more general information on our COVID-19 data, see our main README file in [`/public/data`](https://github.com/owid/covid-19-data/tree/master/public/data).

## Data sources

The data is from the [Human Mortality Database](https://www.mortality.org/) (HMD) Short-term Mortality Fluctuations project for all countries, except the United Kingdom (HMD has data for England & Wales, Scotland, and Northern Ireland separately, but not the UK as a whole). The UK data is sourced from the [UK Office for National Statistics](https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/articles/comparisonsofallcausemortalitybetweeneuropeancountriesandregions/januarytojune2020) (ONS).

For a more detailed description of the HMD data, including week date definitions, the coverage (of individuals, locations, and time), whether dates are for death occurrence or registration, the original national source information, and important caveats, [see the HMD metadata file](https://www.mortality.org/Public/STMF_DOC/STMFmetadata.pdf).

For a more detailed description of the UK ONS data, [see the full report](https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/articles/comparisonsofallcausemortalitybetweeneuropeancountriesandregions/januarytojune2020).

## Excess mortality data

Stored in [`excess_mortality.csv`](https://github.com/owid/covid-19-data/blob/master/public/data/excess_mortality/excess_mortality.csv).

As of 26 January 2021, the data columns are:

- `location`: name of the country
- `date`: date on which a week ended according to ISO 8601; see note below for details
- `p_scores_all_ages`: P-scores for all ages; see note below for the definition of the P-score and how we calculate it
- `p_scores_15_64`: P-scores for ages 15–64
- `p_scores_65_74`: P-scores for ages 65–74
- `p_scores_75_84`: P-scores for ages 75–84
- `p_scores_85plus`: P-scores for ages 85 and above
- `deaths_2021_all_ages`: number of weekly deaths from all causes for all ages in 2021
- `deaths_2020_all_ages`: number of weekly deaths from all causes for all ages in 2020
- `avg_deaths_2015_2019`: average number of weekly deaths from all causes for all ages over the years 2015–2019
- `deaths_2019_all_ages`: number of weekly deaths from all causes for all ages in 2019
- `deaths_2018_all_ages`: number of weekly deaths from all causes for all ages in 2018
- `deaths_2017_all_ages`: number of weekly deaths from all causes for all ages in 2017
- `deaths_2016_all_ages`: number of weekly deaths from all causes for all ages in 2016
- `deaths_2015_all_ages`: number of weekly deaths from all causes for all ages in 2015
- `deaths_2014_all_ages`: number of weekly deaths from all causes for all ages in 2014
- `deaths_2013_all_ages`: number of weekly deaths from all causes for all ages in 2013
- `deaths_2012_all_ages`: number of weekly deaths from all causes for all ages in 2012
- `deaths_2011_all_ages`: number of weekly deaths from all causes for all ages in 2011
- `deaths_2010_all_ages`: number of weekly deaths from all causes for all ages in 2010
- `Week`: week number in the year; see note below for details

## Additional notes about the data

### Week numbering and dates

Death data is typically reported by countries on a weekly basis and numbered from Week 1 to Week 52 (or 53) over the course of a year. But countries define the start and end days of the week differently. Most countries use [international standard ISO 8601](https://en.wikipedia.org/wiki/ISO_week_date), which defines the week as from Monday to Sunday, but not all countries follow this standard. For instance, England & Wales define the week as from Saturday to Friday. This means the dates of any particular reporting week might differ slightly between countries. In our excess mortality data we use the ISO 8601 week end dates.

### How P-scores are defined and calculated

We used the raw weekly death data from HMD to calculate P-scores. The P-score is the percentage difference between the number of weekly deaths in 2020–2021 and the average number of deaths in the same week over the years 2015–2019 (when available; for a small minority of countries fewer previous years are available, usually 2016–2019). For the UK P-scores were calculated by the ONS.

For Week 53 2020, which ended on 3 January 2021, we compare the number of deaths to the average deaths in Week 52 over the years 2015–2019, because only one previous year (2015) had a Week 53.

### We exclude the most recent weeks of data because it is incomplete

We do not show the most recent weeks of countries’ data series. The decision about how many weeks to exclude is made individually for each country based on when the reported number of deaths in a given week changes by less than ~3% relative to the number previously reported for that week, implying that the reports have reached a high level of completeness. The exclusion of data based on this threshold varies from zero weeks (for countries that quickly reach a high level of reporting completeness) to four weeks. For a detailed list of the data we exclude for each country [see the spreadsheet here](https://docs.google.com/spreadsheets/d/1Z_mnVOvI9GVLiJRG1_3ond-Vs1GTseHVv1w-pF2o6Bs/edit?usp=sharing).
