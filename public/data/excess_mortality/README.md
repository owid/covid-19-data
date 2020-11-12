# Excess Mortality Data – HMD (2020)

Data from the Human Mortality Database (HMD) Short-term Mortality Fluctuations project for all countries except the UK (HMD has data for England & Wales, Scotland, and N. Ireland but not the UK as a whole). UK data sourced from the UK Office for National Statistics (ONS).

We used the raw weekly death data from HMD to calculate P-scores. The P-score is the percentage difference between the number of weekly deaths in 2020 and the average number of deaths in the same week over the years 2015–2019. For the UK P-scores were calculated by the ONS.

We do not show the most recent weeks of countries’ data series. The decision about how many weeks to exclude is made individually for each country based on when the reported number of deaths in a given week changes by less than ~3% relative to the number previously reported for that week, implying that the reports have reached a high level of completeness. The exclusion of data based on this threshold varies from zero weeks (for countries that quickly reach a high level of reporting completeness) to four weeks. For a detailed list of the data we exclude for each country see this spreadsheet: https://docs.google.com/spreadsheets/d/1Z_mnVOvI9GVLiJRG1_3ond-Vs1GTseHVv1w-pF2o6Bs/edit?usp=sharing.

For a more detailed description of the HMD data, including reporting week date definitions, the coverage (of individuals, locations, and time), whether dates are for death occurrence or registration, the original national source information, and important caveats, see the HMD metadata file at https://www.mortality.org/Public/STMF_DOC/STMFmetadata.pdf.

For a more detailed description of the UK ONS data, see the full report at https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/articles/comparisonsofallcausemortalitybetweeneuropeancountriesandregions/januarytojune2020.