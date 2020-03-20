# COVID-19 data

Collection of COVID-19 data & scripts used and maintained by [Our World in Data](https://ourworldindata.org/coronavirus).

**NOTE: We plan to keep the data up to date for our own purposes only. We may stop maintaining this repository at any point.**

Up until 17 March 2020, we were using WHO data manually extracted from their daily [situation report PDFs](https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports).

From 19 March 2020, we started relying on data published by the [European CDC](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide). We wrote about [why we decided to switch sources](https://ourworldindata.org/covid-sources-comparison).

### Data transformations

- We convert all country/region names to **Our World in Data standard entity names**.
- We may **correct or discard inconsistencies** that we detect in the original data. Check our [notes on ECDC data](input/ecdc/NOTES.md) for examples.

### Hosting

The `/public` path of this repository is hosted at `https://covid.ourworldindata.org/` using [Netlify](https://netlify.com).

We do our best to keep all past URLs working, however we may decide to stop maintaining data extracts at any point. The order and names of the CSV columns may also change.
