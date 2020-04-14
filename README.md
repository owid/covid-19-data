# COVID-19 data

Collection of COVID-19 data & scripts used and maintained by [Our World in Data](https://ourworldindata.org/coronavirus).

**NOTE: We will continue to publish daily updates of confirmed cases and deaths throughout the duration of the pandemic.**

Up until 17 March 2020, we were using WHO data manually extracted from their daily [situation report PDFs](https://www.who.int/emergencies/diseases/novel-coronavirus-2019/situation-reports).

From 19 March 2020, we started relying on data published by the [European CDC](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide). We wrote about [why we decided to switch sources](https://ourworldindata.org/covid-sources-comparison).

### Data alterations

- We convert all country/region names to **Our World in Data standard entity names**.
- We may **correct or discard inconsistencies** that we detect in the original data. Check our [notes on ECDC data](input/ecdc/NOTES.md) for examples.

### Hosting

The `/public` path of this repository is hosted at `https://covid.ourworldindata.org/` using [Netlify](https://netlify.com).

We do our best to keep all past URLs working, however we may decide to stop maintaining data extracts at any point. The order and names of the CSV columns may also change.

## Update instructions

### Prerequisites

1. You must have **Python 3**.

   Check whether you do by running `python3 --version`.

2. You must have **pip** installed.

   Check whether you do by running `pip3 --version`.

3. Install **virtualenv** (if you don't have it):

   ```sh
   pip3 install virtualenv
   ```

4. Create a **virtual environment** for the project (run at the root of this repository):

   ```sh
   virtualenv venv
   ```

5. Activate the virtual environment:

   ```sh
   source venv/bin/activate
   ```

6. Install all dependencies:

   ```sh
   pip3 install -r requirements.txt
   ```

### ECDC data

To download the latest data and update the CSVs, run:

```sh
python3 scripts/ecdc.py
```

You will be asked for the release you want to use. The script should automatically download any new releases from today and yesterday, you will only need to select which one you want when prompted.

---

If the **automatic download doesn't seem to work**, then:

1. Download the release manually from [**the ECDC website**](https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide)

2. Move it to the `/input/ecdc/releases` folder in this repository and rename it to the date it refers to in ISO date format (e.g. `2020-03-20`).

3. Run the script again: `python3 scripts/ecdc.py`

---

If you get an error saying **some entities are missing Our World in Data names**, then:

1. There should be a file at `/tmp/ecdc.csv` in the repository. Upload this file to the **Country name standardizer** of the Our World in Data [Grapher](https://github.com/owid/owid-grapher).

2. Make sure to create a name for every single entity.

3. Download the standardized `ecdc_country_standardized.csv` file and move it to `/input/ecdc/ecdc_country_standardized.csv` in this repository.

4. Run the script again: `python3 scripts/ecdc.py`

---

If you get **an error unrelated to the above**, something might have changed with the dataset and you will need to change the script, or something isn't installed correctly.
