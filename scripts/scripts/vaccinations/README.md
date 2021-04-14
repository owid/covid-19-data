# Vaccination update automation

Vaccination data is updated on a daily basis. For some countries, the update is done by means of an automated process,
while others require some manual work. To keep track of the currently automated processes, check [this
table](automations/automation_state.csv), and the scripts [in the `batch` folder](automations/batch) and [the `incremental`
folder](automations/incremental).

Once the automation is successfully executed, the following files are updated:

- [`vaccinations.csv`](../../../public/data/vaccinations/vaccinations.csv): main output with vaccination data of all countries.
- [`vaccinations.json`](../../../public/data/vaccinations/vaccinations.json): same as `vaccinations.csv` but in JSON format.
- [`country_data`](../../../public/data/vaccinations/country_data/): individual country CSV files.
- [`locations.csv`](../../../public/data/vaccinations/locations.csv): country-level metadata.
- [`vaccinations-by-manufacturer.csv`](../../../public/data/vaccinations/vaccinations-by-manufacturer.csv): secondary output with vaccination by manufacturer for a select number of countries.
- [`COVID-19 - Vaccinations.csv`](../../grapher/COVID-19%20-%20Vaccinations.csv): internal file for OWID grapher on vaccinations.
- [`COVID-19 - Vaccinations by manufacturer.csv`](../../grapher/COVID-19%20-%20Vaccinations%20by%20manufacturer.csv): internal file for OWID grapher on vaccinations by manufacturer.

_You can find more information about these files [here](../../../public/data/vaccinations/README.md)_.


## Update the data

To update the data, make sure you follow the steps below.


### 0. Dependencies


#### Python and R
Make sure you have a working environment with R and python 3 installed. We recommend R >= 4.0.2 and Python >= 3.7.

You can check:

```
$ python --version
```
and
```
$ R --version
```

#### Install python requirements
In your environment (shell), run:

```
$ pip install -r automations/requirements.txt
```

#### Install R requirements
In your R console, run:

```r
install.packages(c("data.table", "imputeTS", "lubridate", "readr", "retry", "rjson", "stringr", "tidyr", "jsonlite", "bit64"))
```

#### Configuration file (internal)

Create a file `vax_dataset_config.json` with all required parameters:

```json
{
    "greece_api_token": "[GREECE_API_TOKEN]",
    "owid_cloud_table_post": "[OWID_CLOUD_TABLE_POST]",
    "google_credentials": "[CREDENTIALS_JSON_PATH]",
    "google_spreadsheet_vax_id": "[SHEET_ID]"
}
```

For `google`-related fields, you'll need a valid OAuth JSON credentials file, as explained in the [gsheets documentation](https://gsheets.readthedocs.io/en/stable/#quickstart).


### 1. Manual data updates

Check for new updates and manually add them in the internal spreadsheet:
- See this repo's [pull requests](https://github.com/owid/covid-19-data/pulls) and [issues](https://github.com/owid/covid-19-data/issues).
- Look for new data based on previousl-used source URLs.


### 2. Automated data updates
Run the following script:

```
$ python run_python_scripts.py
```

- This will run the scrips in [in this folder](automations/batch) and [this
folder](automations/incremental). It will generate individual country files and save them in
[`automations/output`](automations/output).
- Additionally, it'll automatically run `collect_vax_data.py` which will generate public country data in
  [`country_data`](../../../public/data/vaccinations/country_data/), as well as temporary files 
  `vaccinations.preliminary.csv` and `metadata.preliminary.csv` which are later
  required by `generate_dataset.R`.

**Note**: this step might crash for some countries, as the automation scripts might no longer (or temporarily) work
(e.g. due to changes in the source). Try to keep the scripts up to date.


### 3. Dataset generation
Make sure you've succesfully [configured your environment](#0.-dependencies), then run the following script:

```
$ Rscript generate_dataset.R
```

Running this script in an interactive environment (typically RStudio) is recommended, as it'll make the
potential debugging process much easier.


### 4. Megafile generation

This will update the complete COVID dataset, which also includes all vaccination metrics:

```
$ python ../megafile.py
```

**Note**: you can use [vax_update.sh.template](vax_update.sh.template) as an example of how to automate data updates and push them to the repo.


## How to add new automated data collections
- Create a script and place it in [`automations/batch`](automations/batch) or
[`automations/incremental`](automations/incremental) depending, on whether it is an incremental or batch update (see [#250](https://github.com/owid/covid-19-data/issues/250)
for more details).
- Test that it is working and stable.
- Issue a pull request and wait for a review.


## If an automation no longer works

If you detect that an automation is no longer working, and the process seems like it can't be fixed at the moment:
- Set its state to `automated = FALSE` in the `LOCATIONS` tab of the internal spreadsheet.
- Add a new tab in the spreadsheet to manually input the country data. Make sure to include the historical data from the [`automations/output`](automations/output) file.
- Delete the automation script and automated CSV output to avoid confusion.
