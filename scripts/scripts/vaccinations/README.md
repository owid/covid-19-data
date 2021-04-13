# Vaccination update automation

Vaccination data is updated on a daily basis. For some countries, the update is done by means of an automated process,
while others require from some manual work. To keep track of the currently automated processes check [this
table](automations/automation_state.csv) and the scripts [in this folder](automations/batch) and [this
folder](automations/incremental).


Once the automation is succesfully executed, the following files are updated:

- [`vaccinations.csv`](../../../public/data/vaccinations/vaccinations.csv): Table with vaccination updated vaccination data of all countries.
- [`vaccinations.json`](../../../public/data/vaccinations/vaccinations.json): Same as `vaccinations.csv` but in JSON format.
- [`country_data`](../../../public/data/vaccinations/country_data/): Contains individual country csv files with the
  updated vaccination data.
- [`locations.csv`](../../../public/data/vaccinations/locations.csv): Country data metadata.
- [`COVID-19 - Vaccinations.csv`](../../grapher/COVID-19%20-%20Vaccinations.csv): Vaccination data file for OWID grapher.
- [`vaccinations-by-manufacturer.csv`](../../../public/data/vaccinations/vaccinations-by-manufacturer.csv): Table with vaccination
  data by manufacturer.
- [`COVID-19 - Vaccinations by manufacturer.csv`](../../grapher/COVID-19%20-%20Vaccinations%20by%20manufacturer.csv):
  Vaccination data by manufacturer file for OWID grapher.

_You can find more information about these files [here](../../../public/data/vaccinations/README.md)_

## Update the data

To update the data make sure you follow

### 0. Dependencies

#### Python and R
Make sure you have a working environment with R and python installed. We recommend R >= 4.0.2 and Python >= 3.7.

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

For `google`-related fields, make sure to download valid OAuth JSON credentials file, as explained in [gsheets documentation](https://gsheets.readthedocs.io/en/stable/#quickstart).


### 1. Manual data updates

Check for new updates and manually add them in the internal Spreadsheet:
- See repo [pull requests](https://github.com/owid/covid-19-data/pulls) and [issues](https://github.com/owid/covid-19-data/issues).
- Look for new data based on previously used source urls.

### 2. Automated data updates
Run the following script:

```
$ python run_python_scripts.py
```

- This will run the scrips in [in this folder](automations/batch) and [this
folder](automations/incremental). It will generate individual country files and save them in
[`automations/output`](automations/output).
- Additionally, it also generates public country data in
  [`country_data`](../../../public/data/vaccinations/country_data/).
- Finally, it generates temporary files `vaccinations.preliminary.csv` and `metadata.preliminary.csv` which are later
  required by script `generate_dataset.R`.

**Note**: This step might crash for some countries, as the automation scripts might no longer (or temporary) be valid
(e.g. due to source web changes). Try to keep scripts up to date.

### 3. Dataset generation
Make sure you succesfully [configured your environment](#0.-dependencies).

Run the following script:

```
$ Rscript generate_dataset.R
```

### 4. Megafile generation
> BE CAUTIOUS: Only run this if you know what you are doing.

This will update the whole datasets, so only run this if you want to update them:

```
$ python ../megafile.py
```

**Note**: You can use [vax_update.sh.template](vax_update.sh.template) as an example of how to automate data update + git push.

## Add new automated process
- Create a script and place it in [`automations/batch`](automations/batch) or
[`automations/incremental`](automations/incremental) depending on whether it is an incremental or batch update (see [#250](https://github.com/owid/covid-19-data/issues/250)
for more details).
- Test that it is working.
- Issue a Pull Request and wait for a review.


## Automation no longer works

If you detect that an automation is no longer valid and the automation seems like not feasible at the moment:
- Delete the automation script.
- Set its state to `automated=FALSE` in the spreadsheet.
- Add new tab in spreadsheet to manually input the country data. Make sure to include historical data.
