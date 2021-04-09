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

#### Install python requirements
Run in your environment (shell):

```
$ pip install -r automations/requirements.txt
```

#### Install R requirements
Run the following within R environment:

```r
install.packages(c("data.table", "googlesheets4", "imputeTS", "lubridate", "readr", "retry", "rjson", "stringr", "tidyr", "jsonlite"))
```

#### Configuration JSON file `vax_dataset_config.json` (internal)

```json
{
    "greece_api_token": [GREECE_API_TOKEN],
    "google_credentials_email": [MAIL],
    "vax_time_series_gsheet": [SPREADHSEET_URL]
}
```

Note that your mail needs to have access to the internal spreadsheet. This is currently restricted to OWID members.

### 1. Manual data updates

Check for new updates and add them in the internal Spreadsheet:
- See repo pull requests and issues.
- Based on previous source urls look for data.

### 2. Automated data updates
Run the following script:

```
$ python run_python_scripts.py
```

This will run the scrips in [in this folder](automations/batch) and [this
folder](automations/incremental). It will generate individual country files and save them in
[`automations/output`](automations/output).

**Note**: This step might crash for some countries, as the automation scripts might no longer (or temporary) be valid
(e.g. due to source web changes). Try to keep scripts up to date.

### 3. Dataset generation

```
$ Rscript generate_dataset.R
```

### 4. Megafile generation


## Add new automated process
- Create a script and place it in [`automations/batch`](automations/batch) or
[`automations/incremental`](automations/incremental) depending on whether it is an incremental or batch update (see #250
for more details).
- Test that it is working.
- Issue a Pull Request and wait for a review.


## Automation no longer works

If you detect that an automation is no longer valid and the automation seems like not feasible at the moment:
- Delete the automation script.
- Set its state to `automated=FALSE` in the spreadsheet.
- Add new tab in spreadsheet to manually input the country data. Make sure to include historical data.
