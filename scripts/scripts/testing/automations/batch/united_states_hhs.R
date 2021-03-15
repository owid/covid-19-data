# Test counts
# TODO need to move to the new healthdata.gov API as this is a legacy link which will only be supported for a few weeks more
meta <- fromJSON(file = "https://legacy.healthdata.gov/api/3/action/package_show?id=c13c00e3-f3d0-4d49-8c43-bf600a6c0a0d")
url <- meta$result[[1]]$resources[[1]]$url

tests <- fread(url, showProgress = FALSE, select = c("date", "new_results_reported"))
setnames(tests, "date", "Date")

tests <- tests[, .(`Daily change in cumulative total` = sum(new_results_reported)), Date]
tests[, Date := ymd(Date)]

# Case counts
cases <- jsonlite::fromJSON(txt = "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=us_trend_data")$us_trend_data
setDT(cases)
cases <- cases[state == "United States", c("seven_day_avg_new_cases", "date")]
setnames(cases, "date", "Date")
cases[, Date := mdy(Date)]

# Positive rate
df <- merge(tests, cases, by = "Date", all.x = TRUE)
setorder(df, Date)
df[, `Positive rate` := round(seven_day_avg_new_cases / frollmean(`Daily change in cumulative total`, 7), 3)]
df[, seven_day_avg_new_cases := NULL]

df[, Country := "United States"]
df[, Units := "tests performed"]
df[, `Source label` := "Department of Health & Human Services"]
df[, `Source URL` := "https://legacy.healthdata.gov/dataset/covid-19-diagnostic-laboratory-testing-pcr-testing-time-series"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/United States - tests performed.csv")
