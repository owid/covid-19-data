# Test counts
url <- "https://healthdata.gov/api/views/j8mb-icvb/rows.csv"
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
df[, `Source URL` := "https://healthdata.gov/dataset/COVID-19-Diagnostic-Laboratory-Testing-PCR-Testing/j8mb-icvb"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/United States - tests performed.csv")
