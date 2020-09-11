df <- fread("http://www.chp.gov.hk/files/misc/statistics_on_covid_19_testing_cumulative.csv",
            showProgress = FALSE, select = c("日期由 From Date", "日期至 To Date", "檢測數字 Number of tests"))

setnames(df, c("from", "Date", "change"))

df[, from := NULL]
df[, Date := dmy(Date)]

setorder(df, Date)
df[, `Cumulative total` := cumsum(change)]
df[, change := NULL]

df[, Country := "Hong Kong"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://data.gov.hk/en-data/dataset/hk-dh-chpsebcddr-novel-infectious-agent/resource/64674927-bed8-4db9-9f1a-6999733ff221"]
df[, `Source label` := "Department of Health"]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Hong Kong.csv")
