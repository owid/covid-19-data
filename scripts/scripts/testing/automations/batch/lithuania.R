df <- fread("https://opendata.arcgis.com/datasets/538b7bd574594daa86fefd16509cbc36_0.csv",
            select = c("test_performed_date", "tests_total_cumulative"), showProgress = FALSE)

setnames(df, "test_performed_date", "Date")

df <- df[, .(`Cumulative total` = sum(tests_total_cumulative)), Date]
df <- df[`Cumulative total` > 0]

df[, Date := date(ymd_hms(Date))]

df[, Country := "Lithuania"]
df[, Units := "tests performed"]
df[, `Source label` := "Government of Lithuania"]
df[, `Source URL` := "https://open-data-ls-osp-sdg.hub.arcgis.com/search?q=COVID19%20tyrimai"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Lithuania.csv")
