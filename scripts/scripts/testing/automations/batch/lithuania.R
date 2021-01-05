df <- fread("https://opendata.arcgis.com/datasets/07dce7d43ba04a5b93abbbbe1d20d9ea_0.csv",
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
