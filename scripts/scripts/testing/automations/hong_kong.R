df <- fread("http://www.chp.gov.hk/files/misc/statistics_on_covid_19_testing_cumulative.csv",
            showProgress = FALSE)

setnames(df, c("from", "Date", "change"))

df[, from := NULL]
df[, Date := dmy(Date)]

setorder(df, Date)
df[, `Cumulative total` := cumsum(change)]
df[, change := NULL]

df[, Country := "Hong Kong"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://data.gov.hk/en-data/dataset/hk-dh-chpsebcddr-novel-infectious-agent/resource/000cc801-6294-4ea9-b505-f5f1633a53b9"]
df[, `Source label` := "Department of Health"]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Hong Kong.csv")
