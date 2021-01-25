url <- "https://datadashboardapi.health.gov.il/api/queries/testResultsPerDate"

df <- jsonlite::fromJSON(url) %>%
    data.table()

df <- df[, c("date", "amount")]
setnames(df, c("Date", "Daily change in cumulative total"))
df[, Date := str_sub(Date, 1, 10)]

df[, Country := "Israel"]
df[, Units := "tests performed"]
df[, `Source label` := "Israel Ministry of Health"]
df[, `Source URL` := url]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Israel.csv")
