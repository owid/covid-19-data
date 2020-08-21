df <- fread("https://epistat.sciensano.be/Data/COVID19BE_tests.csv", showProgress = FALSE)
setnames(df, c("DATE", "TESTS_ALL"), c("Date", "Daily change in cumulative total"))

df[, Country := "Belgium"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://epistat.sciensano.be/Data/COVID19BE_tests.csv"]
df[, `Source label` := "Sciensano (Belgian institute for health)"]
df[, Notes := NA_character_]
df[, `Testing type` := "unclear"]

df <- df[ymd(Date) <= today()]
df <- head(df, -1)

fwrite(df, "automated_sheets/Belgium.csv")
