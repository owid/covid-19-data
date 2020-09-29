df <- fread("https://onemocneni-aktualne.mzcr.cz/api/v1/covid-19/testy.csv",
            select = c("datum", "testy_celkem"), showProgress = FALSE)

setnames(df, c("datum", "testy_celkem"), c("Date", "Cumulative total"))

df <- df[, .(Date = min(Date)), `Cumulative total`]

df[, Country := "Czech Republic"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://onemocneni-aktualne.mzcr.cz/api/v1/covid-19/testy.csv"]
df[, `Source label` := "Ministry of Health"]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Czech Republic.csv")
