df <- fread("https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/testy.csv",
            select = c("datum", "kumulativni_pocet_testu"), showProgress = FALSE)

setnames(df, c("datum", "kumulativni_pocet_testu"), c("Date", "Cumulative total"))

df <- df[, .(Date = min(Date)), `Cumulative total`]

df[, Country := "Czechia"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://onemocneni-aktualne.mzcr.cz/api/v1/covid-19/testy.csv"]
df[, `Source label` := "Ministry of Health"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Czechia.csv")
