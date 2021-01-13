url <- "https://covid19-dashboard.ages.at/data/CovidFallzahlen.csv"

df <- fread(url, showProgress = FALSE,
            select = c("Meldedat", "TestGesamt", "Bundesland"))

df <- df[Bundesland == "Alle"]
df[, Bundesland := NULL]

df <- df[, sum(TestGesamt), Meldedat]

setnames(df, c("Date", "Cumulative total"))

df <- df[, .(Date = min(Date)), `Cumulative total`]

df[, Date := dmy(Date)]
df[, Country := "Austria"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://www.data.gv.at/katalog/dataset/846448a5-a26e-4297-ac08-ad7040af20f1"]
df[, `Source label` := "Federal Ministry for Social Affairs, Health, Care and Consumer Protection"]
df[, Notes := NA_character_]

setorder(df, -Date)

fwrite(df, "automated_sheets/Austria.csv")
