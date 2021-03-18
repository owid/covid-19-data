url <- "https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Testzahlen-gesamt.xlsx?__blob=publicationFile"

df <- rio::import(url, sheet = "1_Testzahlerfassung")
setDT(df)

df <- df[str_detect(Kalenderwoche, "^\\d+/\\d+\\*?$")]
df[, Kalenderwoche := str_replace_all(Kalenderwoche, "\\*", "")]
df[, week_number := as.integer(str_extract(Kalenderwoche, "^\\d+"))]
df[, year := as.integer(str_extract(Kalenderwoche, "\\d+$"))]
stopifnot(max(df$year) == 2021)
df[year == 2020, Date := ymd("2019-12-29") + week_number * 7]
df[year == 2021, Date := ymd("2021-01-03") + week_number * 7]

setorder(df, Date)
df[, `Cumulative total` := cumsum(`Anzahl Testungen`)]

df[, `Positive rate` := round(`Positivenanteil (%)` / 100, 3)]

df <- df[, c("Date", "Cumulative total", "Positive rate")]
df[, Country := "Germany"]
df[, Units := "tests performed"]
df[, `Source URL` := url]
df[, `Source label` := "Robert Koch Institut"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Germany.csv")
