url <- "https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Testzahlen-gesamt.xlsx?__blob=publicationFile"

df <- rio::import(url, sheet = "Testzahlen")
setDT(df)
df <- df[!is.na(`Anzahl Testungen`) & `Kalenderwoche 2020` != "Summe"]

# Date parsing will stop working in 2021
stopifnot(year(today()) == 2020)

df[, week_number := as.integer(str_extract(`Kalenderwoche 2020`, "\\d+"))]
df[, Date := ymd("2020-01-01")]
week(df$Date) <- df$week_number
df[, Date := Date + 4]

setorder(df, Date)
df[, `Cumulative total` := cumsum(`Anzahl Testungen`)]

df[, `Positive rate` := round(`Positiven-quote (%)` / 100, 3)]

df <- df[, c("Date", "Cumulative total", "Positive rate")]
df[, Country := "Germany"]
df[, Units := "tests performed"]
df[, `Source URL` := url]
df[, `Source label` := "Robert Koch Institut"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Germany.csv")
