url <- "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/testy-pcr-antigenni.csv"

df <- fread(url, showProgress = FALSE,
            select = c("datum", "pocet_PCR_testy", "pocet_AG_testy", "incidence_pozitivni"))

setnames(df, c("Date", "pcr", "antigen", "positive"))

setorder(df, Date)

df[, `Daily change in cumulative total` := pcr + antigen]
df[, `Positive rate` := round(frollsum(positive, 7) / frollsum(`Daily change in cumulative total`, 7), 3)]
df[, `Cumulative total` := NA_integer_]

df <- df[, c("Date", "Daily change in cumulative total", "Positive rate", "Cumulative total")]

df[, Country := "Czechia"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19"]
df[, `Source label` := "Ministry of Health"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Czechia.csv")
