formatted_date <- format.Date(today() - 1, "%Y%m%d")

confirmados <- sprintf("https://datos.covid-19.conacyt.mx/Downloads/Files/Casos_Diarios_Estado_Nacional_Confirmados_%s.csv", formatted_date)
negativos <- sprintf("https://datos.covid-19.conacyt.mx/Downloads/Files/Casos_Diarios_Estado_Nacional_Negativos_%s.csv", formatted_date)

process_file <- function(url, metric) {
    df <- fread(url, showProgress = FALSE)
    df <- df[nombre == "Nacional"] %>%
        gather(Date, Count, 4:ncol(df)) %>%
        data.table()
    df[, c("cve_ent", "poblacion", "nombre") := NULL]
    df[, Date := dmy(Date)]
    setnames(df, "Count", metric)
    return(df)
}

confirmados <- process_file(confirmados, "positive")
negativos <- process_file(negativos, "negative")

df <- merge(confirmados, negativos, by = "Date", all = TRUE)
df[is.na(positive), positive := 0]
df[is.na(negative), negative := 0]
df[, `Daily change in cumulative total` := positive + negative]

setorder(df, Date)
df[, `Positive rate` := round(frollsum(positive, 7) / frollsum(`Daily change in cumulative total`, 7), 3)]

df[, c("positive", "negative") := NULL]
df <- df[`Daily change in cumulative total` != 0]

df[, Country := "Mexico"]
df[, Units := "people tested"]
df[, `Source URL` := "https://datos.covid-19.conacyt.mx/#DownZCSV"]
df[, `Source label` := "Health Secretary"]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Mexico.csv")
