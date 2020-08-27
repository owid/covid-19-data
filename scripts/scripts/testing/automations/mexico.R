formatted_date <- format.Date(today() - 1, "%Y%m%d")

confirmados <- sprintf("https://coronavirus.gob.mx/datos/Downloads/Files/Casos_Diarios_Estado_Nacional_Confirmados_%s.csv", formatted_date)
negativos <- sprintf("https://coronavirus.gob.mx/datos/Downloads/Files/Casos_Diarios_Estado_Nacional_Negativos_%s.csv", formatted_date)

process_file <- function(url) {
    df <- fread(url, showProgress = FALSE)
    df <- df[nombre == "Nacional"] %>%
        gather(Date, Count, 4:ncol(df)) %>%
        data.table()
    df[, c("cve_ent", "poblacion", "nombre") := NULL]
    df[, Date := dmy(Date)]
    return(df)
}

confirmados <- process_file(confirmados)
negativos <- process_file(negativos)

df <- merge(confirmados, negativos, by = "Date", all = TRUE)
df[is.na(Count.x), Count.x := 0]
df[is.na(Count.y), Count.y := 0]
df[, `Daily change in cumulative total` := Count.x + Count.y]
df[, c("Count.x", "Count.y") := NULL]
df <- df[`Daily change in cumulative total` != 0]

# Because of the significant lag in test reporting in Mexico, we censor the last 5 days of data
# to avoid showing a large decrease in the number of tests
df <- head(df, -5)

df[, Country := "Mexico"]
df[, Units := "people tested"]
df[, `Source URL` := "https://datos.gob.mx/busca/dataset/informacion-referente-a-casos-covid-19-en-mexico"]
df[, `Source label` := "Health Secretary"]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Mexico.csv")
