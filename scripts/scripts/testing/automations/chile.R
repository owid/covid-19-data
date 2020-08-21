df <- fread(
    "https://github.com/jorgeperezrojas/covid19-data/raw/master/csv/notificaciones.csv",
    showProgress = FALSE
)

df[, Institucion := NULL]
df <- colSums(df)

df <- data.table (
    Date = mdy(names(df)),
    `Cumulative total` = unname(df),
    Country = "Chile",
    `Source URL` = "https://github.com/jorgeperezrojas/covid19-data",
    `Source label` = "Ministry of Health",
    Units = "tests performed",
    Notes = "Made available by Jorge Perez Rojas on Github",
    `Testing type` = "PCR only"
)

fwrite(df, "automated_sheets/Chile.csv")
