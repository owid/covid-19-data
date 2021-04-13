url <- "https://datos.ins.gob.pe/dataset/75f98ad5-4be5-4377-89b8-9fee4d88fc1d/resource/f2e841ad-e0f5-43df-b9d9-598c8822e8a4/download/pm02abril2021.zip"

process_file <- function(url) {
    filename <- str_extract(url, "[^/]+\\.zip$")
    local_path <- sprintf("tmp/%s", filename)
    if (!file.exists(local_path)) {
        download.file(url = url, destfile = local_path)
    }
    csv_filename <- unzip(local_path, list = TRUE)$Name[1]
    unzip(local_path, exdir = "tmp")
    df <- fread(sprintf("tmp/%s", csv_filename), showProgress = FALSE, select = c("FECHATOMAMUESTRA", "RESULTADO"))
    setnames(df, c("Date", "Result"))
    df[, Date := as.character(Date)]
    return(df)
}

data <- process_file(url)

data <- data[Date <= today() & Date >= "2020-01-01" & !is.na(Date)]

df <- data[, .(
    `Daily change in cumulative total` = .N,
    `Positive` = sum(Result == "POSITIVO")
), Date]

setorder(df, Date)
df[, `Positive rate` := round(frollsum(Positive, 7) / frollsum(`Daily change in cumulative total`, 7), 3)]
df[, Positive := NULL]

df[, Country := "Peru"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://datos.ins.gob.pe/dataset?q=%22pruebas+moleculares%22&organization=covid-19"]
df[, `Source label` := "National Institute of Health"]
df[, `Testing type` := "PCR only"]
df[, Notes := NA]

stopifnot(max(df$Date) > today() - days(14))

fwrite(df, "automated_sheets/Peru.csv")
