date <- Sys.Date() - 3

url <- sprintf(
    "https://geovision.uned.ac.cr/oges/archivos_covid/%s/%s_CSV_GENERAL.csv",
    format(date, "%Y_%m_%d"),
    format(date, "%m_%d_%y")
)

df <- fread(url, showProgress = FALSE,
            select = c("nue_posi", "conf_nexo","nue_descar", "FECHA"))

df[, lab_pos := zoo::na.fill(nue_posi, 0) - zoo::na.fill(conf_nexo, 0)]
df[, sum := zoo::na.fill(lab_pos, 0) + zoo::na.fill(nue_descar, 0)]

df[, `Positive rate` := round(frollsum(lab_pos, 7) / frollsum(sum, 7), 3)]

df <- df[, c("sum", "FECHA", "Positive rate")]
df <- df[sum != 0]

setnames(df, c("Daily change in cumulative total", "Date", "Positive rate"))

df[, Date := dmy(Date)]
df[, Country := "Costa Rica"]
df[, Units := "people tested"]
df[, `Source URL` := "https://geovision.uned.ac.cr/oges/"]
df[, `Source label` := "Ministry of Health"]
df[, Notes := NA_character_]

setorder(df, -Date)

fwrite(df, "automated_sheets/Costa Rica.csv")
