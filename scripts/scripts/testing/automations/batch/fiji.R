url <- "http://www.health.gov.fj/covid-19-updates/"

df <- read_html(url) %>%
    html_node(".entry-content table") %>%
    html_table() %>%
    data.table()

df[!str_detect(Date, "-21$"), Date := paste0(Date, "-20")]
df[, Date := dmy(Date)]
df[, `Tests per day` := NULL]
setnames(df, "Cumulative", "Cumulative total")

df <- df[!is.na(Date)]

setorder(df, "Cumulative total", "Date")
df <- df[, .SD[1], `Cumulative total`]
df <- df[, .SD[1], Date]

df[, Country := "Fiji"]
df[, Units := "tests performed"]
df[, `Testing type` := "PCR only"]
df[, `Source URL` := url]
df[, `Source label` := "Fiji Ministry of Health & Medical Services"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Fiji.csv")
