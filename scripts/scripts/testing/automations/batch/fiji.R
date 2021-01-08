url <- "http://www.health.gov.fj/covid-19-updates/"

df <- read_html(url) %>%
    html_node(".entry-content table") %>%
    html_table() %>%
    data.table()

start_2021_idx <- which(df$Date == "01-Jan")

df[, Date := paste(Date, "2020")]
df[start_2021_idx:nrow(df), Date := str_replace(Date, "2020$", "2021")]
df[, Date := dmy(Date)]
df[, `Tests per day` := NULL]
setnames(df, "Cumulative", "Cumulative total")

setorder(df, "Cumulative total", "Date")
df <- df[, .SD[1], `Cumulative total`]

df[, Country := "Fiji"]
df[, Units := "tests performed"]
df[, `Testing type` := "PCR only"]
df[, `Source URL` := url]
df[, `Source label` := "Fiji Ministry of Health & Medical Services"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Fiji.csv")
