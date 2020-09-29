url <- "https://msan.gouvernement.lu/fr/graphiques-evolution.html"

tables <- read_html(url) %>%
    html_nodes(".chart-lines table")

tbl_idx <- which(tables %>% html_node("caption") %>% html_text() == "Tests COVID-19")

df <- tables[tbl_idx] %>%
    html_table(fill = TRUE)
df <- df[[1]]
setDT(df)
df <- df[, .(Date, `Nombre de personnes testées`)]

df[, Date := dmy(Date)]
df <- df[!is.na(Date)]
setnames(df, c("Date", "Cumulative total"))

setorder(df, Date)
df <- df[, .SD[1], `Cumulative total`]

df[, Country := "Luxembourg"]
df[, Units := "people tested"]
df[, `Testing type` := "PCR only"]
df[, `Source label` := "Luxembourg Government situation update"]
df[, `Source URL` := url]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Luxembourg.csv")
