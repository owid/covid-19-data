url <- "https://datawrapper.dwcdn.net/OmMQp/"

while (TRUE) {
    page <- read_html(url)
    equiv <- page %>%
        html_node("meta") %>%
        html_attr("http-equiv")

    if (any(equiv == "REFRESH") & !is.na(equiv)) {
        url <- page %>%
            html_node("meta") %>%
            html_attr("content") %>%
            str_extract("\\d+/$") %>%
            paste0("https://datawrapper.dwcdn.net/OmMQp/", .)
    } else {
        break
    }
}

data <- read_html(url) %>%
    html_nodes("script") %>%
    html_text() %>%
    str_extract("chartData.*isPreview") %>%
    na.omit() %>%
    str_split("n") %>%
    unlist %>%
    str_extract_all("\\d+")

d <- sapply(data, FUN = `[`, 1)
m <- sapply(data, FUN = `[`, 2)
y <- sapply(data, FUN = `[`, 3)
confirma <- sapply(data, FUN = `[`, 4) %>% as.integer()
descarga <- sapply(data, FUN = `[`, 5) %>% as.integer()
dates <- ymd(paste(y, m, d, sep = "-"), quiet = TRUE)

df <- data.table(
    Date = dates,
    Confirmed = confirma,
    Discarded = descarga,
    Country = "Costa Rica",
    Units = "people tested",
    `Source URL` = "https://observador.cr/covid19-estadisticas/",
    `Source label` = "Ministry of Health",
    Notes = NA_character_,
    `Testing type` = "includes non-PCR"
)

# Early data point reported by the official source but missing from the graph
# See https://owid.slack.com/archives/C011DSUBY6A/p1596231838050800
df[Date == "2020-03-25" & Discarded == 5, Discarded := 65]
df[Date == "2020-03-25" & Discarded == 5, `Source URL` := "https://www.ministeriodesalud.go.cr/sobre_ministerio/prensa/img_cvd/img_datos_marzo_2020_15.jpeg"]

df[, `Daily change in cumulative total` := Confirmed + Discarded]
df[, c("Confirmed", "Discarded") := NULL]
df <- df[!is.na(`Daily change in cumulative total`)]

stopifnot(nrow(df) > 0)

fwrite(df, "automated_sheets/Costa Rica.csv")
