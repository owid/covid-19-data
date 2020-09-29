url <- "http://sam.lrv.lt/lt/naujienos/koronavirusas"

count <- read_html(url) %>%
    html_nodes(".text") %>%
    html_text()

count <- str_extract(count, "Iki šiol iš viso ištirta ėminių dėl įtariamo koronaviruso:\\s+\\d+") %>%
    str_extract(":\\s+\\d+") %>%
    str_extract("\\d+") %>%
    as.integer()

add_snapshot(
    count = count,
    sheet_name = "Lithuania",
    country = "Lithuania",
    units = "samples tested",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Ministry of Health"
)
