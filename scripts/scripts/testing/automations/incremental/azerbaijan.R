url <- "https://koronavirusinfo.az/az/page/statistika/azerbaycanda-cari-veziyyet"

count <- read_html(url) %>%
    html_nodes('.gray_little_statistic strong') %>%
    html_text() %>%
    gsub(pattern = ',', replacement = '') %>%
    as.integer() %>%
    magrittr::extract(6)

add_snapshot(
    count = count,
    sheet_name = "Azerbaijan",
    country = "Azerbaijan",
    units = "tests performed",
    source_url = url,
    source_label = "Cabinet of Ministers of Azerbaijan",
    testing_type = "PCR only"
)
