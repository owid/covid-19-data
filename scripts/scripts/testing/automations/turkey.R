url <- "https://covid19.saglik.gov.tr/"

count <- read_html(url) %>%
    html_node(".corona-bg .row .col-sm-6 .list-group-genislik li") %>%
    html_text() %>%
    str_extract("[\\d\\.]+") %>%
    str_replace_all("\\.", "") %>%
    as.integer()

add_snapshot(
    count = count,
    sheet_name = "Turkey",
    country = "Turkey",
    units = "tests performed",
    source_url = url,
    testing_type = "PCR only",
    source_label = "Turkish Ministry of Health"
)
