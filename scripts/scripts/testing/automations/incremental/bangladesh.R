url <- "https://corona.gov.bd/lang/en"

page <- read_html(url)

count <- page %>%
    html_nodes(".live-update-box .live-update-box-wrap-h1") %>%
    tail(1) %>%
    html_text() %>%
    str_squish() %>%
    as.integer()

date <- page %>%
    html_node(".last-update") %>%
    html_text() %>%
    str_extract("[\\d-]{10}") %>%
    dmy()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Bangladesh",
    country = "Bangladesh",
    units = "samples tested",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Government of Bangladesh"
)
