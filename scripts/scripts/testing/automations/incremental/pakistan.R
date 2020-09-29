url <- "http://www.covid.gov.pk/"
page <- read_html(url)

count <- page %>%
    html_nodes(".top-statistics .active .counter") %>%
    html_text() %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

date <- page %>%
    html_node(".update-time") %>%
    html_text() %>%
    str_extract("^.*2020") %>%
    dmy()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Pakistan",
    country = "Pakistan",
    units = "tests performed",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Government of Pakistan"
)
