url <- "http://www.covid.gov.pk/"

count <- read_html(url) %>%
    html_nodes(".covid-statistics .container .row .text-center") %>%
    html_text()

count <- count[str_detect(str_to_lower(count), "total tests")] %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

add_snapshot(
    count = count,
    sheet_name = "Pakistan",
    country = "Pakistan",
    units = "tests performed",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Government of Pakistan"
)
