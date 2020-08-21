url <- "https://www.cdc.gov/coronavirus/2019-ncov/cases-updates/testing-in-us.html"

page <- read_html(url)

count <- page %>%
    html_node(".content table") %>%
    html_table() %>%
    unlist %>%
    tail(1) %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

date <- page %>%
    html_node(".last-updated") %>%
    html_text() %>%
    str_replace("Updated ", "") %>%
    mdy()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "United States - CDC",
    country = "United States",
    units = "tests performed (CDC)",
    testing_type = "includes non-PCR",
    source_url = url,
    source_label = "United States CDC"
)
