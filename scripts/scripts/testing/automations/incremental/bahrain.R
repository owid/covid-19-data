url <- "https://www.moh.gov.bh/COVID19"

count <- read_html(url) %>%
    html_node("#renderbody table th span") %>%
    html_text() %>%
    as.integer()

add_snapshot(
    count = count,
    sheet_name = "Bahrain",
    country = "Bahrain",
    units = "units unclear",
    source_url = url,
    source_label = "Ministry of Health",
    testing_type = "PCR only"
)
