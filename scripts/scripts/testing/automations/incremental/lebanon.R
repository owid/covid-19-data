url <- "https://corona.ministryinfo.gov.lb/"

count <- read_html(url) %>%
    html_nodes('.counter-container') %>%
    html_nodes('h1.s-counter3') %>%
    html_text() %>%
    as.integer()

add_snapshot(
    count = count,
    sheet_name = "Lebanon",
    country = "Lebanon",
    units = "tests performed",
    source_url = url,
    source_label = "Lebanon Ministry of Health",
    testing_type = "PCR only"
)