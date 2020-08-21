url <- "https://www.korona.gov.sk/"

page <- read_html(url)

count <- page %>%
    html_node(".govuk-grid-column-one-quarter h2") %>%
    html_text() %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

date <- page %>%
    html_node(".govuk-hint") %>%
    html_text() %>%
    str_replace("Aktualizované ", "") %>%
    dmy()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Slovakia",
    country = "Slovakia",
    units = "tests performed",
    testing_type = "PCR only",
    source_url = url,
    source_label = "National Health Information Centre"
)
