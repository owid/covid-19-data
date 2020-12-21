url <- "https://covid19.gov.gr/"

page <- read_html(url)

elements <- page %>% html_nodes("section .elementor-widget-container")
idx <- which(html_text(elements) == "ΔΕΙΓΜΑΤΑ") - 1

count <- elements[idx] %>%
    html_text() %>%
    str_extract("[\\d\\.]+") %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

date <- page %>%
    html_nodes(".elementor-icon-list-text") %>%
    html_text() %>%
    str_extract("[\\d/]{10}") %>%
    na.omit() %>%
    dmy()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Greece",
    country = "Greece",
    units = "samples tested",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Government of Greece"
)
