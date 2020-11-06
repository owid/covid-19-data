url <- "https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/covid-19-data-and-statistics/covid-19-testing-data"

page <- read_html(url)

date <- page %>%
    html_node(".pane-content .georgia-italic") %>%
    html_text() %>%
    str_extract("\\d+ \\w+ 202\\d\\.$") %>%
    dmy()

df <- page %>%
    html_nodes("table")
df <- (df[str_detect(df %>% html_text(), "Testing results from ")] %>%
    html_table())[[1]] %>%
    data.table()

count <- df[`Test results` == "Total (all tests)", Total]

add_snapshot(
    count = count,
    date = date,
    sheet_name = "New Zealand",
    country = "New Zealand",
    units = "tests performed",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Ministry of Health"
)
