url <- "https://coronavirus.bg/"

page <- read_html(url)

values <- page %>%
    html_nodes(".statistics-container .statistics-value") %>%
    html_text() %>%
    as.integer()

labels <- page %>%
    html_nodes(".statistics-container .statistics-label") %>%
    html_text() %>%
    str_detect("Направени PCR тестове")

count <- values[labels]

add_snapshot(
    count = count,
    sheet_name = "Bulgaria",
    country = "Bulgaria",
    units = "tests performed",
    source_url = url,
    source_label = "Bulgaria COVID-10 Information Portal",
    testing_type = "PCR only"
)
