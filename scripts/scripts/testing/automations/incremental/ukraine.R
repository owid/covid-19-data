url <- "https://covid19.gov.ua/en"

page <- read_html(url)

date <- page %>%
    html_node(".main-section .field-value p") %>%
    html_text() %>%
    str_replace("Information as of", "2020") %>%
    ymd()

count <-  page %>%
    html_nodes(".after-title .light-box .field-value") %>%
    html_text() %>%
    tail(1) %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Ukraine",
    country = "Ukraine",
    units = "tests performed",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Cabinet of Ministers of Ukraine"
)
