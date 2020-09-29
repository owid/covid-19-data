release <- read_html("http://minzdrav.gov.by/ru/sobytiya/index.php") %>%
    html_nodes(".paragraph .news-list-page")

url <- release %>%
    html_node("a") %>%
    html_attr("href")

idx <- min(which(str_detect(url, "patsyent")))

release <- release[idx]

url <- release %>%
    html_node("a") %>%
    html_attr("href") %>%
    paste0("http://minzdrav.gov.by", .)

date <- release %>%
    html_node("header") %>%
    html_text() %>%
    dmy()

page <- read_html(url)

count <- page %>%
    html_node(".content") %>%
    html_text() %>%
    str_extract("Всего проведено [\\d ]+ (млн.? \\d+ тыс. \\d+ ?)?тест") %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

stopifnot(!is.na(count))

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Belarus",
    country = "Belarus",
    units = "tests performed",
    source_url = url,
    testing_type = "PCR only",
    source_label = "Belarus Ministry of Health"
)

