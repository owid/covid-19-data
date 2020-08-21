url <- read_html("https://rospotrebnadzor.ru/about/info/news/") %>%
    html_nodes(".content .page a")

url <- url[str_detect(html_text(url), "Информационный бюллетень о ситуации")] %>%
    head(1) %>%
    html_attr("href") %>%
    paste0("https://rospotrebnadzor.ru", .)

page <- read_html(url)

count <- page %>%
    html_node(".news-detail") %>%
    html_text() %>%
    str_extract("проведено .* исследовани") %>%
    str_extract_all("[\\d,]+") %>%
    unlist() %>%
    str_replace_all(",", ".") %>%
    as.double()

mult_seq <- seq(from = 0, length.out = length(count))
mult_seq <- 1e6 / (1000^(mult_seq))

count <- as.integer(sum(count * mult_seq))

date <- page %>%
    html_node(".date") %>%
    html_text() %>%
    str_extract("[\\d.]+") %>%
    dmy() - 1

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Russia",
    country = "Russia",
    units = "tests performed",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Government of the Russian Federation"
)

