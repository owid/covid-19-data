reports <- read_html("https://www.belarus.by/en/press-center/press-release/?page=1") %>%
    html_nodes(".news_text a") %>%
    html_attr("href") %>%
    paste0("https://www.belarus.by", .)

for (url in reports) {
    page <- read_html(url)

    content <- page %>%
        html_node(".ic") %>%
        html_text()

    count <- content %>%
        str_extract("Belarus (has )?performed [\\d,]+ tests") %>%
        str_replace_all("[^\\d]", "") %>%
        as.integer()
    if (!is.na(count)) break
}

date <- page %>%
    html_node(".pages_header_inner") %>%
    html_text() %>%
    dmy()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Belarus",
    country = "Belarus",
    units = "tests performed",
    source_url = url,
    testing_type = "PCR only",
    source_label = "Government of Belarus"
)

