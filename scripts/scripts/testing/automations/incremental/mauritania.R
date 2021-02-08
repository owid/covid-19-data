url <- read_html("https://www.sante.gov.mr/?cat=4&paged=1") %>%
    html_node(".post-title a") %>%
    html_attr("href")

process_article <- function(url) {
    url <- read_html(url) %>%
        html_node(".post-content a") %>%
        html_attr("href")

    if (is.na(url) | !str_detect(str_to_lower(url), "sitrep")) return(NULL)

    download.file(url = url, destfile = "tmp/tmp.pdf", quiet = TRUE)

    date <- NA
    if (str_detect(url, "\\d\\d-\\d\\d-202\\d")) date <- str_extract(url, "\\d\\d-\\d\\d-202\\d") %>% dmy()
    if (str_detect(url, "\\d{4}202\\d")) date <- str_extract(url, "\\d{4}202\\d") %>% dmy()
    if (str_detect(url, "/2\\d{5}-")) date <- str_extract(url, "2\\d{5}") %>% ymd()
    if (str_detect(url, "/\\d{4}2\\d-")) date <- str_extract(url, "/\\d{4}2\\d-") %>% dmy()
    if (str_detect(url, "/2\\d{6}-")) date <- str_extract(url, "2\\d{6}") %>% str_sub(c(1, 6), c(4, 7)) %>% str_flatten() %>% ymd()
    if (str_detect(url, "\\d+-\\d+-202\\d")) date <- str_extract(url, "\\d+-\\d+-202\\d") %>% dmy()
    stopifnot(!is.na(date))

    count <- pdf_text("tmp/tmp.pdf") %>%
        str_extract("[\\d ]+ ?tests ont été effectués") %>%
        str_replace_all("[^\\d]", "") %>%
        na.omit() %>%
        as.integer() %>%
        max()

    add_snapshot(
        count = count,
        date = date,
        sheet_name = "Mauritania",
        country = "Mauritania",
        units = "tests performed",
        testing_type = "unclear",
        source_url = url,
        source_label = "Ministry of Health"
    )
}

sapply(url, FUN=process_article)
