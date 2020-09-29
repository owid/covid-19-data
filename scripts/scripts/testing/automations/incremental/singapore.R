url <- "https://www.moh.gov.sg/covid-19"

page <- read_html(url)

date <- page %>% html_nodes("h3") %>% html_text()
date <- date[str_detect(date, "Tested")] %>%
    str_extract("as .. [^)]+") %>%
    str_replace("as .. ", "") %>%
    dmy()

count <- page %>%
    html_nodes("#ContentPlaceHolder_contentPlaceholder_C095_Col01 td") %>%
    html_text() %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Singapore - People tested",
    country = "Singapore",
    units = "people tested",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Ministry of Health"
)

count <- page %>%
    html_nodes("#ContentPlaceHolder_contentPlaceholder_C095_Col00 td") %>%
    html_text() %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Singapore - Swabs tested",
    country = "Singapore",
    units = "samples tested",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Ministry of Health"
)
