url <- "https://www.koronavirus.hr/najnovije/ukupno-dosad-382-zarazene-osobe-u-hrvatskoj/35"

page <- read_html(url)

count <- page %>%
    html_node(".page_content") %>%
    html_text() %>%
    str_extract("Do [^\\d]+testira[^\\d]+[\\d.]+") %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

date <- page %>%
    html_node(".time_info") %>%
    html_text() %>%
    str_extract("\\d.*2020") %>%
    dmy()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Croatia",
    country = "Croatia",
    units = "people tested",
    source_url = url,
    testing_type = "PCR only",
    source_label = "Government of Croatia"
)
