url <- read_html("https://eody.gov.gr/epidimiologika-statistika-dedomena/ektheseis-covid-19/") %>%
    html_node(".main-container__inner .panel-body a") %>%
    html_attr("href") %>%
    read_html() %>%
    html_node("aside .custom-bullet-list a") %>%
    html_attr("href")

if (is.na(url)) {
    url <- read_html("https://eody.gov.gr/epidimiologika-statistika-dedomena/ektheseis-covid-19/") %>%
        html_node(".main-container__inner .panel-body a") %>%
        html_attr("href")
}

date <- str_extract(url, "2020\\d{4}") %>% ymd()
if (is.na(date)) date <- today()

download.file(url = url, destfile = "tmp/tmp.pdf", quiet = TRUE)

count <- pdf_text("tmp/tmp.pdf") %>%
    str_extract("έχουν.*\\d+.*δείγματα") %>%
    str_replace_all("[^\\d]", "") %>%
    na.omit() %>%
    as.integer()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Greece",
    country = "Greece",
    units = "samples tested",
    testing_type = "PCR only",
    source_url = url,
    source_label = "National Organization of Public Health"
)
