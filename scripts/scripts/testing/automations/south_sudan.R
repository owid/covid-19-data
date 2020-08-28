url <- "https://moh.gov.ss/daily_updates.php"

page <- read_html(url)

url <- page %>%
    html_nodes(".page-content h5 a") %>%
    html_attr("href") %>%
    paste0("https://moh.gov.ss/", .)
url <- url[str_detect(url, "single_daily_report")][1] %>%
    str_replace_all(" ", "%20")

page <- read_html(url)

date <- page %>%
    html_node("table h2") %>%
    html_text() %>%
    str_extract("[\\d-]{10}") %>%
    dmy()

count <- (page %>%
    html_nodes("table th"))[2] %>%
    html_text() %>%
    as.integer()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "South Sudan",
    country = "South Sudan",
    units = "tests performed",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Ministry of Health"
)
