url <- read_html(GET("https://www.health.gov.au/resources/collections/coronavirus-covid-19-at-a-glance-infographic-collection")) %>%
    html_node("main .container .row .paragraphs-items a") %>%
    html_attr("href") %>%
    paste0("https://www.health.gov.au", .)

page <- read_html(GET(url))

url <- page %>%
    html_node("main .health-file a") %>%
    html_attr("href")

date <- url %>%
    str_extract("\\d+-[^\\d]+-202\\d") %>%
    dmy()

download.file(url = url, destfile = "tmp/tmp.pdf", quiet = TRUE, method = "curl")

regex <- "([\\d,]+\\s+)+Overseas acquired"

count <- pdf_text("tmp/tmp.pdf") %>%
    str_extract(regex) %>%
    str_extract_all("[\\d,]+") %>%
    unlist %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer() %>%
    max()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Australia",
    country = "Australia",
    units = "tests performed",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Australian Government Department of Health"
)
