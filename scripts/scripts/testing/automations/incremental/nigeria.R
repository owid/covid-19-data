url <- "http://covid19.ncdc.gov.ng/"

page <- read_html(url)

count <- page %>%
    html_node(".page-block .col-xl-3 .card-body h2") %>%
    html_text() %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

add_snapshot(
    count = count,
    sheet_name = "Nigeria",
    country = "Nigeria",
    units = "samples tested",
    source_url = url,
    testing_type = "PCR only",
    source_label = "Nigeria Centre for Disease Control"
)
