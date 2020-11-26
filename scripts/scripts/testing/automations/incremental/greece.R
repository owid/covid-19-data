date <- today() - 1

url <- sprintf("https://eody.gov.gr/wp-content/uploads/2020/11/covid-gr-daily-report-%s.pdf", format.Date(date, "%Y%m%d"))

download.file(url = url, destfile = "tmp/tmp.pdf", quiet = TRUE)

count <- pdf_text("tmp/tmp.pdf") %>%
    str_extract("ελεγχθεί \\d+ κλινικά") %>%
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
