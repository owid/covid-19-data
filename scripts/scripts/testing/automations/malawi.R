date <- today() - 1

url <- sprintf(
    "https://malawipublichealth.org/index.php/resources/COVID-19 SitRep Updates/%s/%s_Malawi COVID-19 situation report.pdf/download",
    strftime(date, "%B %Y"),
    strftime(date, "%Y%m%d")
) %>% str_replace_all(" ", "%20")

download.file(url = url, destfile = "tmp/tmp.pdf", quiet = TRUE)

count <- pdf_text("tmp/tmp.pdf") %>%
    str_extract("[\\d,]+ tests conducted") %>%
    na.omit() %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Malawi",
    country = "Malawi",
    units = "samples tested",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Public Health Institute of Malawi"
)
