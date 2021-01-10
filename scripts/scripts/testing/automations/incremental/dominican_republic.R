url <- read_html(GET(
        "https://www.msp.gob.do/web/?page_id=6948#1586785071804-577a2da4-6f72",
        config(ssl_verifypeer=0)
    )) %>%
    html_node(".infobox a") %>%
    html_attr("href") %>%
    paste0("https://www.msp.gob.do", .) %>%
    str_squish()

download.file(url = url, destfile = "tmp/tmp.pdf", quiet = TRUE, method = "curl", extra = "-k")

date <- pdf_text("tmp/tmp.pdf") %>%
    str_extract("Boletín.*\n +[\\d/]{8,10}") %>%
    str_extract("[\\d/]{8,10}$") %>%
    dmy() %>%
    head(1)

count <- pdf_text("tmp/tmp.pdf") %>%
    str_extract("TALARIA\n +últimas 24 horas +últimas 24 horas\n +[\\d,]+") %>%
    na.omit() %>%
    str_extract("[\\d,]+$") %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Dominican Republic",
    country = "Dominican Republic",
    units = "samples tested",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Ministry of Public Health and Social Assistance"
)

file.remove("tmp/tmp.pdf")
