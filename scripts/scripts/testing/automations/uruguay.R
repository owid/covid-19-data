url <- read_html("https://www.gub.uy/ministerio-salud-publica/comunicacion/noticias") %>%
    html_nodes(".List--media h3 a") %>%
    html_attr("href")

url <- url[str_detect(url, "informe-situacion-sobre-coronavirus")] %>%
    paste0("https://www.gub.uy", .) %>%
    head(1)

page <- read_html(url)

date <- page %>%
    html_node(".Page-date") %>%
    html_text() %>%
    dmy()

count <- page %>%
    html_nodes(".Page-document") %>%
    html_text() %>%
    str_extract("se han procesado[^a-zA-Z]+") %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Uruguay",
    country = "Uruguay",
    units = "tests performed",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Ministry of Public Health"
)

