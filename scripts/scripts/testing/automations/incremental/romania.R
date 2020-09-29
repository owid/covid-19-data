links <- read_html("https://gov.ro/ro/media/comunicate") %>%
    html_nodes(".DescriptionList h2 a") %>%
    html_attr("href")

url <- links[str_detect(links, "buletin")][1]

count <- read_html(url) %>%
    html_node(".pageDescription") %>%
    html_text() %>%
    str_extract("Până la această dată.*[\\d.]+.*teste") %>%
    str_extract("[\\d.]+") %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

add_snapshot(
    count = count,
    sheet_name = "Romania",
    country = "Romania",
    testing_type = "PCR only",
    units = "tests performed",
    source_url = url,
    source_label = "Ministry of Internal Affairs"
)
