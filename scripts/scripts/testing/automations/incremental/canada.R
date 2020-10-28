url <- "https://www.canada.ca/en/public-health/services/diseases/2019-novel-coronavirus-infection.html"
page <- read_html(url)

date <- page %>%
    html_nodes("main .mwstransform time") %>%
    html_attr("datetime") %>%
    str_sub(1, 10) %>%
    ymd()

df <- page %>%
    html_node("main .mwstransform table") %>%
    html_table()

pos <- df$`Total positive` %>%
    str_replace_all(",", "") %>%
    as.integer()

neg <- df$`Total negative` %>%
    str_replace_all(",", "") %>%
    as.integer()

count <- pos + neg

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Canada",
    country = "Canada",
    units = "people tested",
    source_url = url,
    source_label = "Government of Canada",
    testing_type = "PCR only"
)
