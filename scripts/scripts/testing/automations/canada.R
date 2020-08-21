url <- "https://www.canada.ca/en/public-health/services/diseases/2019-novel-coronavirus-infection.html"

df <- read_html(url) %>%
    html_nodes("main .mwstransform table")

df <- df[[1]] %>%
    html_table()

count <- df$`Total number of patients tested in Canada` %>%
    str_replace_all(",", "") %>%
    as.integer()

add_snapshot(
    count = count,
    sheet_name = "Canada",
    country = "Canada",
    units = "people tested",
    source_url = url,
    source_label = "Government of Canada",
    testing_type = "PCR only"
)
