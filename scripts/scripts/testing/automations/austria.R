url <- "https://www.sozialministerium.at/Informationen-zum-Coronavirus/Neuartiges-Coronavirus-(2019-nCov).html"

count <- read_html(url) %>%
    html_node(".table-responsive table") %>%
    html_table(dec = ",")

row_n <- which(str_detect(count$Bundesland, "Testungen"))
col_n <- which(str_detect(names(count), "Österreich"))

count <- count[row_n, col_n] %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

add_snapshot(
    count = count,
    sheet_name = "Austria",
    country = "Austria",
    units = "tests performed",
    source_url = url,
    source_label = "Austrian Ministry for Health",
    testing_type = "PCR only"
)
