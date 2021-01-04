url <- "http://ncov.mohw.go.kr/en/"

page <- read_html(url)

count <- page %>%
    html_nodes(".misil_r span") %>%
    html_text() %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()
count <- count[2]

date <- page %>%
    html_node(".m_inspect_status h3 em") %>%
    html_text() %>%
    str_extract(" on [A-Za-z]+ \\d+, 202\\d") %>%
    str_replace(" on ", "") %>%
    str_replace("Jaunary", "January") %>%
    mdy()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "South Korea",
    country = "South Korea",
    testing_type = "PCR only",
    units = "people tested",
    source_url = url,
    source_label = "Ministry of Health and Welfare"
)
