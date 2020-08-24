url <- "https://www.cdc.go.kr/board/board.es?mid=&bid=0030"

url <- read_html("https://www.cdc.go.kr/board/board.es?mid=&bid=0030") %>%
    html_nodes(".dbody .title a")

url <- (url %>% html_attr("href"))[str_detect(url %>% html_text, "The updates on COVID-19 in Korea")][1] %>%
    paste0("https://www.cdc.go.kr", .)

page <- read_html(url)

tables <- page %>%
    html_nodes("table")

table <- tables[str_detect(tables %>% html_text(), "Testing in progress")][2] %>%
    html_table(fill = TRUE)

table <- table[[1]] %>% data.table()
table <- table[str_detect(X1, "As of ")] %>%
    tail(1)

done <- table$X2 %>%
    str_extract("[\\d,]+") %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

pending <- table$X7 %>%
    str_extract("[\\d,]+") %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

count <- done - pending

date <- page %>%
    html_node(".head li b") %>%
    html_text() %>%
    ymd_hm() %>%
    date()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "South Korea",
    country = "South Korea",
    testing_type = "PCR only",
    units = "people tested",
    source_url = url,
    source_label = "South Korea CDC"
)
