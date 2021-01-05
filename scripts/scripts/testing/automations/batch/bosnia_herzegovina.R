url <- "http://mcp.gov.ba/publication/read/epidemioloska-slika-covid-19?pageId=3"

page <- read_html(url)

tables <- page %>%
    html_nodes("#newsContent table")

process_table <- function(table) {
    date <- table %>%
        html_node("tr td p") %>%
        html_text() %>%
        dmy()

    df <- table %>%
        html_table(trim = TRUE) %>%
        data.table()

    setnames(df, c("location", unlist(df[2, 2:ncol(df)])))

    count <- df[location == "BiH", `Broj testiranih`] %>%
        str_replace_all("[\\* ]", "") %>%
        as.integer()

    return(data.table(`Date` = date, `Cumulative total` = count))
}

df <- rbindlist(lapply(tables, FUN = process_table))
df <- df[!is.na(`Cumulative total`)]

df[, Country := "Bosnia and Herzegovina"]
df[, `Source URL` := url]
df[, `Source label` := "Ministry of Civil Affairs"]
df[, Units := "tests performed"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Bosnia and Herzegovina.csv")
