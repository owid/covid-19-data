url <- "https://e.infogram.com/deaf4fd6-0ce9-4b82-97ae-11e34a045060"
page <- read_html(url)

script <- page %>%
    html_nodes("script") %>%
    html_text()
script <- script[which(str_detect(script, "window\\.infographicData"))]

header_string <- '"Diagnostic test","Border screening 1 and 2","Quarantine and random screening","deCODE Genetics screening"'

graph <- script %>%
    str_replace_all("null", "0") %>%
    str_extract_all(sprintf('%s[^A-Za-z]+', header_string)) %>%
    unlist
graph <- graph[which(!str_detect(graph, "%"))]

data <- graph %>%
    str_extract_all('\\[[0-9"\\.,]+\\]') %>%
    unlist %>%
    str_replace_all('["\\[\\]]', '') %>%
    str_split(",")

dates <- sapply(data, "[", 1) %>%
    dmy()

df <- data.table(Date = dates, stringsAsFactors = FALSE)

categories <- header_string %>%
    str_extract_all("[A-Za-z1-2 ]+") %>%
    unlist

for (i in seq_along(categories)) {
    df[[categories[i]]] <- sapply(data, "[", i+1) %>% as.integer() %>% na_replace(0)
}

setDT(df)
df[, `Daily change in cumulative total` :=  `Diagnostic test` + `deCODE Genetics screening` + `Quarantine and random screening`]

df[, Country := "Iceland"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://www.covid.is/data"]
df[, `Source label` := "Government of Iceland"]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]
df[, c("Diagnostic test", "deCODE Genetics screening", "Border screening 1 and 2", "Quarantine and random screening") := NULL]

old <- fread("input/Iceland_old.csv")
old[, Date := ymd(Date)]

full <- rbindlist(list(old, df), use.names = TRUE, fill = FALSE)
setorder(full, -Date)

fwrite(full, "automated_sheets/Iceland.csv")
