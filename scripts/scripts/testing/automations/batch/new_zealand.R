url <- "https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/covid-19-data-and-statistics/covid-19-testing-data"

df <- read_html(url) %>%
    html_node("#tests-cumulative table") %>%
    html_table() %>%
    data.table() %>%
    tail(-1)

setnames(df, c("Total tests (cumulative)", "Tests per day"), c("Cumulative total", "Daily change in cumulative total"))

df[, `Cumulative total` := as.integer(str_replace_all(`Cumulative total`, "[^\\d]", ""))]
df[, `Daily change in cumulative total` := as.integer(str_replace_all(`Daily change in cumulative total`, "[^\\d]", ""))]

df[, Date := dmy(paste0(Date, "-2020"))]
df[, Country := "New Zealand"]
df[, Units := "tests performed"]
df[, `Source URL` := url]
df[, `Source label` := "Ministry of Health"]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/New Zealand.csv")
