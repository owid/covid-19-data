url <- "https://www.folkhalsomyndigheten.se/smittskydd-beredskap/utbrott/aktuella-utbrott/covid-19/statistik-och-analyser/antalet-testade-for-covid-19/"
page <- read_html(url)

df <- page %>%
    html_node("#content-primary table") %>%
    html_table() %>%
    data.table()
df[, Weekly := as.integer(str_replace_all(`NukleinsyrapåvisningGenomförda tester`, "\\s", ""))]

df <- df[str_detect(Vecka, "Vecka")]
df[, Vecka := as.integer(str_replace(Vecka, "Vecka ", ""))]
df <- df[Vecka < 52]
df[, Date := ymd("2021-01-03") + 7 * Vecka]

df <- df[, c("Date", "Weekly")]
setorder(df, Date)

split_week <- function(row_idx) {
    end_date <- df[row_idx, Date]
    weekly_tests <- df[row_idx, Weekly]
    start_date <- end_date - 6
    daily_tests <- round(weekly_tests / 7)
    dates <- seq.Date(start_date, end_date, by = "1 day")
    return(data.table(
        Date = dates,
        `Daily change in cumulative total` = daily_tests
    ))
}

df <- rbindlist(lapply(1:nrow(df), FUN = split_week))

df[, `Source URL` := url]
df[, Country := "Sweden"]
df[, Units := "tests performed"]
df[, `Source label` := "Swedish Public Health Agency"]
df[, Notes := NA]
df[, `Testing type` := "PCR only"]
df[, `Cumulative total` := NA_integer_]

existing <- fread("automated_sheets/Sweden.csv")
existing[, Date := ymd(Date)]
existing <- existing[Date < min(df$Date)]

df <- rbindlist(list(existing, df), use.names = TRUE)
setorder(df, -Date)

fwrite(df, "automated_sheets/Sweden.csv")
