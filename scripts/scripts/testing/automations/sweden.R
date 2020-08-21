url <- "https://www.folkhalsomyndigheten.se/smittskydd-beredskap/utbrott/aktuella-utbrott/covid-19/antal-individer-som-har-testats-for-covid-19/"
page <- read_html(url)

df <- page %>%
    html_node("#content-primary table") %>%
    html_table() %>%
    data.table()
df[, Nukleinsyrapåvisning := as.integer(str_replace_all(Nukleinsyrapåvisning, "\\s", ""))]

df <- df[str_detect(Vecka, "vecka")]
df[, Vecka := as.integer(str_replace(Vecka, "vecka ", ""))]
df[, Date := ymd("2019-12-29") + 7 * Vecka]

setnames(df, "Nukleinsyrapåvisning", "Weekly")
df[, c("Antikroppspåvisning", "Vecka") := NULL]
setorder(df, Date)

split_week <- function(row_idx) {
    end_date <- df[row_idx, Date]
    weekly_tests <- df[row_idx, Weekly]
    start_date <- end_date - 6
    daily_tests <- round(weekly_tests / 7)
    notes <- sprintf("Dividing %s (weekly total) by 7", format(weekly_tests, big.mark = ","))
    dates <- seq.Date(start_date, end_date, by = "1 day")
    return(data.table(
        Date = dates,
        `Daily change in cumulative total` = daily_tests,
        Notes = notes
    ))
}

df <- rbindlist(lapply(1:nrow(df), FUN = split_week))

df[, `Source URL` := url]
df[, Country := "Sweden"]
df[, Units := "samples tested"]
df[, `Source label` := "Swedish Public Health Agency"]
df[, Notes := NA]
df[, `Testing type` := "PCR only"]
df[, `Cumulative total` := NA_integer_]

fwrite(df, "automated_sheets/Sweden - samples tested.csv")
