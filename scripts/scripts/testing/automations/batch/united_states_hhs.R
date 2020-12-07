# Test counts
meta <- fromJSON(file = "https://healthdata.gov/api/3/action/package_show?id=c13c00e3-f3d0-4d49-8c43-bf600a6c0a0d")
url <- meta$result[[1]]$resources[[1]]$url

df <- fread(url, showProgress = FALSE, select = c("date", "new_results_reported"))
setnames(df, "date", "Date")

df <- df[, .(`Daily change in cumulative total` = sum(new_results_reported)), Date]
setorder(df, Date)

# Data presented might not represent the most current counts for the most recent 3 days
# due to the time it takes to report testing information.
df <- df[Date <= today() - 3]

# Positive rate
url <- read_html("https://www.cdc.gov/coronavirus/2019-ncov/covid-data/covidview/index.html") %>%
    html_nodes(".syndicate a") %>%
    html_attr("href") %>%
    str_subset("csv$") %>%
    paste0("https://www.cdc.gov", .)
positive_rate <- fread(url, skip = 5, select = c("Week", "% of Specimens Positive for SARS-CoV-2"))
positive_rate[, year := str_sub(Week, 1, 4)]
positive_rate[, week_no := as.integer(str_sub(Week, 5, 6))]
positive_rate[, Date := ymd(sprintf("%s0101", year))]
week(positive_rate$Date) <- positive_rate$week_no
positive_rate[, Date := Date + 3]
positive_rate[, c("year", "week_no", "Week") := NULL]
setnames(positive_rate, "% of Specimens Positive for SARS-CoV-2", "Positive rate")
positive_rate[, `Positive rate` := round(`Positive rate` / 100, 3)]

df <- merge(df, positive_rate, by = "Date", all.x = TRUE)

df[, Country := "United States"]
df[, Units := "tests performed"]
df[, `Source label` := "Department of Health & Human Services"]
df[, `Source URL` := url]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/United States - tests performed.csv")
