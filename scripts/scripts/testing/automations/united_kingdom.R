url <- read_html("https://www.gov.uk/guidance/coronavirus-covid-19-information-for-the-public") %>%
    html_node(".govspeak ul li .govuk-link") %>%
    html_attr("href")

df <- fread(url, showProgress = FALSE, na.strings = c("Unavailable", ""))

df <- df[Pillar != "Pillar 4"]
df <- df[Pillar != "Pillar 3"]
df <- df[Nation == "UK"]

df[`Date of activity` == "the", `Date of activity` := NA_character_]
df[, Date := dmy(`Date of activity`)]
df[is.na(Date), Date := dmy(`Date of reporting`) - 1]

df[, daily_tests := as.integer(`Daily number of tests processed`)]
df[, total_tests := as.integer(`Cumulative number of tests processed`)]

df <- df[, .(
    `Daily change in cumulative total` = sum(daily_tests),
    `Cumulative total` = sum(total_tests)
), Date]

df <- df[!is.na(`Daily change in cumulative total`) | !is.na(`Cumulative total`)]

df[, Country := "United Kingdom"]
df[, Units := "tests performed"]
df[, `Source label` := "Public Health England/Department of Health and Social Care"]
df[, `Source URL` := url]
df[, Notes := "Sum of tests processed for pillars 1 and 2"]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/United Kingdom - tests.csv")
