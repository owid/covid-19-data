url <- read_html("https://govextra.gov.il/ministry-of-health/corona/corona-virus/") %>%
    html_node(".corona-header .corona-titlebar a") %>%
    html_attr("href") %>%
    paste0("https://govextra.gov.il", .)

df <- rio::import(url)

setDT(df)
df <- df[, c("תאריך", "מספר בדיקות מצטבר")]
setnames(df, c("Date", "Cumulative total"))

# Dates in the Excel file are sometimes formatted in the wrong ways
# Some will appear as dates, others as integers
# This recreates a date sequence (if at least the first and last rows have correct dates)
suppressWarnings(df[, Date := date(mdy_hms(Date))])
min_date <- df$Date[1]
max_date <- df$Date[nrow(df)]
stopifnot(!is.na(min_date))
stopifnot(!is.na(max_date))
date_seq <- seq.Date(min_date, max_date, "1 day")
stopifnot(length(date_seq) == nrow(df))
df$Date <- date_seq

df[, `Cumulative total` := as.integer(str_replace_all(`Cumulative total`, "[^\\d]", ""))]
df <- df[, .(Date = min(Date)), `Cumulative total`]

df[, Country := "Israel"]
df[, Units := "tests performed"]
df[, `Source label` := "Israel Ministry of Health"]
df[, `Source URL` := url]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Israel.csv")
