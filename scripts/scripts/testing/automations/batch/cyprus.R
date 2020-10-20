url <- read_html("https://www.data.gov.cy/node/4617?language=en") %>%
    html_node(".data-link") %>%
    html_attr("href")

df <- fread(url, showProgress = FALSE, select = c("date", "daily tests performed", "total tests"), na.strings = ":")
setnames(df, c("Date", "Daily change in cumulative total", "Cumulative total"))

df <- df[!is.na(`Daily change in cumulative total`) | !is.na(`Cumulative total`)]

df[, Date := dmy(Date)]
df[, Country := "Cyprus"]
df[, `Source label` := "Ministry of Health"]
df[, `Source URL` := url]
df[, Units := "tests performed"]
df[, `Testing type` := "PCR only"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Cyprus.csv")
