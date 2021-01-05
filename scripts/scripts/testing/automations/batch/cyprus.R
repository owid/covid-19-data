url <- read_html("https://www.data.gov.cy/node/4617?language=en") %>%
    html_nodes(".data-link") %>%
    html_attr("href") %>%
    str_subset("Extended.*csv$") %>%
    max()

df <- fread(url, showProgress = FALSE, select = c("date", "total tests"), na.strings = ":")
setnames(df, c("Date", "Cumulative total"))

df <- df[!is.na(`Cumulative total`)]

if (str_detect(df$Date[1], "^\\d{4}")) df[, Date := str_replace(Date, "^00", "")]

df[, Date := dmy(Date)]
df[, Country := "Cyprus"]
df[, `Source label` := "Ministry of Health"]
df[, `Source URL` := "https://www.data.gov.cy/node/4617?language=en"]
df[, Units := "tests performed"]
df[, Notes := NA_character_]

existing <- fread("automated_sheets/Cyprus.csv")
existing <- existing[Date < min(df$Date)]
existing[, Date := ymd(Date)]

df <- rbindlist(list(existing, df))
setorder(df, Date)

fwrite(df, "automated_sheets/Cyprus.csv")
