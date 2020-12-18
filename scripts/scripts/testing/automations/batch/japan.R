url <- "https://www.mhlw.go.jp/content/pcr_tested_daily.csv"

df <- fread(url, showProgress = FALSE)

setnames(df, c("Date", "Daily change in cumulative total"))

df[, Date := ymd(Date)]

df[, Country := "Japan"]
df[, Units := "people tested"]
df[, `Source URL` := url]
df[, `Source label` := "Ministry of Health, Labour and Welfare"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Japan.csv")
