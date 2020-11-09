url <- "https://health-infobase.canada.ca/src/data/covidLive/covid19-download.csv"

df <- fread(url, showProgress = FALSE, select = c("prname", "date", "numtested"))
df <- df[prname == "Canada"]
df <- df[!is.na(numtested)]

setnames(df, c("prname", "date", "numtested"), c("Country", "Date", "Cumulative total"))

df[, Units := "people tested"]
df[, `Source label` := "Government of Canada"]
df[, `Source URL` := url]
df[, `Testing type` := "PCR only"]
df[, Notes := NA_character_]

setorder(df, Date)
df <- df[, .SD[1], `Cumulative total`]

fwrite(df, "automated_sheets/Canada.csv")
