url <- "https://health-infobase.canada.ca/src/data/covidLive/covid19-download.csv"

df <- fread(url, showProgress = FALSE, select = c("prname", "date", "numtested", "numtests"))
df <- df[prname == "Canada"]

df[, `Source label` := "Government of Canada"]
df[, `Source URL` := url]
df[, `Testing type` := "PCR + antigen"]
df[, Notes := NA_character_]

people <- copy(df)
people[, Units := "people tested"]
people[, numtests := NULL]
setnames(people, c("prname", "date", "numtested"), c("Country", "Date", "Cumulative total"))
setorder(people, Date)
people <- people[!is.na(`Cumulative total`)]
people <- people[, .SD[1], `Cumulative total`]

fwrite(people, "automated_sheets/Canada - people tested.csv")

tests <- copy(df)
tests[, Units := "tests performed"]
tests[, numtested := NULL]
setnames(tests, c("prname", "date", "numtests"), c("Country", "Date", "Cumulative total"))
setorder(tests, Date)
tests <- tests[!is.na(`Cumulative total`)]
tests <- tests[, .SD[1], `Cumulative total`]

fwrite(tests, "automated_sheets/Canada - tests performed.csv")
