url <- "https://raw.githubusercontent.com/okestonia/koroonakaart/master/koroonakaart/src/data.json"
df <- fromJSON(file = url)
df <- data.table(
    Date = df$dates2,
    `Cumulative total` = df$dataCumulativeTestsChart$testsAdminstered
)

df[, Country := "Estonia"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://www.koroonakaart.ee/en"]
df[, `Source label` := "Central Health Information System and Patient Portal"]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Estonia.csv")
