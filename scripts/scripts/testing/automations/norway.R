df <- fread("input/number-of-tested-persons.csv")

df[, `Daily change in cumulative total` := Negative + Positive]
df[, Date := dmy(Date)]
df[, Country := "Norway"]
df[, Units := "people tested"]
df[, `Source URL` := "https://www.fhi.no/en/id/infectious-diseases/coronavirus/daily-reports/daily-reports-COVID19/"]
df[, `Source label` := "Norwegian Institute of Public Health"]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]
df[, c("Negative", "Positive", "Percent") := NULL]

stopifnot(max(df$Date) >= today() - 4)

fwrite(df, "automated_sheets/Norway.csv")
