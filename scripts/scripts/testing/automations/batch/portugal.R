df <- suppressMessages(rio::import("input/DATA PORTUGAL.xlsx"))
setDT(df)
df <- df[, 1:4]

setnames(df, c("month", "day", "PCR", "Antigen"))

df[, `Daily change in cumulative total` := PCR + Antigen]
df[, c("PCR", "Antigen") := NULL]

start_date <- dmy(sprintf("%s %s 2020", df$day[1], df$month[1]))
seq_date <- seq.Date(from = start_date, by = "1 day", length.out = nrow(df))
df$Date <- seq_date

df[, c("day", "month") := NULL]

df <- df[!is.na(`Daily change in cumulative total`)]

df[, Country := "Portugal"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://covid19.min-saude.pt/ponto-de-situacao-atual-em-portugal/"]
df[, `Source label` := "Ministry of Health"]
df[, Notes := NA_character_]
df[, `Testing type` := "includes non-PCR"]

fwrite(df, "automated_sheets/Portugal.csv")
