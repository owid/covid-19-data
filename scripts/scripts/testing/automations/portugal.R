df <- suppressMessages(import("input/DATA PORTUGAL.xlsx"))
setDT(df)
df <- df[, 1:3]

setnames(df, c("month", "day", "Daily change in cumulative total"))

df[, month := zoo::na.locf(month)]
df[, Date := dmy(sprintf("%s %s 2020", day, month))]

df[, c("day", "month") := NULL]

df <- df[!is.na(`Daily change in cumulative total`)]

df[, Country := "Portugal"]
df[, Units := "samples tested"]
df[, `Source URL` := "https://covid19.min-saude.pt/ponto-de-situacao-atual-em-portugal/"]
df[, `Source label` := "Ministry of Health"]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Portugal.csv")
