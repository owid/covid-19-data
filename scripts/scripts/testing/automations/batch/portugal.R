df <- suppressMessages(rio::import("input/DATA PORTUGAL.xlsx"))
setDT(df)
df <- df[, 1:3]

setnames(df, c("month", "day", "Daily change in cumulative total"))

df[, month := zoo::na.locf(month)]
df[, Date := dmy(sprintf("%s %s 2020", day, month))]

df[, c("day", "month") := NULL]

df <- df[!is.na(`Daily change in cumulative total`)]

if (max(df$Date) < today() - 7) {
    github <- fread("https://raw.githubusercontent.com/dssg-pt/covid19pt-data/master/amostras.csv",
                showProgress = FALSE, select = c("data", "amostras"))
    setnames(github, c("Date", "Cumulative total"))
    github[, Date := dmy(Date)]
    github <- github[!is.na(`Cumulative total`)]
    if (max(github$Date) > max(df$Date)) df <- copy(github)
}

df[, Country := "Portugal"]
df[, Units := "samples tested"]
df[, `Source URL` := "https://covid19.min-saude.pt/ponto-de-situacao-atual-em-portugal/"]
df[, `Source label` := "Ministry of Health"]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Portugal.csv")
