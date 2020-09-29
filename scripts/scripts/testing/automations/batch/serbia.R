df <- fread("https://raw.githubusercontent.com/aleksandar-jovicic/COVID19-Serbia/master/timeseries.csv",
            showProgress = FALSE, select = c("ts", "tested"))

setnames(df, c("Date", "Cumulative total"))

df[, Date := str_sub(Date, 1, 10)]
df <- df[, .(`Cumulative total` = max(`Cumulative total`)), Date]

df[, Country := "Serbia"]
df[, Units := "people tested"]
df[, `Source label` := "Ministry of Health"]
df[, `Source URL` := "https://github.com/aleksandar-jovicic/COVID19-Serbia/blob/master/timeseries.csv"]
df[, Notes := "Made available by Aleksandar Jovičić on Github"]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Serbia.csv")
