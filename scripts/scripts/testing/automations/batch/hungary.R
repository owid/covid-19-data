gs4_auth(email = CONFIG$google_credentials_email)

df <- suppressMessages(read_sheet(
    "https://docs.google.com/spreadsheets/d/1e4VEZL1xvsALoOIq9V2SQuICeQrT5MtWfBm32ad7i8Q/edit#gid=311133316",
    sheet = "koronahun"
))

setDT(df)
df <- df[, .(Dátum, `Mintavétel száma`)]
setnames(df, c("Date", "Cumulative total"))

df <- df[nchar(Date) == 10]
df[, Date := as_datetime(as.integer(df$Date))]
df[, `Cumulative total` := as.integer(`Cumulative total`)]
df <- df[, .(Date = min(Date)), `Cumulative total`]

df[, Date := date(Date)]
df[, Country := "Hungary"]
df[, Units := "tests performed"]
df[, `Source label` := "Government of Hungary"]
df[, `Source URL` := "https://atlo.team/koronamonitor/"]
df[, Notes := "Made available by Atlo.team"]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Hungary.csv")
