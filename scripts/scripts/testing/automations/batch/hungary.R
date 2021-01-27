gs4_auth(email = CONFIG$google_credentials_email)

retry(
    expr = {
        df <- suppressMessages(read_sheet(
            "https://docs.google.com/spreadsheets/d/1e4VEZL1xvsALoOIq9V2SQuICeQrT5MtWfBm32ad7i8Q/edit#gid=311133316",
            sheet = "koronahun"
        ))
    },
    when = "RESOURCE_EXHAUSTED",
    max_tries = 10,
    interval = 20
)

setDT(df)
df <- df[, .(Dátum, `Új mintavételek száma`)]
setnames(df, c("Date", "Daily change in cumulative total"))

df <- df[`Daily change in cumulative total` != 0]

df[, Date := date(Date)]
df[, Country := "Hungary"]
df[, Units := "tests performed"]
df[, `Source label` := "Government of Hungary"]
df[, `Source URL` := "https://atlo.team/koronamonitor/"]
df[, Notes := "Made available by Atlo.team"]

fwrite(df, "automated_sheets/Hungary.csv")
