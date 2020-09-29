df <- import("https://www.gov.si/assets/vlada/Koronavirus-podatki/COVID-19-vsi-podatki.xlsx")
setDT(df)
df <- df[, .(Datum, `Skupno število testiranj`)]

setnames(df, c("Date", "Cumulative total"))

df <- df[!is.na(`Cumulative total`)]

df <- df[, Date := as.integer(Date) + ymd("1899-12-30")]
df[, Country := "Slovenia"]
df[, Units := "tests performed"]
df[, `Source label` := "Government of Slovenia"]
df[, `Source URL` := "https://www.gov.si/assets/vlada/Koronavirus-podatki/COVID-19-vsi-podatki.xlsx"]
df[, Notes := NA_character_]
df[, `Testing type` := "unclear"]

fwrite(df, "automated_sheets/Slovenia.csv")
