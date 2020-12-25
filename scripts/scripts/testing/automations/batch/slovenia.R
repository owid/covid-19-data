df <- rio::import("https://www.gov.si/assets/vlada/Koronavirus-podatki/COVID-19-vsi-podatki.xlsx")
setDT(df)
df <- df[, c("Datum", "Dnevno število PCR testiranj", "Dnevno število HAGT testiranj")]

setnames(df, c("Date", "pcr_tests", "ag_tests"))

suppressWarnings(df <- df[, Date := as.integer(Date) + ymd("1899-12-30")])
df <- df[!is.na(Date)]

setnafill(df, fill = 0)

df[, `Daily change in cumulative total` := pcr_tests + ag_tests]

df <- df[, c("Date", "Daily change in cumulative total")]

df[, Country := "Slovenia"]
df[, Units := "tests performed"]
df[, `Source label` := "Government of Slovenia"]
df[, `Source URL` := "https://www.gov.si/assets/vlada/Koronavirus-podatki/COVID-19-vsi-podatki.xlsx"]
df[, Notes := NA_character_]
df[, `Testing type` := "unclear"]

fwrite(df, "automated_sheets/Slovenia.csv")
