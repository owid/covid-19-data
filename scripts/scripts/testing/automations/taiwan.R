df <- fread("https://data.cdc.gov.tw/en/download?resourceid=7ee40c7d-a14c-47b3-bf27-a5de4c278782&dataurl=https://od.cdc.gov.tw/eic/covid19/covid19_tw_specimen.csv",
            showProgress = FALSE, select = c("通報日", "Total"))

df <- df[!is.na(Total) & Total != 0]
setnames(df, c("Date", "Daily change in cumulative total"))
df[, Date := ymd(Date)]

df[, Notes := NA_character_]
df[, Country := "Taiwan"]
df[, Units := "people tested"]
df[, `Source URL` := "https://data.cdc.gov.tw/en/dataset/daily-cases-suspected-sars-cov-2-infection_tested"]
df[, `Source label` := "Taiwan CDC Open Data Portal"]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Taiwan.csv")
