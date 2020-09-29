df <- fread("https://data.gov.lv/dati/dataset/f01ada0a-2e77-4a82-8ba2-09cf0cf90db3/resource/d499d2f0-b1ea-4ba2-9600-2c701b03bd4a/download/covid_19_izmeklejumi_rezultati.csv",
            showProgress = FALSE, select = c("Datums", "TestuSkaits"))

setnames(df, c("Date", "Daily change in cumulative total"))

df[, Date := ymd(Date)]
df <- df[!is.na(Date)]
df[, Country := "Latvia"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://data.gov.lv/dati/eng/dataset/covid-19"]
df[, `Source label` := "Center for Disease Prevention and Control"]
df[, Notes := "Collected from the Latvian Open Data Portal"]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Latvia.csv")
