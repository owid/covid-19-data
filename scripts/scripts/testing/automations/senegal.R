df <- fread("https://raw.githubusercontent.com/senegalouvert/COVID-19/master/data/confirmes.csv",
            select = c("date", "tests"), showProgress = FALSE)

df <- df[!is.na(tests)]

setnames(df, c("Date", "Daily change in cumulative total"))

df[, Country := "Senegal"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://github.com/senegalouvert/COVID-19"]
df[, `Source label` := "Ministry of Health and Social Action"]
df[, Notes := "Made available by Senegal Ouvert on Github"]
df[, Date := ymd(Date)]
df[, `Testing type` := "PCR only"]

# Early data point with wrong figure on GitHub
# https://www.notion.so/owid/Senegal-41b493bf7877420f9ba9358bdcf627ef
df[Date == ymd("2020-03-04"), `Daily change in cumulative total` := 11]

fwrite(df, "automated_sheets/Senegal.csv")
