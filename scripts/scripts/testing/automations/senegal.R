df <- fread("https://raw.githubusercontent.com/senegalouvert/COVID-19/master/data/confirmes.csv",
            select = c("date", "tests"), showProgress = FALSE)

df <- df[!is.na(tests)]

setnames(df, c("Date", "Daily change in cumulative total"))

df[, Country := "Senegal"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://github.com/senegalouvert/COVID-19"]
df[, `Source label` := "Ministry of Health and Social Action"]
df[, Notes := "Made available by Senegal Ouvert on GitHub"]
df[, Date := ymd(Date)]
df[, `Testing type` := "PCR only"]

# Data point with wrong figure on GitHub
# https://www.notion.so/owid/Senegal-41b493bf7877420f9ba9358bdcf627ef
df[Date == ymd("2020-03-04"), `Daily change in cumulative total` := 11]
df[Date == ymd("2020-03-04"), `Source URL` := "http://www.sante.gouv.sn/sites/default/files/Communiqu%C3%A9%20de%20presse%20N%C2%B03%20sur%20le%20Coronavirus.pdf"]
df[Date == ymd("2020-03-04"), Notes := "Based on official data source instead of GitHub"]

# Data point with wrong figure on GitHub
# https://owid.slack.com/archives/C011DSUBY6A/p1599859723005400
df[Date == ymd("2020-05-18"), `Daily change in cumulative total` := 1172]
df[Date == ymd("2020-05-18"), `Source URL` := "http://www.sante.gouv.sn/sites/default/files/COMMUNIQUE%2078%20DU%2018%20MAI%202020.pdf"]
df[Date == ymd("2020-05-18"), Notes := "Based on official data source instead of GitHub"]

fwrite(df, "automated_sheets/Senegal.csv")
