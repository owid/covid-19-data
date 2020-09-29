df <- fread("https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_timeline_testing.csv",
            select = c("YYYYMMDD", "cumulative_tests"), showProgress = FALSE)

setnames(df, c("Date", "Cumulative total"))

df <- df[!is.na(`Cumulative total`)]
df[, Date := ymd(Date)]
df <- df[, .(`Cumulative total` = min(`Cumulative total`)), Date]

df[, Country := "South Africa"]
df[, Units := "people tested"]
df[, `Source URL` := "https://github.com/dsfsi/covid19za"]
df[, `Source label` := "National Institute for Communicable Diseases (NICD)"]
df[, Notes := "Made available by the University of Pretoria on Github"]
df[, `Testing type` := "PCR only"]

# Hard-coded first point for 7 February 2020, missing from GitHub
first <- head(df, 1)
first[, Date := ymd("2020-02-07")]
first[, `Cumulative total` := 42]
first[, `Source URL` := "https://www.nicd.ac.za/novel-coronavirus-update/"]
first[, `Testing type` := "PCR only"]
first[, Notes := NA_character_]

df <- rbindlist(list(first, df))

fwrite(df, "automated_sheets/South Africa.csv")
