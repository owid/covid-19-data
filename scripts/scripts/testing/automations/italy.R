df <- fread("https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-andamento-nazionale/dpc-covid19-ita-andamento-nazionale.csv",
            select = c("data", "tamponi", "casi_testati"), showProgress = FALSE)

setnames(df, "data", "Date")

df[, Date := date(ymd_hms(Date))]
df[, Country := "Italy"]
df[, `Source URL` := "https://github.com/pcm-dpc/COVID-19/tree/master/dati-andamento-nazionale"]
df[, `Source label` := "Presidency of the Council of Ministers"]
df[, Notes := "Made available by the Department of Civil Protection on GitHub"]
df[, `Testing type` := "PCR only"]

samples <- copy(df)
samples[, Units := "tests performed"]
samples[, casi_testati := NULL]
setnames(samples, "tamponi", "Cumulative total")
samples <- samples[!is.na(`Cumulative total`)]

fwrite(samples, "automated_sheets/Italy - tests performed.csv")

people <- copy(df)
people[, Units := "people tested"]
people[, tamponi := NULL]
setnames(people, "casi_testati", "Cumulative total")
people <- people[!is.na(`Cumulative total`)]

fwrite(people, "automated_sheets/Italy - people tested.csv")
