df <- fread("https://raw.githubusercontent.com/COVID19-Malta/COVID19-Cases/master/COVID-19%20Malta%20-%20Number%20of%20PCR%20Tests%20by%20Date.csv",
            showProgress = FALSE)

df[, Units := str_replace(Entity, "Malta - ", "")]
df[, Country := "Malta"]
df[, Date := str_sub(Date, 1, 10)]
df[, `Source URL` := "https://github.com/COVID19-Malta/COVID19-Cases/blob/master/COVID-19%20Malta%20-%20Number%20of%20PCR%20Tests%20by%20Date.csv"]
df[, `Testing type` := "PCR only"]
df[, `Source label` := "COVID-19 Malta Public Health Response Team"]
df[, Notes := NA_character_]
setnames(df, "Change in cumulative total", "Daily change in cumulative total")

df[, c("Entity", "ISO Code", "Change in cumulative total per thousand", "Cumulative total per thousand", "7-day smoothed daily change per thousand") := NULL]

df <- df[`Daily change in cumulative total` != 0]

fwrite(df, "automated_sheets/Malta.csv")
