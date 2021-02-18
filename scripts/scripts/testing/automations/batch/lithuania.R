df <- fread("https://opendata.arcgis.com/datasets/d49a63c934be4f65a93b6273785a8449_0.csv?where=municipality_code%20%3D%20%2700%27",
            select = c("date","dgn_pos_day", "dgn_tot_day"), showProgress = FALSE)

setnames(df, c("date","dgn_pos_day", "dgn_tot_day"), c("Date", "daily_pos", "Daily change in cumulative total"))

df <- df[`Daily change in cumulative total` > 0]

df[, Date := date(ymd_hms(Date))]
setorder(df, Date)

df[, `Positive rate` := round(frollsum(daily_pos, 7) / frollsum(`Daily change in cumulative total`, 7), 3)]

df = subset(df, select= -daily_pos)

df[, Country := "Lithuania"]
df[, Units := "tests performed"]
df[, `Source label` := "Government of Lithuania"]
df[, `Source URL` := "https://open-data-ls-osp-sdg.hub.arcgis.com/datasets/d49a63c934be4f65a93b6273785a8449_0"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Lithuania.csv")
