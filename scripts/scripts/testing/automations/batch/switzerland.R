url <- read_html("https://www.covid19.admin.ch/fr/epidemiologic/test") %>%
    html_nodes(".footer__nav .footer__nav__link") %>%
    html_attr("href") %>%
    str_subset("csv.zip") %>%
    paste0("https://www.covid19.admin.ch", .)

download.file(url, "tmp/switzerland.zip", quiet = TRUE)
unzip("tmp/switzerland.zip", exdir = "tmp", files = "data/COVID19Test_geoRegion_PCR_Antigen.csv")

df <- fread("tmp/data/COVID19Test_geoRegion_PCR_Antigen.csv",
            select = c("datum", "entries", "entries_pos", "nachweismethode", "geoRegion"))
df <- df[geoRegion == "CH"]

df <- df[, .(
    entries = sum(entries, na.rm = TRUE),
    entries_pos = sum(entries_pos, na.rm = TRUE)
), datum]

setorder(df, datum)
df[, pr := round(frollsum(entries_pos, 7) / frollsum(entries, 7), 3)]
df <- df[, c("datum", "entries", "pr")]
setnames(df, c("datum", "entries", "pr"), c("Date", "Daily change in cumulative total", "Positive rate"))

df[, Country := "Switzerland"]
df[, Units := "tests performed"]
df[, `Source URL` := url]
df[, `Source label` := "Federal Office of Public Health"]
df[, Notes := NA_character_]

df <- df[`Daily change in cumulative total` > 0]

fwrite(df, "automated_sheets/Switzerland.csv")

file.remove("tmp/data/COVID19Test_geoRegion_PCR_Antigen.csv")
file.remove("tmp/switzerland.zip")
