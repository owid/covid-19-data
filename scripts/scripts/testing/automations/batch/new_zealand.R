df <- read_html("https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/covid-19-current-situation/covid-19-current-cases") %>%
    html_nodes(".table-style-two")

df <- df[df %>% html_node("caption") %>% html_text %>% str_detect("tests") %>% replace_na(FALSE)][[1]] %>% html_table
setDT(df)
df <- df[2:nrow(df)]

df[, `Tests per day` := NULL]
setnames(df, c("Total tests (cumulative)"), c("Cumulative total"))

df[, Date := dmy(paste0(Date, "-2020"))]
df[, Country := "New Zealand"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/covid-19-current-situation/covid-19-current-cases"]
df[, `Source label` := "Ministry of Health"]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/New Zealand.csv")
