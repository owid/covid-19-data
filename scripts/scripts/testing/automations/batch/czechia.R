page <- read_html("https://onemocneni-aktualne.mzcr.cz/covid-19")

url <- page %>% html_node("#prehled a") %>% html_attr("href")

download.file(url = url, destfile = "tmp/czechia.xlsx", quiet = TRUE)

df <- readxl::read_excel("tmp/czechia.xlsx", skip = 4) %>% data.table()

setnames(df, c("Datum", "Počet PCR testů celkem", "Počet antigenních testů celkem", "Pozitivní celkem...8"), c("Date", "pcr", "antigen", "positive"))

df[, Date := date(Date)]
setorder(df, Date)

df[, `Cumulative total` := pcr + antigen]
df[, `Positive rate` := round(frollsum(positive, 7) / frollsum(`Cumulative total`, 7), 3)]

df <- df[, c("Date", "Cumulative total", "Positive rate")]

df[, Country := "Czechia"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://onemocneni-aktualne.mzcr.cz/covid-19"]
df[, `Source label` := "Ministry of Health"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Czechia.csv")

file.remove("tmp/czechia.xlsx")
