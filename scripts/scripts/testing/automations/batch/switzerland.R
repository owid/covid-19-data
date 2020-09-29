url <- "https://www.bag.admin.ch/bag/en/home/krankheiten/ausbrueche-epidemien-pandemien/aktuelle-ausbrueche-epidemien/novel-cov/situation-schweiz-und-international.html"

page <- read_html(url)

files <- page %>%
    html_nodes(".clearfix .icon--excel")

idx <- files %>%
    html_text() %>%
    str_detect("tests conducted") %>%
    which()

url <- files[idx] %>%
    html_attr("href") %>%
    paste0("https://www.bag.admin.ch", .)

df <- rio::import(url) %>%
    data.table()

df <- df[, c("Datum", "Number_of_tests")]
setnames(df, "Datum", "Date")
df[, Date := date(Date)]
df <- df[, .(`Daily change in cumulative total` = sum(Number_of_tests)), Date]

df[, Country := "Switzerland"]
df[, Units := "tests performed"]
df[, `Source URL` := url]
df[, `Source label` := "Federal Office of Public Health"]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Switzerland.csv")
