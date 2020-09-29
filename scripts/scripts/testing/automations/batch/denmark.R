url <- read_html("https://www.ssi.dk/sygdomme-beredskab-og-forskning/sygdomsovervaagning/c/covid19-overvaagning/arkiv-med-overvaagningsdata-for-covid19") %>%
    html_node(".accordion-body") %>%
    html_nodes("a")

url <- url[min(which(html_text(url) != ""))] %>%
    html_attr("href") %>%
    str_replace_all("\\s", "")

download.file(url = url, destfile = "tmp/tmp.zip", quiet = TRUE)
unzip(zipfile = "tmp/tmp.zip", exdir = "tmp")

df <- fread("tmp/Test_pos_over_time.csv", showProgress = FALSE, select = c("Date", "Tested", "Tested_kumulativ"), dec = ",")

df[, Tested := as.integer(str_replace_all(Tested, "\\.", ""))]
df[, Tested_kumulativ := as.integer(str_replace_all(Tested_kumulativ, "\\.", ""))]

df <- df[str_detect(Date, "[\\d-]{10}")]
df <- df[Tested != 0]
setnames(
    df,
    c("Tested", "Tested_kumulativ"), 
    c("Daily change in cumulative total", "Cumulative total")
)

df[, Country := "Denmark"]
df[, Units := "tests performed"]
df[, `Source URL` := url]
df[, `Source label` := "Statens Serum Institut"]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Denmark.csv")
