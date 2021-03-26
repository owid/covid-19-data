url <- read_html("https://covid19.ssi.dk/overvagningsdata/download-fil-med-overvaagningdata") %>%
    html_nodes("accordions a") %>%
    html_attr("href") %>%
    str_subset("data-epidemiologiske-rapport") %>%
    head(1)

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
