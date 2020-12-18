# Link to data drop: https://drive.google.com/drive/folders/1ZPPcVU4M7T-dtRyUceb0pMAd8ickYf8o

suppressMessages(drive_auth(email = CONFIG$google_credentials_email))

master_folder <- "https://drive.google.com/drive/folders/1ZPPcVU4M7T-dtRyUceb0pMAd8ickYf8o"
files <- drive_ls(master_folder)
setDT(files)
pdf_file <- files[str_detect(files$name, "READ ME FIRST"), id]
drive_download(file = as_id(pdf_file), path = "tmp/tmp.pdf", overwrite = TRUE, verbose = FALSE)

url <- pdf_text("tmp/tmp.pdf") %>%
    str_extract_all("https?://bit.ly.*") %>%
    unlist() %>%
    head(1)
url <- httr::GET(url)$url

files <- drive_ls(url)
setDT(files)
testing_aggregates_file <- files[str_detect(files$name, "Testing Aggregates.csv"), id]
drive_download(file = as_id(testing_aggregates_file), path = "tmp/tmp.csv", overwrite = TRUE, verbose = FALSE)

df <- fread("tmp/tmp.csv", select = c("report_date", "cumulative_unique_individuals"), showProgress = FALSE)
setnames(df, "report_date", "Date")
df <- df[, .(`Cumulative total` = sum(cumulative_unique_individuals)), Date]
setorder(df, Date)

df[, Date := ymd(Date)]
stopifnot(sum(is.na(df$Date)) == 0)
df[, Country := "Philippines"]
df[, Units := "people tested"]
df[, `Source URL` := url]
df[, `Source label` := "Philippines Department of Health"]
df[, Notes := NA_character_]

stopifnot(max(df$Date) > (today() - 7))

fwrite(df, "automated_sheets/Philippines.csv")
