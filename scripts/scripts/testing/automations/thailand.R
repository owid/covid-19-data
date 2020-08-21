urls <- read_html("https://ddc.moph.go.th/viralpneumonia/situation.php") %>%
    html_nodes(".banner-section table a") %>%
    html_attr("href") %>%
    paste0("https://ddc.moph.go.th/viralpneumonia/", .)

process_url <- function(url) {
    download.file(url = url, destfile = "tmp/tmp.pdf", quiet = TRUE)

    string <- "ยสะสม\n.*[\\d,]{6,}.*\n.*[\\d,]{6,}.*"

    counts <- pdf_text("tmp/tmp.pdf") %>%
        str_extract(string) %>%
        str_extract_all("[\\d,]{6,}") %>%
        unlist() %>%
        na.omit() %>%
        str_replace_all("[^\\d]", "") %>%
        as.integer()

    date <- url %>%
        str_extract("situation-no\\d+-\\d{4}") %>%
        str_extract("\\d+$") %>%
        paste0("2020") %>%
        dmy()

    tests <- counts[1]
    people <- counts[2]
    stopifnot(!is.na(tests) & !is.na(people))
    stopifnot(tests > people)

    add_snapshot(
        count = people,
        date = date,
        sheet_name = "Thailand - people tested",
        country = "Thailand",
        units = "people tested",
        testing_type = "PCR only",
        source_url = url,
        source_label = "Department of Disease Control"
    )

    add_snapshot(
        count = tests,
        date = date,
        sheet_name = "Thailand - tests performed",
        country = "Thailand",
        units = "tests performed",
        testing_type = "PCR only",
        source_url = url,
        source_label = "Department of Disease Control"
    )
}

process_url(urls[1])
