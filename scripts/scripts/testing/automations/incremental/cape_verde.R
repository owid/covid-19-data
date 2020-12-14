retry(
    expr = {
        url <- read_html("https://covid19.cv/category/boletim-epidemiologico/page/1/") %>%
            html_node(".elementor-post__title a") %>%
            html_attr("href")
    },
    when = "SSL_ERROR_SYSCALL",
    max_tries = 3,
    interval = 10
)

page <- read_html(url)
date <- page %>%
    html_node(".entry-title") %>%
    html_text() %>%
    str_extract("\\d+ .* 202\\d") %>%
    str_replace_all(" de ", "") %>%
    str_to_lower() %>%
    str_replace_all(
        c("janeiro" = "/01/", "fevereiro" = "/02/", "março" = "/03/", "abril" = "/04/",
          "maio" = "/05/", "junho" = "/06/", "julho" = "/07/", "agosto" = "/08/",
          "setembro" = "/09/", "outubro" = "/10/", "novembro" = "/11/", "dezembro" = "/12/")) %>%
    dmy() - 1
content <- page %>% html_node(".page-content") %>% html_text()
count <- content %>% str_extract("(total|totais) (de|dos|das) [\\d ]+ (resultados|amostras)") %>% str_replace_all("[^\\d]", "") %>% as.integer()
if (is.na(count)) count <- content %>% str_extract("Total de amostras .*[\\d ]+") %>% str_replace_all("[^\\d]", "") %>% as.integer()

new <- data.table(Date = date, `Daily change in cumulative total` = count, `Source URL` = url)
new[, Country := "Cape Verde"]
new[, Units := "tests performed"]
new[, `Source label` := "Government of Cape Verde"]
new[, Notes := NA_character_]
new[, `Cumulative total` := NA_integer_]

existing <- fread("automated_sheets/Cape Verde.csv")
existing[, Date := ymd(Date)]

stopifnot(!is.na(count))

if (date > max(existing$Date)) {
    df <- rbindlist(list(new, existing))
    setorder(df, -Date)
    fwrite(df, "automated_sheets/Cape Verde.csv")
}
