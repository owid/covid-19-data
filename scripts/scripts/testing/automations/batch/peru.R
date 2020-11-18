get_resources <- function() {
    resources <- read_html("https://datos.ins.gob.pe/dataset?q=%22pruebas+moleculares%22&organization=covid-19&sort=metadata_modified+desc") %>%
        html_nodes(".dataset-heading a") %>%
        html_attr("href") %>%
        paste0("https://datos.ins.gob.pe", .)
    return(resources)
}

get_resource_url <- function(url) {
    file_url <- read_html(url) %>%
        html_nodes(".resource-item a") %>%
        html_attr("href") %>%
        str_subset("\\.csv$")
    return(file_url)
}

process_file <- function(url) {
    df <- fread(url, showProgress = FALSE, select = c("FECHATOMAMUESTRA"))
    setnames(df, "Date")
    df[, Date := dmy(Date)]
    return(df)
}

resources <- get_resources()
files <- sapply(resources, FUN = get_resource_url, USE.NAMES = FALSE)
data <- rbindlist(lapply(files, FUN = process_file))
df <- data[, .(`Daily change in cumulative total` = .N), Date]
setorder(df, Date)

df[, Country := "Peru"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://datos.ins.gob.pe/dataset?q=%22pruebas+moleculares%22&organization=covid-19"]
df[, `Source label` := "National Institute of Health"]
df[, `Testing type` := "PCR only"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Peru.csv")
