json <- fromJSON(file = "https://atlas.jifo.co/api/connectors/88d50014-8241-4771-972b-547ace9ad447")
json <- json$data[[1]]
json <- tail(json, -2)

dates <- sapply(json, FUN = "[", 1) %>% str_extract("\\d+/\\d+/\\d+") %>% dmy()
values <- sapply(json, FUN = "[", 2) %>% str_replace_all("[^\\d]", "") %>% as.integer()
stopifnot(length(dates) == length(values))

df <- data.table(`Cumulative total` = values, Date = dates)

df[, Country := "Colombia"]
df[, Units := "samples tested"]
df[, `Source URL` := "https://www.ins.gov.co/Noticias/Paginas/Coronavirus.aspx#muestras"]
df[, `Source label` := "National Institute of Health"]
df[, Notes := NA_character_]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Colombia.csv")
