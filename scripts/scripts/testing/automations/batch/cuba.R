json <- fromJSON(file = "https://raw.githubusercontent.com/covid19cubadata/covid19cubadata.github.io/master/data/covid19-cuba.json")
json <- json$casos$dias

process_record <- function(record) {
    date <- ymd(record["fecha"])
    value <- record["tests_total"] %>% unlist
    if (is.null(value)) {
        return(NULL)
    } else {
        return(data.table(
            Date = date,
            `Cumulative total` = value
        ))
    }
}

df <- rbindlist(lapply(json, FUN = process_record))
df[, Country := "Cuba"]
df[, Units := "tests performed"]
df[, `Source label` := "Ministry of Public Health"]
df[, `Source URL` := "https://covid19cubadata.github.io/#cuba"]
df[, Notes := "Made available on GitHub by covid19cubadata"]
df[, `Testing type` := "PCR only"]

fwrite(df, "automated_sheets/Cuba.csv")
