df <- rjson::fromJSON(file = "https://raw.githubusercontent.com/datameet/covid19/master/data/icmr_testing_status.json")

process_entry <- function(entry) {
    entry <- entry$value
    return(data.table(
        Date = str_sub(entry$report_time, 1, 10),
        Samples = entry$samples
    ))
}

df <- rbindlist(lapply(df$rows, FUN = process_entry), fill = TRUE)
setorder(df, Samples)
df <- df[, .SD[1], Samples]
setorder(df, Date)
df <- df[, .SD[1], Date]

samples <- data.table(
    Date = df$Date,
    `Cumulative total` = df$Samples,
    Country = "India",
    Units = "samples tested",
    `Source label` = "Indian Council of Medical Research",
    `Source URL` = "https://github.com/datameet/covid19",
    Notes = "Made available by DataMeet on GitHub"
)

fwrite(samples, "automated_sheets/India - Samples tested.csv")
