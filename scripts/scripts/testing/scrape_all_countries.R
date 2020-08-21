library(data.table)
library(googledrive)
library(googlesheets4)
library(imputeTS)
library(lubridate)
library(pdftools)
library(reticulate)
library(rio)
library(rjson)
library(rvest)
library(stringr)
library(tidyr)
rm(list = ls())

SKIP <- c()
start_after <- "arh"

TESTING_FOLDER <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(TESTING_FOLDER)
CONFIG <- fromJSON(file = "testing_dataset_config.json")

add_snapshot <- function(count, sheet_name, country, units, date = today(),
                         source_url, source_label, testing_type,
                         notes = NA_character_, daily_change = NA_integer_) {

    prev <- fread(file = sprintf("automated_sheets/%s.csv", sheet_name))
    prev[, Date := as.character(Date)]

    stopifnot(!is.na(date))
    stopifnot(is.integer(count))
    stopifnot(!is.na(count))
    stopifnot(testing_type %in% c("PCR only", "unclear", "includes non-PCR"))
    stopifnot(units %in% c("people tested", "samples tested", "tests performed", "units unclear", "tests performed (CDC)"))
    stopifnot(length(count) == 1)
    stopifnot(count >= max(prev$`Cumulative total`, na.rm = TRUE))

    if (count == max(prev$`Cumulative total`, na.rm = TRUE)) {
        return(FALSE)
    }

    new <- data.table(
        Country = country,
        Units = units,
        Date = as.character(date),
        `Cumulative total` = count,
        `Source URL` = source_url,
        `Source label` = source_label,
        `Testing type` = testing_type,
        Notes = notes
    )

    if (!is.na(daily_change)) {
        new[, `Daily change in cumulative total` := daily_change]
    }

    df <- rbindlist(list(prev, new), use.names = TRUE)
    setorder(df, -Date, -`Cumulative total`)
    df <- df[, .SD[1], Date]

    fwrite(df, sprintf("automated_sheets/%s.csv", sheet_name))
}

scripts <- list.files("automations", pattern = "\\.(R|py)$", full.names = TRUE, include.dirs = FALSE)
if (length(SKIP) > 0) scripts <- scripts[!str_detect(scripts, paste(SKIP, collapse = "|"))]
if (!is.null(start_after)) scripts <- scripts[str_extract(scripts, "[a-z_.]+(R|py)$") > start_after]

for (s in scripts) {
    rm(list = setdiff(ls(), c("scripts", "add_snapshot", "s", "CONFIG")))
    message(sprintf("%s - %s", Sys.time(), s))
    if (str_detect(s, "py$")) {
        source_python(s)
    } else {
        source(s)
    }
}
