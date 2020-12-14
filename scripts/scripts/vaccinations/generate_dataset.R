library(data.table)
library(googlesheets4)
library(imputeTS)
library(lubridate)
library(retry)
library(rjson)
rm(list = ls())

setwd(dirname(rstudioapi::getSourceEditorContext()$path))
CONFIG <- fromJSON(file = "vax_dataset_config.json")
Sys.setlocale("LC_TIME", "en_US")
gs4_auth(email = CONFIG$google_credentials_email)
GSHEET_KEY <- CONFIG$vax_time_series_gsheet
VACCINE_LIST <- c("pfizer_biontech")

get_metadata <- function() {
    metadata <- data.table(read_sheet(GSHEET_KEY, sheet = "LOCATIONS"))
    metadata <- metadata[include == TRUE]
    setorder(metadata, location)
    return(metadata)
}

add_world <- function(df) {
    world <- df[, .(total_vaccinations = sum(total_vaccinations)), date]
    world[, location := "World"]
    df <- rbindlist(list(df, world), use.names = TRUE)
    return(df)
}

add_daily <- function(df) {
    setorder(df, date)
    df[, new_vaccinations := (total_vaccinations - shift(total_vaccinations, 1))]
    df[date != shift(date, 1) + 1, new_vaccinations := NA_integer_]
    return(df)
}

add_smoothed <- function(df) {
    setorder(df, date)
    date_seq <- seq.Date(from = min(df$date), to = max(df$date), by = "day")
    time_series <- data.table(date = date_seq, location = df$location[1])
    if ("vaccine" %in% names(df)) time_series[, vaccine := df$vaccine[1]]
    df <- merge(df, time_series, all = TRUE)
    setorder(df, date)
    df[, total_interpolated := na_interpolation(total_vaccinations, option = "linear")]
    df[, new_interpolated := total_interpolated - shift(total_interpolated, 1)]
    df[, new_vaccinations_smoothed := round(frollmean(new_interpolated, 7), 3)]
    df[, c("total_interpolated", "new_interpolated") := NULL]
    return(df)
}

process_location <- function(location_name) {
    message(location_name)
    is_automated <- metadata[location == location_name, automated]
    if (is_automated) {
        filepath <- sprintf("automated_sheets/%s.csv", location_name)
        df <- fread(filepath, showProgress = FALSE)
    } else {
        retry(
            expr = {df <- suppressMessages(read_sheet(GSHEET_KEY, sheet = location_name))},
            when = "RESOURCE_EXHAUSTED",
            max_tries = 5,
            interval = 100
        )
        setDT(df)
    }
    df[, date := date(date)]

    # Sanity checks
    stopifnot(length(setdiff(df$vaccine, c(VACCINE_LIST, "total"))) == 0)

    # Derived variables
    df <- rbindlist(lapply(split(df, by = "vaccine"), FUN = add_daily))
    df <- rbindlist(lapply(split(df, by = "vaccine"), FUN = add_smoothed))

    fwrite(df, sprintf("../../../public/data/vaccinations/country_data/%s.csv", location_name), scipen = 999)
    return(df)
}

add_per_capita <- function(df) {
    pop <- fread("../../input/un/population_2020.csv", select = c("entity", "population"), col.names = c("location", "population"))
    df <- merge(df, pop)
    for (metric in c("total_vaccinations", "new_vaccinations", "new_vaccinations_smoothed")) {
        df[[sprintf("%s_per_thousand", metric)]] <- round(df[[metric]] * 1000 / df$population, 3)
    }
    df[, population := NULL]
    return(df)
}

generate_locations_file <- function(metadata) {
    metadata <- metadata[, c("location", "vaccines", "source_name", "source_website")]
    fwrite(metadata, "../../../public/data/vaccinations/locations.csv")
}

generate_vaccinations_file <- function(vax) {
    fwrite(vax, "../../../public/data/vaccinations/vaccinations.csv", scipen = 999)
}

generate_grapher_file <- function(grapher) {
    setnames(grapher, c("date", "location"), c("year", "country"))
    setcolorder(grapher, c("country", "year"))
    grapher[, year := as.integer(year - ymd("2020-01-21"))]
    fwrite(grapher, "../../../public/data/vaccinations/COVID-19 - Vaccinations.csv", scipen = 999)
}

metadata <- get_metadata()
vax <- rbindlist(lapply(metadata$location, FUN = process_location))
vax <- vax[, .(total_vaccinations = sum(total_vaccinations)), c("date", "location")]

# Global figures
vax <- add_world(vax)

# Derived variables
vax <- rbindlist(lapply(split(vax, by = "location"), FUN = add_daily))
vax <- rbindlist(lapply(split(vax, by = "location"), FUN = add_smoothed))
vax <- add_per_capita(vax)

generate_locations_file(metadata)
generate_vaccinations_file(vax)
generate_grapher_file(copy(vax))
