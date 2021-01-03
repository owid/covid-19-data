library(data.table)
library(googlesheets4)
library(imputeTS)
library(lubridate)
library(readr)
library(retry)
library(rjson)
library(stringr)
library(tidyr)
rm(list = ls())

setwd(dirname(rstudioapi::getSourceEditorContext()$path))
CONFIG <- fromJSON(file = "vax_dataset_config.json")
Sys.setlocale("LC_TIME", "en_US")
gs4_auth(email = CONFIG$google_credentials_email)
GSHEET_KEY <- CONFIG$vax_time_series_gsheet

subnational_pop <- fread("../../input/owid/subnational_population_2020.csv", select = c("location", "population"))

get_metadata <- function() {
    retry(
        expr = {metadata <- data.table(read_sheet(GSHEET_KEY, sheet = "LOCATIONS"))},
        when = "RESOURCE_EXHAUSTED",
        max_tries = 10,
        interval = 20
    )
    metadata <- metadata[include == TRUE]
    setorder(metadata, location)
    return(metadata)
}

add_world <- function(df) {
    world <- df[!location %in% subnational_pop$location]
    world <- spread(world, location, total_vaccinations)
    world <- gather(world, location, total_vaccinations, 2:ncol(world))
    setDT(world)
    setorder(world, location, date)
    world[, total_vaccinations := na_locf(total_vaccinations, na_remaining = "keep"), location]
    world <- world[, .(total_vaccinations = sum(total_vaccinations, na.rm = TRUE)), date]
    world <- world[, .(date = min(date)), total_vaccinations]
    world[, location := "World"]
    df <- rbindlist(list(df, world), use.names = TRUE)
    return(df)
}

add_smoothed <- function(df) {
    if (df$location[1] == "World") return(df)
    setorder(df, date)
    date_seq <- seq.Date(from = min(df$date), to = max(df$date), by = "day")
    time_series <- data.table(date = date_seq, location = df$location[1])
    if ("vaccine" %in% names(df)) time_series[, vaccine := df$vaccine[1]]
    df <- merge(df, time_series, all = TRUE, c("date", "location"))
    setorder(df, date)
    df[, total_interpolated := na_interpolation(total_vaccinations, option = "linear")]
    df[, new_interpolated := total_interpolated - shift(total_interpolated, 1)]
    windows <- head(c(0:6, rep(7, 1e4)), nrow(df))
    df[, new_vaccinations_smoothed := round(frollmean(new_interpolated, n = windows, adaptive = TRUE), 3)]
    df[, c("total_interpolated", "new_interpolated") := NULL]
    return(df)
}

process_location <- function(location_name) {
    message(location_name)
    is_automated <- metadata[location == location_name, automated]
    if (is_automated) {
        filepath <- sprintf("automations/output/%s.csv", location_name)
        df <- data.table(suppressMessages(read_csv(filepath)))
    } else {
        retry(
            expr = {df <- suppressMessages(read_sheet(GSHEET_KEY, sheet = location_name))},
            when = "RESOURCE_EXHAUSTED",
            max_tries = 10,
            interval = 20
        )
        setDT(df)
    }
    df <- df[, c("location", "date", "vaccine", "total_vaccinations", "source_url")]
    df[, date := date(date)]

    setorder(df, date)
    fwrite(df, sprintf("../../../public/data/vaccinations/country_data/%s.csv", location_name), scipen = 999)
    return(df)
}

add_per_capita <- function(df) {
    pop <- fread("../../input/un/population_2020.csv", select = c("entity", "population"), col.names = c("location", "population"))
    pop <- rbindlist(list(pop, subnational_pop))

    df <- merge(df, pop)

    df[, total_vaccinations_per_hundred := round(total_vaccinations * 100 / population, 2)]
    df[, new_vaccinations_smoothed_per_million := round(new_vaccinations_smoothed * 1000000 / population, 2)]

    df[, population := NULL]
    return(df)
}

add_iso <- function(df) {
    iso_codes <- fread("../../input/iso/iso3166_1_alpha_3_codes.csv")
    df <- merge(iso_codes, df, by = "location", all.y = TRUE)
    return(df)
}

improve_metadata <- function(metadata, vax) {
    setorder(vax, date)
    vax_per_loc <- vax[, .(vaccines = paste0(sort(unique(unlist(str_split(vaccine, ", ")))), collapse = ", ")), location]
    latest_meta <- vax[, .SD[.N], location]
    metadata <- merge(merge(metadata, vax_per_loc, "location"), latest_meta, "location")
    metadata[is.na(source_website), source_website := source_url]
    setnames(metadata, "date", "last_observation_date")
    metadata[, c("automated", "include", "total_vaccinations", "vaccine", "source_url") := NULL]
    metadata <- add_iso(metadata)
    return(metadata)
}

generate_locations_file <- function(metadata) {
    metadata <- metadata[, c("location", "iso_code", "source_name", "source_website", "vaccines", "last_observation_date")]
    fwrite(metadata, "../../../public/data/vaccinations/locations.csv")
}

generate_vaccinations_file <- function(vax) {
    vax <- add_iso(vax)
    setnames(vax, c("new_vaccinations_smoothed", "new_vaccinations_smoothed_per_million"),
             c("daily_vaccinations", "daily_vaccinations_per_million"))
    fwrite(vax, "../../../public/data/vaccinations/vaccinations.csv", scipen = 999)
}

generate_grapher_file <- function(grapher) {
    setnames(grapher, c("date", "location"), c("year", "country"))
    setcolorder(grapher, c("country", "year"))
    grapher[, year := as.integer(year - ymd("2020-01-21"))]
    fwrite(grapher, "../../grapher/COVID-19 - Vaccinations.csv", scipen = 999)
}

generate_html <- function(metadata) {
    html <- copy(metadata)
    html[, location := paste0("<tr><td><strong>", location, "</strong></td>")]
    html[, last_observation_date := paste0("<td>", str_squish(format.Date(last_observation_date, "%B %e, %Y")), "</td>")]
    html[, vaccines := paste0("<td>", vaccines, "</td></tr>")]
    html[, source := paste0('<td><a href="', source_website, '">', source_name, '</a></td>')]
    html <- html[, c("location", "source", "last_observation_date", "vaccines")]
    setnames(html, c("Location", "Source", "Last observation date", "Vaccines"))
    header <- paste0("<tr>", paste0("<th>", names(html), "</th>", collapse = ""), "</tr>")
    html[, body := paste0(Location, Source, `Last observation date`, Vaccines)]
    body <- paste0(html$body, collapse = "")
    html_table <- paste0("<table><tbody>", header, body, "</tbody></table>")
    writeLines(html_table, "automations/source_table.html")
}

metadata <- get_metadata()
vax <- lapply(metadata$location, FUN = process_location)
vax <- rbindlist(vax, use.names=TRUE)

metadata <- improve_metadata(metadata, vax)

# Aggregate across all vaccines
vax <- vax[, .(total_vaccinations = sum(total_vaccinations)), c("date", "location")]

# Global figures
vax <- add_world(vax)

# Derived variables
vax <- rbindlist(lapply(split(vax, by = "location"), FUN = add_smoothed), fill = TRUE)
vax <- add_per_capita(vax)

setorder(vax, location, date)
generate_vaccinations_file(copy(vax))
generate_grapher_file(copy(vax))
generate_locations_file(metadata)
generate_html(metadata)
