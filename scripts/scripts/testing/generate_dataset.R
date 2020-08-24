library(data.table)
library(dplyr)
library(ggplot2)
library(googlesheets4)
library(imputeTS)
library(lubridate)
library(readr)
library(rjson)
library(slackr)
library(stringr)
library(tidyr)
library(zoo)
rm(list = ls())

NOTIFY <- FALSE

TESTING_FOLDER <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(TESTING_FOLDER)
CONFIG <- fromJSON(file = "testing_dataset_config.json")
Sys.setlocale("LC_TIME", "en_US")

# Utils
source("smoother.R")

# Create timestamps
now_uk <- now(tzone = "Europe/London")
ts <- now_uk %>% str_replace_all("[-:]", "") %>% str_replace_all(" ", "-")
update_time <- floor_date(now(tzone = "Europe/London"), unit = "hours") %>%
    format.Date("%B %e, %Y %H:%M") %>%
    str_squish()

# Offset date for grapher dataset
origin_date <- ymd("2020-01-21")

population <- fread("../../input/un/population_2020.csv")
population <- population[, .(Country = entity, Population = population)]
world_population <- population[Country == "World", Population]

# Find sheets marked as Collate = TRUE in METADATA
gs4_auth(email = CONFIG$google_credentials_email)
key <- CONFIG$covid_time_series_gsheet
metadata <- read_sheet(key, sheet = "METADATA", range = "A2:J300") %>%
    filter(Collate == TRUE)
stopifnot("Detailed description" %in% names(metadata))
fwrite(metadata, sprintf("%s/backups/METADATA.csv", CONFIG$internal_shared_folder))
sheet_names <- sort(metadata$Sheet)

parse_country <- function(sheet_name) {
    message(sheet_name)
    is_automated <- metadata %>% filter(Sheet == sheet_name) %>% pull("Automated")
    stopifnot(length(is_automated) == 1)

    if (is_automated) {
        filepath <- sprintf("automated_sheets/%s.csv", sheet_name)
        collated <- suppressMessages(read_csv(filepath))
    } else {
        collated <- suppressMessages(read_sheet(key, sheet = sheet_name))
    }

    stopifnot(length(table(collated$Units)) == 1)
    stopifnot(length(table(collated$`Testing type`)) == 1)
    stopifnot(collated$`Testing type`[1] %in% c("PCR only", "unclear", "includes non-PCR"))
    stopifnot(collated$Units[1] %in% c("people tested", "samples tested", "tests performed", "units unclear", "tests performed (CDC)"))

    collated <- collated %>%
        mutate(Units = if_else(`Testing type` == "includes non-PCR", sprintf("%s (incl. non-PCR)", Units), Units)) %>%
        select(-`Testing type`)

    collated <- collated %>%
        filter(!is.na(Country) & !is.na(Date)) %>%
        select(Country, Units, Date, `Source URL`, `Source label`, Notes,
               matches("^Cumulative total$"),
               matches("^Daily change in cumulative total$"))

    fwrite(collated, sprintf("%s/backups/%s.csv", CONFIG$internal_shared_folder, sheet_name))

    collated <- collated %>%
        inner_join(population, by = "Country") %>%
        arrange(Date) %>%
        mutate(Sheet = sheet_name) %>%
        mutate(Date = date(Date))

    stopifnot(nrow(collated) > 0)

    # Calculate daily change when absent
    if (!"Daily change in cumulative total" %in% names(collated)) {
        collated <- collated %>%
            mutate(`Daily change in cumulative total` = `Cumulative total` - lag(`Cumulative total`, 1, default = 0)) %>%
            mutate(`Daily change in cumulative total` = if_else(lag(Date, 1) + duration(1, "day") == Date,
                                                                `Daily change in cumulative total`, NA_real_))
    }

    # Calculate cumulative total when absent
    if (!"Cumulative total" %in% names(collated)) {
        collated <- collated %>%
            mutate(`Daily change in cumulative total` = if_else(is.na(`Daily change in cumulative total`), 0,
                                                                `Daily change in cumulative total`)) %>%
            mutate(`Cumulative total` = cumsum(`Daily change in cumulative total`))
    }

    # Calculate rates per capita
    collated <- collated %>%
        mutate(`Cumulative total per thousand` = round(1000 * `Cumulative total` / `Population`, 3)) %>%
        mutate(`Daily change in cumulative total per thousand` = round(1000 * `Daily change in cumulative total` / `Population`, 3))

    collated <- collated %>%
        arrange(Date)

    # For each country, if the daily change in the most recent row is less than 50% of the daily
    # change of the day before, the last day is removed from the series. This only applies to the
    # last day of data, so if new data appears and that low daily change remains unchanged,
    # it will not be removed anymore. This means we won’t accidentally remove genuinely low changes.
    last_dailies <- tail(collated$`Daily change in cumulative total`, 2)
    if (all(!is.na(last_dailies)) & (last_dailies[2] < (last_dailies[1] * 0.5))) {
        collated <- head(collated, -1)
    }

    # Remove NA cumulative totals unless all of them are NA
    if (any(!is.na(collated$`Cumulative total`))) {
        collated <- collated %>%
            filter(!is.na(`Cumulative total`))
    }

    # Add number of observations per country
    collated <- collated %>%
        mutate(`Number of observations` = 1:nrow(collated))

    collated <- add_smoothed_series(collated)

    # Sanity checks
    setDT(collated)
    if (any(collated$`Daily change in cumulative total` == 0, na.rm = TRUE)) {
        View(collated)
        stop("At least one daily change == 0")
    }
    repeated <- collated[, .N, Date][N>1]
    if (nrow(repeated) > 0) {
        View(repeated)
        stop("Duplicate date")
    }

    return(collated)
}

# Process all countries
collated <- rbindlist(lapply(sheet_names, FUN = parse_country), use.names = TRUE)
collated[, Entity := paste(Country, "-", Units)]
setorder(collated, Country, Units, Date)

# Calculate population coverage
pop_df <- collated[, .(update_date = max(Date)), c("Country", "Population")]
world_population_covered_1d <- (100 * sum(pop_df[update_date >= today() - 1, Population]) / world_population) %>%
    round() %>% as.character()
world_population_covered_7d <- (100 * sum(pop_df[update_date >= today() - 7, Population]) / world_population) %>%
    round() %>% as.character()
world_population_covered_anytime <- (100 * sum(pop_df[, Population]) / world_population) %>%
    round() %>% as.character()
collated[, Population := NULL]
date_1d <- format.Date(today() - 1, "%e %B %Y") %>% str_squish()
date_7d <- format.Date(today() - 7, "%e %B %Y") %>% str_squish()

# Change URLs and Notes based on audit
source("replace_audited_metadata.R")

# Add ISO codes
add_iso_codes <- function(df) {
    iso <- fread("../../input/iso/iso3166_1_alpha_3_codes.csv")
    setnames(iso, c("ISO code", "Country"))
    stopifnot(all(df$Country %in% iso$Country))
    df <- merge(iso, df, by = "Country")
    return(df)
}
collated <- add_iso_codes(collated)

add_case_ratios <- function(df) {
    # Import cases from latest online version rather than local to avoid desync
    confirmed_cases <- fread("https://covid.ourworldindata.org/data/ecdc/total_cases.csv", showProgress = FALSE)
    setnames(confirmed_cases, "date", "Date")
    confirmed_cases <- gather(confirmed_cases, Country, cases, 2:ncol(confirmed_cases))
    setDT(confirmed_cases)
    confirmed_cases[, Date := ymd(Date)]
    setorder(confirmed_cases, Country, Date)
    confirmed_cases[, cases := na_locf(cases, na_remaining = "keep"), Country]
    confirmed_cases[, ra7d_new_cases := frollmean(cases - shift(cases, 1), n = 7, align = "right"), Country]
    confirmed_cases[ra7d_new_cases < 0, ra7d_new_cases := NA_real_]

    # Drop observations of all tests per case and positivity variables for countries
    # that include positive antibody results in their confirmed cases
    confirmed_cases <- confirmed_cases[!Country %in% c("Peru", "Ecuador", "Brazil", "Costa Rica")]
    df <- merge(df, confirmed_cases, by = c("Country", "Date"), all.x = TRUE)

    # Tests per cases
    df[, `Cumulative tests per case` := round(`Cumulative total` / cases, 3)]
    df[`Cumulative tests per case` == Inf | `Cumulative tests per case` < 1, `Cumulative tests per case` := NA]
    df[, `Short-term tests per case` := round(`7-day smoothed daily change` / ra7d_new_cases, 3)]
    df[`Short-term tests per case` == Inf | `Short-term tests per case` < 1, `Short-term tests per case` := NA]

    # Positive rate
    df[, `Cumulative positive rate` := round(cases / `Cumulative total`, 3)]
    df[`Cumulative positive rate` > 1, `Cumulative positive rate` := NA]
    df[, `Short-term positive rate` := round(ra7d_new_cases / `7-day smoothed daily change`, 3)]
    df[`Short-term positive rate` > 1, `Short-term positive rate` := NA]

    df[, c("cases", "ra7d_new_cases") := NULL]
    return(df)
}
collated <- add_case_ratios(collated)

# Make grapher version
grapher <- collated[, .(
    country = Country,
    date = Date,
    annotation = Units,
    total_tests = `Cumulative total`,
    total_tests_per_thousand = `Cumulative total per thousand`,
    new_tests = `Daily change in cumulative total`,
    new_tests_per_thousand = `Daily change in cumulative total per thousand`,
    new_tests_7day_smoothed = `7-day smoothed daily change`,
    new_tests_per_thousand_7day_smoothed = `7-day smoothed daily change per thousand`,
    cumulative_tests_per_case = `Cumulative tests per case`,
    cumulative_positivity_rate = `Cumulative positive rate`,
    short_term_tests_per_case = `Short-term tests per case`,
    short_term_positivity_rate = `Short-term positive rate`,
    testing_observations = `Number of observations`
)]

# How many days old is the observation
# For each country-date it subtracts the date of the observation
# from the date of the most recent observation in the dataset.
most_recent_obs_date <- grapher[, max(date)]
grapher[!is.na(total_tests), days_since_observation := as.integer(most_recent_obs_date - date)]

# Add attempted countries
source("attempts.R")

# Observations found, with zero for attempted countries
grapher[, observations_found := testing_observations]
grapher <- rbindlist(list(
    grapher,
    data.table(
        country = attempts$Entity,
        date = max(grapher$date),
        observations_found = 0
    )
), use.names = TRUE, fill = TRUE)

# Convert date to fake year format for OWID grapher
grapher[, date := as.integer(difftime(date, origin_date, units = "days"))]
setnames(grapher, "date", "year")

# Remove secondary series
secondary_series <- fread("../../input/owid/secondary_testing_series.csv")
for (i in 1:nrow(secondary_series)) {
    sec_country <- secondary_series[i, location]
    sec_annotation <- secondary_series[i, tests_units]
    concatenated <- sprintf("%s, %s", sec_country, sec_annotation)
    grapher[country == sec_country & annotation == sec_annotation, country := concatenated]
    grapher[country == concatenated, annotation := NA_character_]
}

copy_paste_annotation <- unique(grapher[!is.na(annotation), .(country, annotation)])
copy_paste_annotation <- paste(copy_paste_annotation$country, copy_paste_annotation$annotation, sep = ": ")
writeLines(copy_paste_annotation, sprintf("%s/copy_paste_annotation.txt", CONFIG$internal_shared_folder))

# Write grapher file
fwrite(grapher, sprintf("%s/grapher-latest/grapher-latest.csv", CONFIG$internal_shared_folder))
fwrite(grapher, sprintf("%s/grapher-history/grapher-%s.csv", CONFIG$internal_shared_folder, ts))

# Make public version
public <- copy(collated)
public[, c("Country", "Units") := NULL]
public_latest <- merge(public, metadata)
public_latest[, c("Sheet", "Ready for review", "Collate") := NULL]
setorder(public_latest, Entity, -Date)
public_latest <- public_latest[, .SD[1], Entity]
public[, c("Sheet", "Number of observations", "Cumulative tests per case", "Cumulative positive rate") := NULL]
setcolorder(public, "Entity")

# Generate HTML code for WordPress
source("generate_html.R")

# Write public version as Excel file
public_latest <- public_latest[, c(
    "ISO code", "Entity", "Date", "Source URL", "Source label", "Notes",
    "Number of observations", "Cumulative total", "Cumulative total per thousand",
    "Daily change in cumulative total", "Daily change in cumulative total per thousand",
    "7-day smoothed daily change", "7-day smoothed daily change per thousand",
    "Short-term positive rate", "Short-term tests per case",
    "General source label", "General source URL", "Short description", "Detailed description"
)]

fwrite(public_latest, "../../../public/data/testing/covid-testing-latest-data-source-details.csv")
rio::export(public_latest, "../../../public/data/testing/covid-testing-latest-data-source-details.xlsx")
fwrite(public, "../../../public/data/testing/covid-testing-all-observations.csv")
rio::export(public, "../../../public/data/testing/covid-testing-all-observations.xlsx")

# For each day if >1 version then delete old versions
clean_folder <- function(path) {
    filename <- list.files(path = path, full.names = TRUE)
    versions <- data.table(filename)
    versions[, date := str_extract(filename, "2020\\d{4}")]
    latest <- versions[, max(filename), date]$V1
    to_delete <- versions[!filename %in% latest]$filename
    file.remove(to_delete)
}
clean_folder(sprintf("%s/grapher-history/", CONFIG$internal_shared_folder))

# Log to txt file
log <- sprintf("%s\n\n%s series from %s countries were included in the output\nUpdate time: %s (British time)",
               paste0(public_latest$Entity, collapse = "\n"),
               nrow(public_latest),
               length(countries),
               now_uk)
writeLines(log, sprintf("%s/LOGFILE.txt", CONFIG$internal_shared_folder))

# Make sanity check graph
ggplot(data = public[!is.na(`Cumulative total`)],
       aes(x = Date, y = `Cumulative total`, group = Entity)) +
    geom_line(alpha = 0.5, color="black") +
    geom_point(alpha = 0.5, color="black") +
    scale_y_log10() +
    ggsave(sprintf("%s/sanitycheck_total.png", CONFIG$internal_shared_folder), width = 10, height = 6)

old_updates <- head(setorder(public[, .(LastUpdate = max(Date)), Entity], LastUpdate), 10)
old_updates <- paste0(old_updates$Entity, ": ", old_updates$LastUpdate, collapse = "\n")

# Send Slack message
if (NOTIFY == TRUE) {
    slack_log <- sprintf(
        "```%s series from %s countries were included in the output\nUpdate time: %s (British time)\n\nLeast up-to-date time series:\n%s```",
        nrow(public_latest),
        length(countries),
        now_uk,
        old_updates
    )
    slackr_setup(config_file = CONFIG$slack_credentials)
    slackr_upload(sprintf("%s/sanitycheck_total.png", CONFIG$internal_shared_folder),
                  channel = CONFIG$slack_channel, title = "Sanity check", initial_comment = slack_log)
}

message(update_time)
