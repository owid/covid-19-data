add_smoothed_series <- function(collated) {

    # Identify gaps of >3 weeks
    testing_dates <- collated$Date
    gaps <- testing_dates - lag(testing_dates)
    to_be_removed_idx <- which(gaps > 21)

    # Create empty time series to fill
    start_date <- min(collated$Date)
    end_date <- max(collated$Date)
    date_seq <- seq.Date(from = start_date, to = end_date, by = "day")

    # Remove long gaps
    for (idx in to_be_removed_idx) {
        date_seq <- date_seq[!(date_seq > testing_dates[idx-1] & date_seq < testing_dates[idx])]
    }

    time_series <- tibble(
        Country = collated$Country[1],
        Units = collated$Units[1],
        Sheet = collated$Sheet[1],
        Population = as.integer(collated$Population[1]),
        Date = date_seq
    )

    collated <- time_series %>%
        left_join(collated, c("Country", "Units", "Sheet", "Population", "Date")) %>%
        arrange(Date)

    if (any(!is.na(collated$`Cumulative total`))) {
        collated <- collated %>%
            mutate(total_interpolated = na_interpolation(`Cumulative total`, option = "linear")) %>%
            mutate(daily_interpolated = if_else(Date == (lag(Date, 1) + 1), total_interpolated - lag(total_interpolated, 1), NA_real_)) %>%
            select(-total_interpolated)
    } else {
        collated <- collated %>%
            mutate(daily_interpolated = `Daily change in cumulative total`)
    }

     collated <- collated %>%
        mutate(`7-day smoothed daily change` = round(frollmean(daily_interpolated, n = 7, align = "right"))) %>%
        mutate(`7-day smoothed daily change` = if_else(`7-day smoothed daily change` >= 0, `7-day smoothed daily change`, NA_real_)) %>%
        mutate(`7-day smoothed daily change per thousand` = round(1000 * `7-day smoothed daily change` / Population, 3)) %>%
        select(-daily_interpolated)

    return(collated)
}
