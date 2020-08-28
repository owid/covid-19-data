meta <- fromJSON(file = "https://healthdata.gov/api/3/action/package_show?id=c13c00e3-f3d0-4d49-8c43-bf600a6c0a0d")
url <- meta$result[[1]]$resources[[1]]$url

df <- fread(url, showProgress = FALSE, select = c("date", "new_results_reported"))
setnames(df, "date", "Date")

df <- df[, .(`Daily change in cumulative total` = sum(new_results_reported)), Date]
setorder(df, Date)

# Data presented might not represent the most current counts for the most recent 3 days
# due to the time it takes to report testing information.
df <- df[Date <= today() - 3]

df[, Country := "United States"]
df[, Units := "tests performed"]
df[, `Source label` := "Department of Health & Human Services"]
df[, `Source URL` := url]
df[, `Testing type` := "unclear"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/United States - tests performed.csv")
