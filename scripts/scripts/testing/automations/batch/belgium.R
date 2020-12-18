df <- fread("https://epistat.sciensano.be/Data/COVID19BE_tests.csv", showProgress = FALSE)

df <- df[, .(TESTS_ALL = sum(TESTS_ALL), TESTS_ALL_POS = sum(TESTS_ALL_POS)), DATE]

setorder(df, DATE)
df[, PR := round(frollsum(TESTS_ALL_POS, 7) / frollsum(TESTS_ALL, 7), 3)]
df[, TESTS_ALL_POS := NULL]

setnames(df, c("DATE", "TESTS_ALL", "PR"), c("Date", "Daily change in cumulative total", "Positive rate"))

df[, Country := "Belgium"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://epistat.sciensano.be/Data/COVID19BE_tests.csv"]
df[, `Source label` := "Sciensano (Belgian institute for health)"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Belgium.csv")
