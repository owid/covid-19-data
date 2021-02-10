setwd(dirname(rstudioapi::getSourceEditorContext()$path))
rm(list = ls())

files <- list.files("automations/output/by_manufacturer/", pattern = "*.csv", full.names = TRUE)

df <- rbindlist(lapply(files, FUN = fread), use.names = TRUE)
setcolorder(df, c("location", "date", "vaccine", "total_vaccinations"))
setorder(df, location, date, vaccine)

approved <- c(
    "Moderna",
    "Oxford/AstraZeneca",
    "Pfizer/BioNTech"
)
stopifnot(sort(unique(df$vaccine)) == approved)

# Generate public file
fwrite(df, "../../../public/data/vaccinations/vaccinations-by-manufacturer.csv")

# Generate grapher file
df <- spread(df, vaccine, total_vaccinations)
setnames(df, c("date", "location"), c("Year", "Country"))
setcolorder(df, c("Country", "Year"))
setorder(df, "Country", "Year")
for (vax in approved) {
    setnames(df, vax, "vax")
    df[, vax := zoo::na.locf(vax, na.rm = FALSE), Country]
    df[is.na(vax), vax := 0]
    setnames(df, "vax", vax)
}
df[, Year := as.integer(ymd(Year) - ymd("2021-01-01"))]
fwrite(df, "../../grapher/COVID-19 - Vaccinations by manufacturer.csv", scipen = 999)
