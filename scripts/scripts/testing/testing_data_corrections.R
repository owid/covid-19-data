# January 2021: very large changes in cumulative tests in Austria, leading to unrealistic PRs
collated[Country == "Austria" & Date >= "2021-01-01", `Short-term positive rate` := NA]
collated[Country == "Austria" & Date >= "2021-01-01", `Short-term tests per case` := NA]
