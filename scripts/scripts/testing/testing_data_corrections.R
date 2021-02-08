# January 2021: very large changes in cumulative tests in Austria, leading to unrealistic PRs
collated[Country == "Austria" & Date >= "2021-01-01", `Short-term positive rate` := NA]
collated[Country == "Austria" & Date >= "2021-01-01", `Short-term tests per case` := NA]

# Ecuador: the PR should start from Sept 2020, because the case data prior to that
# included cases confirmed without PCR tests (while our data only includes PCR tests)
collated[Country == "Ecuador" & Date < "2020-09-14", `Short-term positive rate` := NA]
collated[Country == "Ecuador" & Date < "2020-09-14", `Short-term tests per case` := NA]
