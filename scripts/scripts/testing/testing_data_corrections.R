# January 2021: very large changes in cumulative tests in Austria, leading to unrealistic PRs
collated[Country == "Austria" & Date >= "2021-01-01", `Short-term positive rate` := NA]
collated[Country == "Austria" & Date >= "2021-01-01", `Short-term tests per case` := NA]

# Ecuador: the PR should start from Sept 2020, because the case data prior to that
# included cases confirmed without PCR tests (while our data only includes PCR tests)
collated[Country == "Ecuador" & Date < "2020-09-14", `Short-term positive rate` := NA]
collated[Country == "Ecuador" & Date < "2020-09-14", `Short-term tests per case` := NA]

# Mauritania: the test definition does not match the case definition (screening tests possibly included in testing figures)
collated[Country == "Mauritania", `Short-term positive rate` := NA]
collated[Country == "Mauritania", `Short-term tests per case` := NA]

# United Arab Emirates: the test definition does not match the case definition (non-diagnostic tests possibly included in testing figures)
collated[Country == "United Arab Emirates", `Short-term positive rate` := NA]
collated[Country == "United Arab Emirates", `Short-term tests per case` := NA]

# Lebanon: the test definition does not match the case definition (testing figures exclude antigen tests, which can be used to diagnose cases of COVID-19)
collated[Country == "Lebanon", `Short-term positive rate` := NA]
collated[Country == "Lebanon", `Short-term tests per case` := NA]

# Iceland: the test definition does not match the case definition (confirmed cases does not exclude positive results from tests included in the testing figure)
collated[Country == "Iceland", `Short-term positive rate` := NA]
collated[Country == "Iceland", `Short-term tests per case` := NA]
