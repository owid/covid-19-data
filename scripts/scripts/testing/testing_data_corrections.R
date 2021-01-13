# January 2021: large negative drop in cumulative tests in Austria, leading to very high PR for one week
collated[Country == "Austria" & Date >= "2021-01-02" & Date <= "2021-01-09", `Short-term positive rate` := NA]
collated[Country == "Austria" & Date >= "2021-01-02" & Date <= "2021-01-09", `Short-term tests per case` := NA]
