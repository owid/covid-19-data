attempts <- read_sheet(CONFIG$attempted_countries_ghseet, sheet = "Sources – country by country")
setDT(attempts)
attempts <- attempts[Outcome == "We know there's no data"]
attempts <- attempts[!Entity %in% grapher$country]
attempts <- attempts[, .(Entity, `Date last tried to add`, `Notes for OWID website`)]
setorder(attempts, Entity)
