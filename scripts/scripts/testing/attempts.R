retry(
    expr = {attempts <- read_sheet(CONFIG$attempted_countries_ghseet, sheet = "Sources – country by country")},
    when = "RESOURCE_EXHAUSTED",
    max_tries = 10,
    interval = 20
)
setDT(attempts)
attempts <- attempts[Outcome == "We know there's no data"]
attempts <- attempts[!Entity %in% grapher$country]
attempts <- attempts[, .(Entity, `Date last tried to add`, `Notes for OWID website`)]
attempts[is.na(`Notes for OWID website`), `Notes for OWID website` := "no data from official sources could be found."]
setorder(attempts, Entity)
