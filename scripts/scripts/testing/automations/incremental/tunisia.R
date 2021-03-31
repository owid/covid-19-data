url <- "https://onmne.tn"

page <- read_html(url)

count <- page %>%
  html_nodes("span.vcex-milestone-time") %>%
  .[[1]] %>%
  html_attr("data-options") %>%
  str_remove(fixed("{\"startVal\":0,\"endVal\":")) %>%
  str_remove(fixed(",\"duration\":2,\"decimals\":0,\"separator\":\"\",\"decimal\":\".\"}")) %>%
  as.integer()

add_snapshot(
  count = count,
  sheet_name = "Tunisia",
  country = "Tunisia",
  units = "people tested",
  source_url = url,
  source_label = "Tunisia Minsitry of Health",
  testing_type = "unclear"
)
