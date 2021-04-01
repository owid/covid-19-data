url <- "https://onmne.tn"

page <- read_html(url)

count <- page %>%
  html_node("span.vcex-milestone-time") %>%
  html_attr("data-options") %>%
  str_extract("endVal..\\d+") %>%
  str_replace_all("[^\\d]", "") %>%
  as.integer()

add_snapshot(
  count = count,
  sheet_name = "Tunisia",
  country = "Tunisia",
  units = "people tested",
  source_url = url,
  source_label = "Tunisia Ministry of Health",
  testing_type = "unclear"
)
