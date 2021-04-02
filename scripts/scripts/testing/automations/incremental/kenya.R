url <- "http://covidkenya.org/"

page <- read_html(url)

count <- page %>%
  html_nodes("div.elementor-element-b36fad5") %>%
  html_text2() %>%
  str_replace_all("[^\\d]", "") %>%
  as.integer

add_snapshot(
  count = count,
  sheet_name = "Kenya",
  country = "Kenya",
  units = "samples tested",
  source_url = url,
  source_label = "Kenya Ministry of Health",
  testing_type = "unclear"
)
