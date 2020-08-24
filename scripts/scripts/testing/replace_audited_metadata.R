original_shape <- dim(collated)

audited <- read_sheet(CONFIG$audit_gsheet, sheet = "AUDIT") %>%
    data.table()
audited[, Date := date(Date)]
audited <- audited[CHECKED == TRUE & (`REPLACE URL` == TRUE | `REPLACE NOTES` == TRUE)]

for (i in seq_len(nrow(audited))) {
    row <- audited[i]

    if (row$`REPLACE NOTES` == TRUE) {
        collated[Entity == row$Entity & Date == row$Date, Notes := row$`NEW NOTES`]
    }

    if (row$`REPLACE URL` == TRUE) {
        collated[Entity == row$Entity & Date == row$Date, `Source URL` := row$`NEW URL`]
    }
}

new_shape <- dim(collated)
stopifnot(original_shape == new_shape)
rm(audited)
