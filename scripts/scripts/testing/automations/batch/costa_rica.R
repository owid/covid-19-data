library(dplyr)

minus <- function(x) sum(x[1],na.rm=T) - sum(x[2],na.rm=T)

date <- Sys.Date() - 1
date1 <- format(date, "%Y_%m_%d")

date <- Sys.Date() - 1
date2 <- format(date, "%m_%d_%y")

url <- paste("https://geovision.uned.ac.cr/oges/archivos_covid/",date1,"/",date2,"_CSV_GENERAL.csv", sep = "")

df <- fread(url, showProgress = FALSE, 
            select = c("nue_posi", "conf_nexo","nue_descar", "FECHA"))

df$lab_pos <- apply(df[,c("nue_posi","conf_nexo")],1,minus)
df$sum <- rowSums(df[,c("lab_pos", "nue_descar")], na.rm=TRUE)

df[, `Positive rate` := round(frollsum(lab_pos, 7) / frollsum(sum, 7), 3)]

df <- select(df, sum, FECHA, `Positive rate`) 
df <- df[df$sum != 0,]

setnames(df, c("Daily change in cumulative total", "Date", "Positive rate"))

df[, Date := dmy(Date)]
df[, Country := "Costa Rica"]
df[, Units := "people tested"]
df[, `Source URL` := "https://geovision.uned.ac.cr/oges/"]
df[, `Source label` := "Ministry of Health"]
df[, Notes := NA_character_]

setorder(df, -Date)

fwrite(df, "automated_sheets/Costa_Rica.csv")
