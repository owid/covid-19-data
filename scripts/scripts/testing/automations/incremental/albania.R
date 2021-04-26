library(rvest)

existing <- fread("automated_sheets/Albania.csv")
existing$Date <- as.Date(existing$Date, '%d/%m/%Y')

latest <- max(as.Date(existing$Date, '%d/%m/%Y'))
date <- as.Date(format(Sys.Date(), '%Y-%m-%d'))

df <- data.frame(date=double(), daily=integer(), source=character())
i = 1

while (date > latest) {
    url <- paste('https://shendetesia.gov.al/category/lajme/page/',i,sep='')
    pages <- read_html(url) %>%
        html_nodes('.newsDetails')

    for (page in pages) {
        date <- page %>%
            html_nodes('.dateDetalils') %>%
            html_text %>%
            str_remove('Postuar më: ') %>% 
            as.Date('%d/%m/%Y')

        if (date == latest) break
        
        daily <- page %>%
            html_nodes('a') %>%
            html_text %>%
            str_extract('[\\d,]+ testime') %>%
            str_remove(' testime') %>%
            as.integer()
        
        date <- as.character(date)

        source <-  page %>%
            html_nodes('a') %>%
            html_attr('href')

        if(!is.na(daily)) {
            new <- c(date, daily, source)
            df[nrow(df) + 1, ] <- new
        }
    }
    date <- as.Date(date)
    i=i+1
}

df <- cbind(df, Country = 'Albania', Units = 'tests performed', 'Source label'='Ministry of Health and Social Protection', Notes = NA)
setnames(df, c('date', 'daily', 'source'), c('Date', 'Daily change in cumulative total', 'Source URL'))
df$Date <- as.Date(df$Date)

df <- rbind(existing, df)
setorder(df, -Date)

fwrite(df, 'automated_sheets/Albania.csv')
