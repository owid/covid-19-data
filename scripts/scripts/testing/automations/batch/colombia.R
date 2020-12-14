df <- fread("https://www.datos.gov.co/resource/8835-5baf.csv", showProgress = FALSE,
            select = c("fecha", "positivas_acumuladas", "negativas_acumuladas"))

df <- df[fecha != "Acumulado Feb"]
df[, Date := date(ymd_hms(fecha))]

df[, `Cumulative total` := positivas_acumuladas + negativas_acumuladas]

setorder(df, Date)
df[, `Positive rate` := frollmean(positivas_acumuladas - shift(positivas_acumuladas, 1), 7) / frollmean(`Cumulative total` - shift(`Cumulative total`, 1), 7)]

df <- df[, c("Date", "Cumulative total", "Positive rate")]
df <- df[!is.na(`Cumulative total`)]

df[, Country := "Colombia"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://www.datos.gov.co/Salud-y-Protecci-n-Social/Pruebas-PCR-procesadas-de-COVID-19-en-Colombia-Dep/8835-5baf"]
df[, `Source label` := "National Institute of Health"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Colombia.csv")
