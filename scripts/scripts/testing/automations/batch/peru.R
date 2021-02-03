files <- c(
    "https://datos.ins.gob.pe/dataset/52efc3cd-8d13-4fed-a524-cab71dcb971a/resource/212a2519-0af4-47e9-bec4-8db893a010d8/download/pm_mar_2020.csv", # Mar 2020
    "https://datos.ins.gob.pe/dataset/986a5144-84be-4921-b8e5-074109ae9aec/resource/13e3c778-76db-4df8-a99b-aa574f2289e0/download/pm_apr_2020.csv", # Apr 2020
    "https://datos.ins.gob.pe/dataset/79b343d3-f111-409b-ba1a-545cbb11783e/resource/3b327e52-520c-4d93-8c99-7d6c55a1a784/download/pm_may_2020.csv", # May 2020
    "https://datos.ins.gob.pe/dataset/56942bb4-1f66-4ab5-a864-d4d90436be82/resource/1ee81357-d55b-4d37-9153-6681957bb74e/download/pm_jun_2020.csv", # Jun 2020
    "https://datos.ins.gob.pe/dataset/59c21804-5b6d-46f2-884d-9cfce3a857db/resource/e5787c8f-301f-4b41-8adc-b870ae46ab23/download/pm_jul_2020.csv", # Jul 2020
    "https://datos.ins.gob.pe/dataset/23eeb1fe-be73-4592-9df9-9b020666bf18/resource/1e0b1110-aae1-43e6-9a9e-f46306d71e62/download/pm_ago_2020.csv", # Aug 2020
    "https://datos.ins.gob.pe/dataset/2bd33df7-6910-43f4-9ace-e62aed1c4be1/resource/c6e887fa-6bc8-4673-a5e7-3564afd91cf0/download/pm_set_2020.csv", # Sep 2020
    "https://datos.ins.gob.pe/dataset/1d94b98b-cc3a-41b6-a8a4-ff4622878528/resource/11e6b1a0-b200-47f9-a3b4-dc89d358a927/download/pm_oct_2020.csv", # Oct 2020
    "https://datos.ins.gob.pe/dataset/666d1d60-a737-4729-a649-a8ad9ecd235a/resource/2e8d3d4e-4815-4dcc-950d-4619b49e179d/download/pm_nov_2020.csv", # Nov 2020
    "https://datos.ins.gob.pe/dataset/47daea44-df80-4120-bca3-88a5174bfa50/resource/d8468594-383b-422c-9812-3d1f3de87574/download/pm_dic_2020.csv", # Dec 2020
    "https://datos.ins.gob.pe/dataset/910e9c26-4744-4287-87db-c1d91b01b7ff/resource/09e2bc6b-1200-46bf-9974-9ce4aec323b7/download/pm_08jan_2021.csv" # Jan 2021
)

process_file <- function(url) {
    filename <- str_extract(url, "[^/]+\\.csv$")
    local_path <- sprintf("input/peru/%s", filename)
    if (!file.exists(local_path)) {
        download.file(url = url, destfile = local_path)
    }
    df <- fread(local_path, showProgress = FALSE, select = c("FECHATOMAMUESTRA", "RESULTADO"))
    setnames(df, c("Date", "Result"))
    return(df)
}

data <- rbindlist(lapply(files, FUN = process_file))

data[, Date := str_replace(Date, "^2121", "2021")]
data[, Date := ymd(Date)]
data <- data[Date <= today()]

df <- data[, .(
    `Daily change in cumulative total` = .N,
    `Positive` = sum(Result == "POSITIVO")
), Date]

setorder(df, Date)
df[, `Positive rate` := round(frollsum(Positive, 7) / frollsum(`Daily change in cumulative total`, 7), 3)]
df[, Positive := NULL]

df[, Country := "Peru"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://datos.ins.gob.pe/dataset?q=%22pruebas+moleculares%22&organization=covid-19"]
df[, `Source label` := "National Institute of Health"]
df[, `Testing type` := "PCR only"]
df[, Notes := NA]

fwrite(df, "automated_sheets/Peru.csv")
