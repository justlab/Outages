suppressPackageStartupMessages(
   {library(DBI)
    library(data.table)
    loadNamespace("lubridate")
    loadNamespace("jsonlite")})

config = jsonlite::fromJSON(here::here("config.json"))

outages = function()
  # Read outage data from the database and return it as a data table.
   {db = dbConnect(RSQLite::SQLite(), config$db.path)
    on.exit(dbDisconnect(db))
    message("Loading outages")
    d = as.data.table(dbGetQuery(db, sprintf(
        "select
            time, ilon / 1e%d as lon, ilat / 1e%d as lat,
                etr, cust_a, %s
            from Events",
        config$polyline.precision, config$polyline.precision,
        paste(config$enum.cols, collapse = ", "))))
    for (ec in config$enum.cols)
        d[, (ec) := structure(class = "factor",
            get(ec) + 1,
            .Label = dbGetQuery(db, sprintf(
                "select meaning from Enumeration_%s order by code",
                ec))[[1]])]
    for (tc in c("time", "etr"))
        d[, (tc) := lubridate::as_datetime(get(tc))]
    setkey(d, time, lon, lat)
    d}
