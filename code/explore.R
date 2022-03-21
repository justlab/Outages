# explore some scrapped data
library(ggplot2)
library(sf)
library(mapview)

# start by loading the current data
source(here::here("code/data.R"))
d <- outages()

# what is the most recent record?
d[which.max(time),]

#  how frequently is each long/lat in the dataset?
d[, .(.N, time.min = min(time), time.max = max(time), cust_a.max = max(cust_a)), by = c("lon", "lat")][order(N, decreasing = TRUE)][1:10,]

# take a look at all the records for the outage with 119 records
ggplot(d[lon == -73.93706 & lat == 40.68850,]) + 
  aes(x = time, y = reported_problem, size = cust_a) + 
  geom_point()

# example where it is clearly multiple events (107 records)
last_plot() %+% d[lon == -73.94414 & lat == 40.65294, ]

# instance where lots of folks were impacted
d[which.max(cust_a),]
# luckily, it looks brief
d[lon == -74.12831 & lat == 40.59905, ]
# BUT -- this exact lon and lat looks like a single family home on Staten Island?? 
# (so cust_a == 1628 doesn't make sense there)
mapview(st_as_sf(d[lon == -74.12831 & lat == 40.59905, ], coords = c("lon", "lat"), crs = 4326))

# are there a lot of certain kinds of problems?
d[, table(reported_problem)]
d[, mean(cust_a), by = c("reported_problem")]

# where are these coming from overall?
mapview(st_as_sf(d, coords = c("lon", "lat"), crs = 4326))
