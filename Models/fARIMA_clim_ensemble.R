# Generate some ensemble forecasts based on those submitted

# 1. Start by looking in the bucket and grabbing the forecasts
# 2. Sub-sample the forecast to an equal ensemble size (n=200)
# 3. Check that it's in the EFI format
# 4. Submit all the ensemble forecast to challenge!
setwd(here::here())
library(tidyverse)
library(arrow)
library(lubridate)
source('./R/create_mme.R')

Sys.unsetenv("AWS_ACCESS_KEY_ID")
Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")

# where are the raw forecasts
s3 <- "neon4cast-forecasts/parquet/aquatics/"


# when do you want to generate the MMEs for
forecast_date <- as.character(Sys.Date() - days(1))

# generate ensembles
if (dir.exists('./Forecasts/ensembles') != T) {
  dir.create('./Forecasts/ensembles', recursive = T)
}

# Ensemble 2 = climatology + fARIMA
mme_file <- create_mme(forecast_models = c('fARIMA',
                                           'climatology'),
                       ensemble_name = 'fARIMA_clim_ensemble',
                       forecast_date = forecast_date, 
                       var = 'temperature', 
                       h = 30, 
                       theme = 'aquatics',
                       n = 200)

neon4cast::submit(file.path('./Forecasts/ensembles', mme_file), ask = F)


# # check for any missing forecasts
# message("==== Checking for missed forecasts ====")
# challenge_model_name <- 'fARIMA_clim_ensemble'
# 
# # Dates of forecasts 
# today <- paste(Sys.Date() - days(2), '00:00:00')
# this_year <- data.frame(date = as.character(paste0(seq.Date(as_date('2023-01-01'), to = as_date(today), by = 'day'), ' 00:00:00')),
#                         exists = NA)
# # issue resolved on 9 Oct 2023
# end <- as_date('2023-10-09')
# 
# # what forecasts have already been submitted?
# challenge_s3_region <- "data"
# challenge_s3_endpoint <- "ecoforecast.org"
# 
# # is that file present in the bucket?
# for (i in 1:nrow(this_year)) {
#   forecast_file <- paste0('aquatics-', as_date(this_year$date[i]), '-', challenge_model_name, '.csv.gz')
#   
#   this_year$exists[i] <- suppressMessages(aws.s3::object_exists(object = file.path("raw", 'aquatics', forecast_file),
#                                                                 bucket = "neon4cast-forecasts",
#                                                                 region = challenge_s3_region,
#                                                                 base_url = challenge_s3_endpoint))
#   
#   # if it is present, has it been submitted recently?
#   if (this_year$exists[i]) {
#     modified <- attr(suppressMessages(aws.s3::head_object(object = file.path("raw", 'aquatics', forecast_file),
#                                                           bucket = "neon4cast-forecasts",
#                                                           region = challenge_s3_region,
#                                                           base_url = challenge_s3_endpoint)), 
#                      "last-modified")
#     
#     this_year$already_rerun[i] <- ifelse(parse_date_time(gsub('GMT', '', str_split_1(modified, ', ')[2]),
#                                                          orders = "%d %b %Y %H:%M:%S") > end, 
#                                          T, F)
#   }
# }
# 
# # which dates do you need to generate forecasts for?
# # those that are missing or haven't been submitted
# missed_dates <- this_year |> 
#   filter(!(exists == T &  already_rerun == T)) |> 
#   pull(date) |> 
#   as_date()
# 
# for (i in 1:length(missed_dates)) {
#   
#   date <- missed_dates[i]
#   
#   # Ensemble 2 = climatology + fARIMA
#   mme_file <- create_mme(forecast_models = c('fARIMA',
#                                              'climatology'),
#                          ensemble_name = 'fARIMA_clim_ensemble',
#                          forecast_date = date, 
#                          var = 'temperature', 
#                          h = 30, 
#                          theme = 'aquatics',
#                          n = 200)
#   
#   if (!is.na(mme_file)) {
#     neon4cast::submit(file.path('./Forecasts/ensembles', mme_file), ask = F)
#   }  
#   
# }
