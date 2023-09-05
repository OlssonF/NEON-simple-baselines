library(tsibble)
library(tidyverse)
library(neon4cast)
library(lubridate)
library(arrow)
library(fable)

source('download_noaa.R')
source('Models/TSLM_lags.R')
source('Models/ARIMA_model.R')
options(dplyr.summarise.inform = FALSE)

# Script for computing two baseline models

sites <- readr::read_csv("https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv") |> 
  dplyr::filter(aquatics == 1) |> 
  dplyr::select(field_site_id) |> 
  pull()

target_url <- "https://data.ecoforecast.org/neon4cast-targets/aquatics/aquatics-targets.csv.gz"

# when do you want to generate the forecast for
forecast_today <- Sys.Date() - days(1)

# generate ensembles
if (dir.exists('./Forecasts/') != T) {
  dir.create('./Forecasts/', recursive = T)
}


# Download the NOAA data needed
noaa_data_today <- download_noaa(sites = sites,
                                 forecast_date = forecast_today)


# fARIMA forecasts --------------------------------------------------------
# Generate the forecasts
fARIMA_file <- generate_fARIMA(team_name = 'fARIMA',
                               sites = sites,
                               var = 'temperature',
                               theme = 'aquatics',
                               forecast_date = forecast_today,
                               n = 20, 
                               h = 30, 
                               target_url = target_url, 
                               noaa_future = noaa_data_today$future, 
                               noaa_past = noaa_data_today$past)
# Submit forecast!
neon4cast::submit(forecast_file = file.path('Forecasts', fARIMA_file),
                  ask = F, s3_region = 'data', s3_endpoint = 'ecoforecast.org')

# Check for missing forecasts
message('Checking for missing ARIMA forecasts')

challenge_model_name <- 'fARIMA'


# Dates of forecasts 
today <- paste(Sys.Date() - days(2), '00:00:00')
this_year <- data.frame(date = as.character(paste0(seq.Date(as_date('2023-01-01'), to = as_date(today), by = 'day'), ' 00:00:00')),
                        exists = NA)

# what forecasts have already been submitted?
challenge_s3_region <- "data"
challenge_s3_endpoint <- "ecoforecast.org"

# is that file present in the bucket?
for (i in 1:nrow(this_year)) {
  forecast_file <- paste0('aquatics-', as_date(this_year$date[i]), '-', challenge_model_name, '.csv.gz')
  
  this_year$exists[i] <- suppressMessages(aws.s3::object_exists(object = file.path("raw", 'aquatics', forecast_file),
                                                                bucket = "neon4cast-forecasts",
                                                                region = challenge_s3_region,
                                                                base_url = challenge_s3_endpoint))
}

# which dates do you need to generate forecasts for?
missed_dates <- this_year |> 
  filter(exists == F) |> 
  pull(date) |> 
  as_date()


for (i in 1:length(missed_dates)) {
  
  forecast_date <- missed_dates[i]
  
  # Download the NOAA data needed
  noaa_data <- download_noaa(sites = sites,
                             forecast_date = forecast_date)
  
  if (!is.na(noaa_data)) {
    # Generate the forecasts
    fARIMA_file <- generate_fARIMA(team_name = 'fARIMA',
                                   sites = sites,
                                   var = 'temperature',
                                   theme = 'aquatics',
                                   forecast_date = forecast_date,
                                   n = 200, 
                                   h = 30, 
                                   target_url = target_url, 
                                   noaa_future = noaa_data$future, 
                                   noaa_past = noaa_data$past)
    # Submit forecast!
    neon4cast::submit(forecast_file = file.path('Forecasts', fARIMA_file),
                      ask = F, s3_region = 'data', s3_endpoint = 'ecoforecast.org')
  } else {
    message('Cannot submit forecast for this date')
  }
  
  
  
}

#----------------------------------------------------------#

# fTSLM_lag forecasts -----------------------------------------------------
# Generate the TSLM forecasts
message('generating todays TSML forecast')
fTSLM_file <- generate_fTSLM_lag(team_name = 'fTSLM_lag',
                                 sites = sites,
                                 var = 'temperature',
                                 theme = 'aquatics',
                                 forecast_date = forecast_date,
                                 n = 30, 
                                 h = 30, 
                                 target_url = target_url, 
                                 noaa_future = noaa_data_today$future, 
                                 noaa_past = noaa_data_today$past)
# Submit forecast!
neon4cast::submit(forecast_file = fTSLM_file,
                  ask = F, s3_region = 'data', s3_endpoint = 'ecoforecast.org')

# Check for missing forecasts
message('Checking for missing TSLM forecasts')

challenge_model_name <- 'fTSLM_lag'


# Dates of forecasts 
today <- paste(Sys.Date() - days(2), '00:00:00')
this_year <- data.frame(date = as.character(paste0(seq.Date(as_date('2023-01-01'), to = as_date(today), by = 'day'), ' 00:00:00')),
                        exists = NA)

# what forecasts have already been submitted?
challenge_s3_region <- "data"
challenge_s3_endpoint <- "ecoforecast.org"

# is that file present in the bucket?
for (i in 1:nrow(this_year)) {
  forecast_file <- paste0('aquatics-', as_date(this_year$date[i]), '-', challenge_model_name, '.csv.gz')
  
  this_year$exists[i] <- suppressMessages(aws.s3::object_exists(object = file.path("raw", 'aquatics', forecast_file),
                                                                bucket = "neon4cast-forecasts",
                                                                region = challenge_s3_region,
                                                                base_url = challenge_s3_endpoint))
}

# which dates do you need to generate forecasts for?
missed_dates <- this_year |> 
  filter(exists == F) |> 
  pull(date) |> 
  as_date()


for (i in 1:length(missed_dates)) {
  
  forecast_date <- missed_dates[i]
  
  # Download the NOAA data needed
  noaa_data <- download_noaa(sites = sites,
                             forecast_date = forecast_date)
  
  if (!is.na(noaa_data)){
    # Generate the forecasts
    fTSLM_file <- generate_fTSLM_lag(team_name = 'fTSLM_lag',
                                     sites = sites,
                                     var = 'temperature',
                                     theme = 'aquatics',
                                     forecast_date = forecast_date,
                                     n = 30, 
                                     h = 30, 
                                     target_url = target_url, 
                                     noaa_future = noaa_data$future, 
                                     noaa_past = noaa_data$past)
    # Submit forecast!
    neon4cast::submit(forecast_file = file.path('Forecasts', fTSLM_file),
                      ask = F, s3_region = 'data', s3_endpoint = 'ecoforecast.org')
  } else {
    message('Cannot submit forecast for this date')
  }
  
  
  
}
#----------------------------------------------------------------#