library(tsibble)
library(tidyverse)
library(neon4cast)
library(lubridate)
library(arrow)
library(fable)

source('R/download_noaa.R')
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
forecast_today <- Sys.Date() 

# generate ensembles
if (dir.exists('./Forecasts/') != T) {
  dir.create('./Forecasts/', recursive = T)
}

message('Download NOAA')
# Download the NOAA data needed
noaa_data_today <- download_noaa(sites = sites,
                                 forecast_date = forecast_today)


# fARIMA forecasts --------------------------------------------------------
# Generate the forecasts
message('Running fARIMA forecast')
fARIMA_file <- generate_fARIMA(team_name = 'fARIMA',
                               sites = sites,
                               var = 'temperature',
                               theme = 'aquatics',
                               forecast_date = forecast_today,
                               n = 200, 
                               h = 30, 
                               target_url = target_url, 
                               noaa_future = noaa_data_today$future, 
                               noaa_past = noaa_data_today$past)
# Submit forecast!
message('submit fARIMA')
neon4cast::submit(forecast_file = file.path('Forecasts', fARIMA_file),
                  ask = F)

# Check for missing forecasts
message('Checking for missing ARIMA forecasts')

challenge_model_name <- 'fARIMA'


# Dates of forecasts
today <- paste(Sys.Date() - days(2), '00:00:00')
this_year <- data.frame(date = as.character(paste0(seq.Date(as_date('2024-01-01'), 
                                                            to = as_date(today),
                                                            by = 'day'), 
                                                   ' 00:00:00')),
                        exists = NA)
# issue resolved on 9 Oct 2023
# end <- as_date('2023-10-09')

# Get all the submissions 
submissions <- aws.s3::get_bucket_df("bio230014-bucket01", 
                                     prefix = "challenges/forecasts/raw",
                                     region = "sdsc",
                                     base_url = "osn.xsede.org",
                                     max = Inf)

Sys.unsetenv("AWS_ACCESS_KEY_ID")
Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")

# is that file present in the bucket?
for (i in 1:nrow(this_year)) {
  forecast_file <- paste0('aquatics-', as_date(this_year$date[i]), '-', challenge_model_name, '.csv.gz')

  this_year$exists[i] <- nrow(dplyr::filter(submissions, stringr::str_detect(Key, forecast_file))) > 0
}

# which dates do you need to generate forecasts for?
# those that are missing or haven't been submitted
missed_dates <- this_year |>
  filter(exists == F) |>
  pull(date) |>
  as_date()


if (length(missed_dates) != 0) {
  for (i in 1:length(missed_dates)) {
    
    forecast_date <- missed_dates[i]
    
    # Download the NOAA data needed
    noaa_data <- download_noaa(sites = sites,
                               forecast_date = forecast_date)
    
    if (is.list(noaa_data)) {
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
      message('submitting missing fARIMA forecast')
      neon4cast::submit(forecast_file = file.path('Forecasts', fARIMA_file),
                        ask = F)
    } else {
      message('Cannot submit forecast for ', forecast_date)
    }
    
  }
  
} else {
  'No missing forecasts!'
}

#----------------------------------------------------------#

# fTSLM_lag forecasts -----------------------------------------------------
Sys.unsetenv("AWS_ACCESS_KEY_ID")
Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
# Generate the TSLM forecasts
message('generating todays TSML forecast')
fTSLM_file <- generate_fTSLM_lag(team_name = 'fTSLM_lag',
                                 sites = sites,
                                 var = 'temperature',
                                 theme = 'aquatics',
                                 forecast_date = forecast_today,
                                 n = 200, 
                                 h = 30, 
                                 target_url = target_url, 
                                 noaa_future = noaa_data_today$future, 
                                 noaa_past = noaa_data_today$past)
# Submit forecast!
message('submit TSLM forecast')
neon4cast::submit(forecast_file = file.path('Forecasts', fTSLM_file),
                  ask = F)

# Check for missing forecasts
message('Checking for missing TSLM forecasts')

challenge_model_name <- 'fTSLM_lag'


# Dates of forecasts
today <- paste(Sys.Date() - days(2), '00:00:00')
this_year <- data.frame(date = as.character(paste0(seq.Date(as_date('2024-01-01'), to = as_date(today), by = 'day'), ' 00:00:00')),
                        exists = NA)


Sys.unsetenv("AWS_ACCESS_KEY_ID")
Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")

# is that file present in the bucket?
for (i in 1:nrow(this_year)) {
  forecast_file <- paste0('aquatics-', as_date(this_year$date[i]), '-', challenge_model_name, '.csv.gz')

  this_year$exists[i] <- nrow(dplyr::filter(submissions, stringr::str_detect(Key, forecast_file))) > 0

}

# which dates do you need to generate forecasts for?
# those that are missing or haven't been submitted
missed_dates <- this_year |>
  filter(exists == F) |>
  pull(date) |>
  as_date()

if (length(missed_dates) !=0) {
  for (i in 1:length(missed_dates)) {
    
    forecast_date <- missed_dates[i]
    
    # Download the NOAA data needed
    noaa_data <- download_noaa(sites = sites,
                               forecast_date = forecast_date)
    
    if (is.list(noaa_data)){
      # Generate the forecasts
      fTSLM_file <- generate_fTSLM_lag(team_name = 'fTSLM_lag',
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
      message('submitting missing TSLM forecast')
      neon4cast::submit(forecast_file = file.path('Forecasts', fTSLM_file),
                        ask = F)
    } else {
      message('Cannot submit forecast for ', forecast_date)
    }
    
    
    
  }
} else {
  'No missing forecasts!'
}

#----------------------------------------------------------------#