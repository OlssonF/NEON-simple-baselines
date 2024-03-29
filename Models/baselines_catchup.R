library(tidyverse)
library(tsibble)
library(aws.s3)

source("R/generate_baselines.R")

Sys.unsetenv("AWS_ACCESS_KEY_ID")
Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")

target_url <- "https://data.ecoforecast.org/neon4cast-targets/aquatics/aquatics-targets.csv.gz"

sites <- read_csv("https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv") |> 
  dplyr::filter(aquatics == 1) |> 
  dplyr::select(field_site_id) |> 
  dplyr::pull()

# generate ensembles
if (dir.exists('./Forecasts') != T) {
  dir.create('./Forecasts', recursive = T)
}

# ====== climatology ======

# check for any missing forecasts
message("==== Checking for missed climatology forecasts ====")
challenge_model_name <- 'climatology'


# Dates of forecasts 
today <- paste(Sys.Date() - days(2), '00:00:00')
this_year <- data.frame(date = as.character(paste0(seq.Date(as_date('2024-01-01'), to = as_date(today), by = 'day'), ' 00:00:00')),
                        exists = NA)

# Get all the submissions 
submissions <- aws.s3::get_bucket_df("bio230014-bucket01", 
                                     prefix = "challenges/forecasts/raw",
                                     region = "sdsc",
                                     base_url = "osn.xsede.org",
                                     max = Inf)

# is that file present in the bucket?
for (i in 1:nrow(this_year)) {
  forecast_file <- paste0('aquatics-', as_date(this_year$date[i]), '-', challenge_model_name, '.csv.gz')
  
  this_year$exists[i] <- nrow(dplyr::filter(submissions, stringr::str_detect(Key, forecast_file))) > 0
  
}

# which dates do you need to generate forecasts for?
missed_dates <- this_year |> 
  filter(exists == F) |> 
  pull(date) |> 
  as_date()

if (length(missed_dates) != 0) {
  for (i in 1:length(missed_dates)) {
    date <- missed_dates[i]
    
    forecast_file <- generate_climatology(forecast_date = date,
                                          forecast_name = 'climatology', 
                                          target_url = target_url,
                                          sites = sites)
    
    
    if (!is.na(forecast_file)) {
      neon4cast::submit(file.path('./Forecasts/', forecast_file), ask = F)
      message('submitting new climatology forecast')
    }
    
  }
} else {
  message('no new climatology forecasts')  
}
#=========== persistence ===============
message("==== Checking for missed persistence forecasts ====")
challenge_model_name <- 'persistenceRW'


# Dates of forecasts 
today <- paste(Sys.Date() - days(2), '00:00:00')
this_year <- data.frame(date = as.character(paste0(seq.Date(as_date('2023-01-01'), to = as_date(today), by = 'day'), ' 00:00:00')),
                        exists = NA)



# is that file present in the bucket?
for (i in 1:nrow(this_year)) {
  forecast_file <- paste0('aquatics-', as_date(this_year$date[i]), '-', challenge_model_name, '.csv.gz')
  
  this_year$exists[i] <- nrow(dplyr::filter(submissions, stringr::str_detect(Key, forecast_file))) > 0
  
}

# which dates do you need to generate forecasts for?
missed_dates <- this_year |> 
  filter(exists == F) |> 
  pull(date) |> 
  as_date()

if (length(missed_dates) != 0) {
  for (i in 1:length(missed_dates)) {
    date <- missed_dates[i]
    
    forecast_file <- generate_persistenceRW(forecast_date = date,
                                          forecast_name = 'persistenceRW', 
                                          target_url = target_url,
                                          sites = sites)
    
    
    if (!is.na(forecast_file)) {
      neon4cast::submit(file.path('./Forecasts/', forecast_file), ask = F)
      message('submitting new persistence forecast')
      
    }
    
  }
}else {
  message('no new climatology forecasts')  
}

