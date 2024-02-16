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

# when do you want to generate the MMEs for
forecast_date <- as.character(Sys.Date() - days(1))

# generate ensembles
if (dir.exists('./Forecasts/ensembles') != T) {
  dir.create('./Forecasts/ensembles', recursive = T)
}

# Ensemble 3 = FLARE-LER
mme_file <- create_mme(forecast_models = c('flareGLM',
                               'flareGOTM',
                               'flareSimstrat'),
                       ensemble_name = 'flare_ler',
                       forecast_date = forecast_date, 
                       var = 'temperature', 
                       h = 30, 
                       theme = 'aquatics',
                       n = 200)

neon4cast::submit(file.path('./Forecasts/ensembles', mme_file), ask = F)


# check for any missing forecasts
message("==== Checking for missed forecasts ====")
challenge_model_name <- 'flare_ler'


# Dates of forecasts
today <- paste(Sys.Date() - days(2), '00:00:00')
this_year <- data.frame(date = as.character(paste0(seq.Date(as_date('2024-01-01'), to = as_date(today), by = 'day'), ' 00:00:00')),
                        exists = NA)

# what forecasts have already been submitted?
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

for (i in 1:length(missed_dates)) {
  date <- missed_dates[i]

  mme_file <- create_mme(forecast_models = c('flareGLM',
                                             'flareGOTM',
                                             'flareSimstrat'),
                         ensemble_name = 'flare_ler',
                         forecast_date = date,
                         var = 'temperature',
                         h = 30,
                         theme = 'aquatics',
                         n = 200)
  if (!is.na(mme_file)) {
    neon4cast::submit(file.path('./Forecasts/ensembles', mme_file), ask = F)
  }



}
