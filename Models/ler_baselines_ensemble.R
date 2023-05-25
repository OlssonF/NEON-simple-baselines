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

# Ensemble 4 = FLARE-LER + baselines
mme_file <- create_mme(forecast_models = c('flareGLM',
                               'flareGOTM',
                               'flareSimstrat',
                               'climatology', 
                               'persistenceRW'),
           ensemble_name = 'flare_ler_baselines',
           forecast_date = forecast_date,
           s3 = s3, n = 200)

neon4cast::submit(file.path('./Forecasts/ensembles', mme_file), ask = F)
