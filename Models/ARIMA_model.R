# ARIMA model with air temperature
# remotes::install_github("tidyverts/fable")
library(fable)
library(tsibble)
library(tidyverse)
library(neon4cast)
library(lubridate)
#library(rMR)
library(arrow)

options(dplyr.summarise.inform = FALSE)

# submission information
team_name <- "fARIMA"

# Target data
targets <- readr::read_csv("https://data.ecoforecast.org/neon4cast-targets/aquatics/aquatics-targets.csv.gz", guess_max = 1e6)

sites <- unique(targets$site_id)

site_data <- readr::read_csv("https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv") |> 
  dplyr::filter(aquatics == 1)

# Do we need a value from yesterday to start?
forecast_starts <- targets %>%
  na.omit() %>%
  group_by(variable, site_id) %>%
  # Start the day after the most recent non-NA value
  dplyr::summarise(start_date = max(datetime) + lubridate::days(1)) %>% # Date
  dplyr::mutate(h = (Sys.Date() - start_date) + 30) %>% # Horizon value
  dplyr::filter(variable == 'temperature') %>%
  dplyr::ungroup()


# Merge in past NOAA data into the targets file, matching by date.
# Before building our linear model we need merge in the historical air 
# temperature to match with the historical water temperature

targets <- targets |> 
  select(datetime, site_id, variable, observation) |> 
  filter(variable == 'temperature') |> 
  pivot_wider(names_from = "variable", values_from = "observation") %>%
  filter(!is.na(temperature))

targets <- left_join(targets, noaa_past_mean, by = c("datetime","site_id"))



# NOAA weather - combine the past and future 
# sometimes we need to start the forecast in the past
past_weather <- NULL

# Extract the past weather data for the met observations where we don't have 
  # temperature observations
for (i in 1:nrow(forecast_starts)) {
  subset_past_weather <- noaa_past_mean %>%
    # only take the past weather that is after the last water temperature observation and 
      # less than what is in the weather forecast
    filter(site_id == forecast_starts$site_id[i]  &
             datetime >= forecast_starts$start_date[i] &
             datetime < min(noaa_future_mean$datetime)) %>% 
    # create a past "ensemble" - just repeats each value 31 times
    slice(rep(1:n(), 31))
  past_weather <- bind_rows(past_weather, subset_past_weather)
}


past_weather <- past_weather %>%
  group_by(site_id, air_temperature) %>%
  mutate(parameter = row_number())


# Combine the past weather with weather forecast
message('creating weather ensembles')
noaa_weather <- bind_rows(past_weather, noaa_future_mean) %>%
  arrange(site_id, parameter)

# Split the NOAA forecast into each ensemble (parameter)
noaa_ensembles <- split(noaa_weather, f = noaa_weather$parameter)
# For each ensemble make this into a tsibble that can be used to forecast
  # then when this is supplied as the new_data argument it will run the forecast for each 
  # air-temperature forecast ensemble
test_scenarios <- lapply(noaa_ensembles, as_tsibble, key = 'site_id', index = 'datetime')


message('starting ARIMA model fitting and forecast generations')
# Fits separate LM with ARIMA errors for each site
ARIMA_model <- targets %>%
  as_tsibble(key = 'site_id', index = 'datetime') %>%
  # add NA values for explicit gaps
  tsibble::fill_gaps() %>%
  model(ARIMA(temperature ~ air_temperature)) 
message('ARIMA fitted')

# Forecast using the fitted model
ARIMA_fable <- ARIMA_model %>%
  generate(new_data = test_scenarios, bootstrap = T, times = 100) %>%
  mutate(variable = 'temperature',
         # Recode the ensemble number based on the scenario and replicate
         parameter = as.numeric(.rep) + (100 * (as.numeric(.scenario) - 1)))  %>%
  filter(datetime > Sys.Date())
message('forecast generated')

# Function to convert to EFI standard
convert.to.efi_standard <- function(df){
  
  df %>% 
    as_tibble() %>%
    dplyr::rename(prediction = .sim) %>%
    dplyr::select(datetime, site_id, prediction, variable, parameter) %>%
    dplyr::mutate(family = "ensemble",
                  model_id = team_name,
                  reference_datetime = min(datetime) - lubridate::days(1)) %>%
    dplyr::select(any_of(c('datetime', 'reference_datetime', 'site_id', 'family', 
                           'parameter', 'variable', 'prediction', 'ensemble', 'model_id')))
}
message('converting to EFI standard')
ARIMA_EFI <- convert.to.efi_standard(ARIMA_fable)
  

forecast_file <- paste0('aquatics-', ARIMA_EFI$reference_datetime[1], '-', team_name, '.csv.gz')

write_csv(ARIMA_EFI, forecast_file)
# Submit forecast!

# Now we can submit the forecast output to the Challenge using 
neon4cast::forecast_output_validator(forecast_file)
neon4cast::submit(forecast_file = forecast_file,
                  ask = F, s3_region = 'data', s3_endpoint = 'ecoforecast.org')
