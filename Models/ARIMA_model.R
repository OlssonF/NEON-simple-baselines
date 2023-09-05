# ARIMA model with air temperature

generate_fARIMA <- function(team_name = 'fARIMA', # model_id and challenge team name
                            sites, # vector of sites
                            var, # variable to make a forecast of (single var only)
                            theme, # Challenge theme
                            forecast_date, # reference_datetime of forecast
                            n = 200, # number of ensemble members
                            h = 30, # forecast horizon
                            target_url, # URL of targets
                            noaa_future, #dateframe containing the relevant noaa data
                            noaa_past) { #dateframe containing the relevant noaa data
 
  
  # First check that the NOAA data looks right
  if (min(noaa_future$datetime) != forecast_date) {
    return(NA)
    stop('Error: NOAA forecast is not correct!')
  }
  
  # Target data
  targets <- readr::read_csv(target_url, guess_max = 1e6, show_col_types = F) |> 
    filter(site_id %in% sites,
           datetime <= forecast_date)
  
  # Do we need a value from yesterday to start?
  forecast_starts <- targets %>%
    na.omit() %>%
    group_by(variable, site_id) %>%
    # Start the day after the most recent non-NA value
    dplyr::summarise(start_date = max(datetime) + lubridate::days(1)) %>% # Date
    dplyr::mutate(h = (forecast_date - start_date) + h) %>% # Horizon value
    dplyr::filter(variable == 'temperature') %>%
    dplyr::ungroup()
  
  
  targets <- targets |> 
    select(datetime, site_id, variable, observation) |> 
    filter(variable == var) |> 
    pivot_wider(names_from = "variable", values_from = "observation") %>%
    filter(!is.na(temperature))
  
  targets <- left_join(targets, noaa_past, by = c("datetime","site_id"))
  
  
  
  # NOAA weather - combine the past and future 
  # sometimes we need to start the forecast in the past
  past_weather <- NULL
  
  # Extract the past weather data for the met observations where we don't have 
  # temperature observations
  for (i in 1:nrow(forecast_starts)) {
    subset_past_weather <- noaa_past %>%
      # only take the past weather that is after the last water temperature observation and 
      # less than what is in the weather forecast
      filter(site_id == forecast_starts$site_id[i]  &
               datetime >= forecast_starts$start_date[i] &
               datetime < min(noaa_future$datetime)) %>% 
      # create a past "ensemble" - just repeats each value 31 times
      slice(rep(1:n(), 31))
    past_weather <- bind_rows(past_weather, subset_past_weather)
  }
  
  # noaa_vars <- noaa_future |> 
  #   ungroup() |> 
  #   select(-c(datetime, site_id, parameter)) |> 
  #   colnames()
  
  past_weather <- past_weather %>%
    group_by(site_id, air_temperature) %>%
    mutate(parameter = row_number())
  
  
  # Combine the past weather with weather forecast
  message('creating weather ensembles')
  noaa_weather <- bind_rows(past_weather, noaa_future) %>%
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
  times  <- round(n/31) # for each NOAA ensemble member how many times should the model run
  
  ARIMA_fable <- ARIMA_model %>%
    generate(new_data = test_scenarios, bootstrap = T, times = times) %>%
    mutate(variable = var,
           # Recode the ensemble number based on the scenario and replicate
           parameter = as.numeric(.rep) + (10 * (as.numeric(.scenario) - 1)))  %>%
    filter(datetime > forecast_date)
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
  
  
  forecast_file <- paste0(theme, '-', ARIMA_EFI$reference_datetime[1], '-', team_name, '.csv.gz')
  
  write_csv(ARIMA_EFI, file.path('./Forecasts/', forecast_file))
  
  message(team_name, ' generated for ', forecast_date)
  
  valid <- neon4cast::forecast_output_validator(file.path('./Forecasts/', forecast_file))
  
  if (!valid) {
    file.remove(file.path('./Forecasts/', forecast_file))
    message('forecast not valid')
  } else {
    return(forecast_file)
  }
   
}

