
# TSLM model with air temperature
generate_fTSLM_lag <- function(team_name = 'fTSLM_lag', # model_id and challenge team name
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

  targets <- readr::read_csv(target_url, guess_max = 1e6, 
                             show_col_types = F, progress = F)  |> 
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
    pivot_wider(names_from = "variable", values_from = "observation") 
  
  targets <- left_join(targets, noaa_past, by = c("datetime","site_id"))
  
  
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
      # slice(rep(1:n(), 31)) %>%
      mutate()
    past_weather <- bind_rows(past_weather, subset_past_weather)
  }
  
  # noaa_vars <- noaa_future |> 
  #   ungroup() |> 
  #   select(-c(datetime, site_id, parameter)) |> 
  #   colnames()
  
  # create a past "parameter" - just repeats each value 31 times
  past_weather <- past_weather %>%
    group_by(site_id) %>%
    slice(rep(1:n(), 31)) %>%
    group_by(site_id, air_temperature) %>%
    mutate(parameter = row_number()) 
  
  # Combine the past weather with weather forecast
  message('creating weather ensembles')
  noaa_weather <- bind_rows(past_weather, noaa_future) %>%
    arrange(site_id)
  
  message('starting TSLM model fitting and forecast generations')
  TSLM_fable <- NULL
  
  for (j in 1:length(sites)) {
    # Loop through each site
    site_use <- sites[j]
    # Split the NOAA forecast into each ensemble
    noaa_ensembles <- noaa_weather %>%
      filter(site_id == site_use,
             parameter != 31) %>%
      split(., ~ parameter)
    
    # For each ensemble make this into a tsibble that can be used to forecast
    # then when this is supplied as the new_data argument it will run the forecast for each 
    # air-temperature forecast ensemble
    test_scenarios <- lapply(noaa_ensembles, as_tsibble, key = 'site_id', index = 'datetime')
    
    # Fits separate LM with ARIMA errors for each site
    # message('fitting TSLM model')
    targets_use <- targets %>%
      filter(site_id == site_use) %>%
      as_tsibble(key = 'site_id', index = 'datetime') %>%
      # add NA values up to today (index)
      fill_gaps(.end = forecast_date) 
    
    if (nrow(targets_use) >= 1) {
      TSLM_model <- targets_use %>%
        model(TSLM(temperature ~ air_temperature + lag(air_temperature)))
      
      # Forecast using the fitted model
      # message('producing ensemble forecast using TSLM model')
      times  <- round(n/31) # for each NOAA ensemble member how many times should the model run
      
      TSLM_fable_site <-  TSLM_model %>%
        generate(new_data = test_scenarios, bootstrap = T, times = times) %>%
        mutate(variable = var,
               parameter = as.numeric(.rep) + (100 * (as.numeric(.scenario) - 1))) %>%
        filter(datetime > forecast_date)
      
      TSLM_fable <- bind_rows(TSLM_fable, TSLM_fable_site)
      message('TSLM forecast for ', site_use, ' complete')
    } else {
      message('no forecast generated for ', site_use, ' no observations present')
    }
    
    
  }
  
  
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
                             'parameter', 'variable', 'prediction', 'model_id')))
  }
  
  # Convert to the EFI standard from a fable with distribution
  message('converting to EFI standard')
  TSLM_EFI <- convert.to.efi_standard(TSLM_fable)  
  
  forecast_file <- paste0(theme,'-',TSLM_EFI$reference_datetime[1], '-', team_name, '.csv.gz')
  
  write_csv(TSLM_EFI, file.path('./Forecasts', forecast_file))
  
  message(team_name, ' generated for ', forecast_date)
  
  valid <- neon4cast::forecast_output_validator(file.path('./Forecasts', forecast_file))
  
  if (!valid) {
    file.remove(file.path('./Forecasts', forecast_file))
    message('forecast not valid')
  } else {
    return(forecast_file)
  }
}
