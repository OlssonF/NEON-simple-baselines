
generate_climatology <- function(forecast_date, 
                                 forecast_name = 'climatology',
                                 target_url,
                                 sites) {
  
 
  targets <- readr::read_csv(target_url, guess_max = 10000)
  
  # calculates a doy average for each target variable in each site
  target_clim <- targets %>%  
    mutate(doy = yday(datetime),
           year = year(datetime)) %>% 
    filter(year < year(forecast_date),
           site_id %in% sites) |> 
    group_by(doy, site_id, variable) %>% 
    summarise(mean = mean(observation, na.rm = TRUE),
              sd = sd(observation, na.rm = TRUE),
              .groups = "drop") %>% 
    mutate(mean = ifelse(is.nan(mean), NA, mean)) 
  
  # subset to the DOY needed
  curr_month <- month(forecast_date)
  if(curr_month < 10){
    curr_month <- paste0("0", curr_month)
  }
  
  curr_year <- year(forecast_date)
  start_date <- forecast_date + days(1)
  
  forecast_dates <- seq(start_date, as_date(start_date + days(34)), "1 day")
  forecast_doy <- yday(forecast_dates)
  
  forecast_dates_df <- tibble(datetime = forecast_dates,
                              doy = forecast_doy)
  # generate forecast
  forecast <- target_clim %>%
    mutate(doy = as.integer(doy)) %>% 
    filter(doy %in% forecast_doy) %>% 
    full_join(forecast_dates_df, by = 'doy') %>%
    arrange(site_id, datetime)
  
  
  subseted_site_names <- unique(forecast$site_id)
  site_vector <- NULL
  for(i in 1:length(subseted_site_names)){
    site_vector <- c(site_vector, rep(subseted_site_names[i], length(forecast_dates)))
  }
  
  forecast_tibble1 <- tibble(datetime = rep(forecast_dates, length(subseted_site_names)),
                             site_id = site_vector,
                             variable = "temperature")
  
  forecast_tibble2 <- tibble(datetime = rep(forecast_dates, length(subseted_site_names)),
                             site_id = site_vector,
                             variable = "oxygen")
  
  forecast_tibble3 <- tibble(datetime = rep(forecast_dates, length(subseted_site_names)),
                             site_id = site_vector,
                             variable = "chla")
  
  forecast_tibble <- bind_rows(forecast_tibble1, forecast_tibble2, forecast_tibble3)
  
  foreast <- right_join(forecast, forecast_tibble)
  
  combined <- forecast %>% 
    select(datetime, site_id, variable, mean, sd) %>% 
    group_by(site_id, variable) %>% 
    # remove rows where all in group are NA
    filter(all(!is.na(mean))) %>%
    # retain rows where group size >= 2, to allow interpolation
    filter(n() >= 2) %>%
    mutate(mu = imputeTS::na_interpolation(mean),
           sigma = median(sd, na.rm = TRUE)) %>%
    pivot_longer(c("mu", "sigma"),names_to = "parameter", values_to = "prediction") |> 
    mutate(family = "normal") |> 
    ungroup() |> 
    mutate(reference_datetime = lubridate::as_date(min(datetime)) - lubridate::days(1),
           model_id = forecast_name) |> 
    select(model_id, datetime, reference_datetime, site_id, family, parameter, variable, prediction)
  
  # write forecast
  file_date <- combined$reference_datetime[1]
  
  file_name <- paste0('aquatics-', forecast_date, '-', forecast_name, '.csv.gz')
  
  readr::write_csv(combined, file.path('Forecasts', file_name))  
  
  message(forecast_name, ' generated for ', forecast_date)
  
  
  valid <- neon4cast::forecast_output_validator(file.path('./Forecasts', file_name))

  
  if (!valid) {
    file.remove(file.path('./Forecasts/', file_name))
    message('forecast not valid')
  } else {
    return(file_name)
  }
  
}



generate_persistenceRW <- function(forecast_date, 
                                 forecast_name = 'persistenceRW',
                                 target_url,
                                 sites) {
  source('R/fablePersistenceRW.R')
  # 1.Read in the targets data
  targets <- readr::read_csv(target_url, guess_max = 10000) |> 
    mutate(observation = ifelse(observation == 0 & variable == "chla", 0.00001, observation))
  
  
  # 2. Make the targets into a tsibble with explicit gaps
  targets_ts <- targets %>%
    filter(datetime < forecast_date) |> 
    as_tsibble(key = c('variable', 'site_id'), index = 'datetime') %>%
    # add NA values up to today (index)
    tsibble::fill_gaps(.end = forecast_date)
  
  
  # 3. Run through each via map
  # Requires a dataframe that has each of the variable in the RW_forecast function
  site_var_combinations <- expand.grid(site = unique(targets$site_id),
                                       var = unique(targets$variable)) %>%
    # assign the transformation depending on the variable. chla and oxygen get a log(x) transformation
    mutate(transformation = ifelse(var %in% c('chla', 'oxygen'), 'log', 'none')) %>%
    mutate(boot_number = 200,
           ref_date = forecast_date,
           h = 35,
           bootstrap = T, 
           verbose = T)
  
  # runs the RW forecast for each combination of variable and site_id
  RW_forecasts <- purrr::pmap_dfr(site_var_combinations, RW_daily_forecast) 
  # convert the output into EFI standard
  RW_forecasts_EFI <- RW_forecasts %>%
    rename(parameter = .rep,
           prediction = .sim) %>%
    # For the EFI challenge we only want the forecast for future
    filter(datetime > forecast_date) %>%
    group_by(site_id, variable) %>%
    mutate(reference_datetime = min(datetime) - lubridate::days(1),
           family = "ensemble",
           model_id = forecast_name) %>%
    select(model_id, datetime, reference_datetime, site_id, family, parameter, variable, prediction) 
  
  # write forecast
  file_date <- RW_forecasts_EFI$reference_datetime[1]
  
  file_name <- paste0('aquatics-', file_date, '-', forecast_name, '.csv.gz')
  
  readr::write_csv(RW_forecasts_EFI, file.path('Forecasts', file_name))  
  
  message(forecast_name, ' generated for ', forecast_date)
  
  
  valid <- neon4cast::forecast_output_validator(file.path('./Forecasts', file_name))
  
  
  if (!valid) {
    file.remove(file.path('./Forecasts/', file_name))
    message('forecast not valid')
  } else {
    return(file_name)
  }
  
}