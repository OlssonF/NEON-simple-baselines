# function to generate a balance MME from a list of forecast model names

create_mme <- function(forecast_models, # vector of list of model names
                       n = 200, # what size ensemble do you want?
                       ensemble_name, # what is the name of the ensemble output
                       forecast_date, # when is the forecast for (what forecast to grab)
                       s3 = s3) # the s3 region to look for the forecasts
  {
  
  message('generating ensemble for ', 
          paste(forecast_models, sep="' '", collapse=", "))
  
  # How many from each forecast should be sampled
  n_models <- length(forecast_models)
  sample <- round(n / n_models, digits = 0)
  
  mme_forecast <- NULL
  
  for (i in 1:length(forecast_models)) {
    # connect to the forecast bucket
    s3_model <- s3_bucket(paste0(s3, 'model_id=', forecast_models[i], '/reference_datetime=',forecast_date),
                          endpoint_override= "data.ecoforecast.org")

    forecast <- arrow::open_dataset(s3_model)  |>
      collect() |> 
      filter(variable == 'temperature') |>
      group_by(site_id) |> 
      # remove sites that contain NAs
      filter(!any(is.na(prediction))) |> 
      ungroup() |> 
      mutate(model_id = forecast_models[i],
             horizon = as_date(datetime) - as_date(forecast_date)) |> 
      filter(horizon <= 30) |> 
      select(-horizon)
    
    message(forecast_models[i], ' read in')
    
    # different workflow if the forecast is an ensemble (sample) or normal family
    if (forecast$family[1] != 'sample') {
      forecast_normal <- forecast |> 
        
        pivot_wider(names_from = parameter,
                    values_from = prediction, 
                    id_cols = c(datetime, site_id, model_id)) |> 
        
        group_by(site_id, datetime, model_id) |> 
        # sample from the distribution based on the mean and sd
        summarise(prediction = rnorm(sample, mean = mu, sd = sigma)) |> 
        group_by(site_id, datetime) |> 
        # parameter value needs to be character
        mutate(parameter = as.character(row_number()),
               # model_id = ensemble_name, 
               reference_datetime = forecast_date,
               variable = 'temperature',
               family = 'ensemble')
      mme_forecast <- bind_rows(mme_forecast, forecast_normal) 
    } else { # for an ensemble forecast
      forecast_sample <- forecast %>%
        distinct(parameter) %>%
        slice_sample(n = sample) %>%
        left_join(., forecast, by = "parameter", multiple = 'all') %>%
        mutate(#model_id = ensemble_name, 
               reference_datetime = forecast_date,
               parameter = as.character(parameter)) 
      mme_forecast <- bind_rows(mme_forecast, forecast_sample) 
    }
    
  }
  
  # Check for all sites represented in all models
  site_model_combinations <- nrow(mme_forecast |> distinct(model_id, site_id))
  all_combinations <- nrow(mme_forecast |> distinct(model_id)) * nrow(mme_forecast |> distinct(site_id))
  
  if (site_model_combinations != all_combinations) {
    
    
    mme_forecast <- mme_forecast |> 
      complete(site_id, model_id) |> 
      group_by(site_id) |> 
      filter(!any(is.na(prediction))) 
    message('not all sites are represented by all models, subsetting sites')
    message(paste(unique(mme_forecast$site_id), sep="' '", collapse=", "))
  }
  
  # need to recode the parameter values so each is unqiue
  mme_forecast <- mme_forecast |> 
    group_by(datetime, site_id) |> 
    mutate(parameter = row_number(),
           family = 'ensemble') |> 
    ungroup() |> 
    mutate(model_id = ensemble_name)
  
  #Check for all models
  if (length(unique(mme_forecast$parameter)) != (n_models* round(n / n_models, digits = 0))) {
    stop('you are missing some ensemble members, there may be forecasts missing!')
  }
  
  filename <- paste0('aquatics-', forecast_date, '-', ensemble_name, '.csv.gz')
  mme_forecast |>
    select(-any_of(c('pubDate', 'date'))) |> 
    readr::write_csv(file.path('./Forecasts/ensembles', filename))
  
  message(ensemble_name, ' generated')
  
  neon4cast::forecast_output_validator(file.path('./Forecasts/ensembles', filename))
  
  
  valid <- neon4cast::forecast_output_validator(file.path('./Forecasts/ensembles', filename))
  
  if (!valid) {
    file.remove(file.path('./Forecasts/ensembles', filename))
    message('forecast not valid')
  } else {
    return(filename)
  }
}
