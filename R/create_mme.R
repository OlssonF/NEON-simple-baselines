# function to generate a balance MME from a list of forecast model names

create_mme <- function(forecast_models, # vector of list of model names
                       n = 200, # what size ensemble do you want?
                       ensemble_name, # what is the name of the ensemble output
                       forecast_date, # when is the forecast for (what forecast to grab)
                       var, # which variable do you want a forecast for
                       h = 30, # what is the required forecast horizon
                       theme) # challenge theme
  {
  
  message('generating ensemble for ', 
          paste(forecast_models, sep="' '", collapse=", "), ' on ', forecast_date)
  
  # How many from each forecast should be sampled
  n_models <- length(forecast_models)
  sample <- round(n / n_models, digits = 0)
  
  mme_forecast <- NULL
  
  for (i in 1:length(forecast_models)) {
    # connect to the forecast bucket
    s3_model <- s3_bucket(paste0('neon4cast-forecasts/parquet/', theme, '/model_id=', 
                                 forecast_models[i], '/reference_datetime=', forecast_date),
                          endpoint_override= "data.ecoforecast.org")

    if (class(try(s3_model$ls(), silent = T)) == 'try-error') {
      message('Error: forecast model ', forecast_models[i], ' not available, skipping MME for ', as.character(forecast_date))
      return(NA)
      stop()
    } else {
      
      forecast <- arrow::open_dataset(s3_model) |>
        collect() |> 
        filter(variable == var) |>
        group_by(site_id) |> 
        # remove sites that contain NAs
        filter(!any(is.na(prediction))) |> 
        ungroup() |> 
        mutate(model_id = forecast_models[i],
               horizon = as_date(datetime) - as_date(forecast_date)) |> 
        filter(horizon <= h) |> 
        select(-horizon)
      
      message(forecast_models[i], ' read in')
      
      # different workflow if the forecast is an ensemble (sample) or normal family
      if (forecast$family[1] != 'sample') {
        forecast_normal <- forecast |> 
          select(datetime, site_id, variable, family, parameter, prediction, model_id) |> 
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
                 variable = var,
                 family = 'ensemble')
        mme_forecast <- bind_rows(mme_forecast, forecast_normal) 
      } else { # for an ensemble forecast
        if (sample <= length(unique(forecast$parameter))) {
          forecast_sample <- forecast %>%
            distinct(parameter) %>%
            slice_sample(n = sample) %>%
            left_join(., forecast, by = "parameter", multiple = 'all') %>%
            mutate(#model_id = ensemble_name, 
              reference_datetime = forecast_date,
              parameter = as.character(parameter)) 
          mme_forecast <- bind_rows(mme_forecast, forecast_sample)
        } else {
          message('Error: forecast model ', forecast_models[i], ' only has ', length(unique(forecast$parameter)), ' ensemble members. Reduce sample n')
          return(NA)
          stop() 
        }
         
      }
      
    }
    
    
    
  }
  
  # Check for all sites represented in all models
  site_model_combinations <- nrow(mme_forecast |> distinct(model_id, site_id))
  all_combinations <- nrow(mme_forecast |> distinct(model_id)) * nrow(mme_forecast |> distinct(site_id))
  all_sites <- mme_forecast |> distinct(site_id) |> pull()
  
  if (site_model_combinations != all_combinations) {
    
    
    mme_forecast <- mme_forecast |> 
      complete(site_id, model_id) |> 
      group_by(site_id) |> 
      filter(!any(is.na(prediction))) 
    message('Warning: not all sites are represented by all models, subsetting sites')
    message(paste0(paste(unique(mme_forecast$site_id), sep="' '", collapse=", ")), ' submitted')
    message(paste0(paste(all_sites[which(!all_sites %in% unique(mme_forecast$site_id))], sep = "' ", collapse = ', '), ' omitted')
)
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
    return(NA)
    stop('Error: you are missing some ensemble members, there may be forecasts missing!')
  }
  
  filename <- paste0(theme, '-', forecast_date, '-', ensemble_name, '.csv.gz')
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
