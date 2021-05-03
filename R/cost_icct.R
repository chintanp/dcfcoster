# This file does dcfc costing as per the ICCT publication:
# https://theicct.org/sites/default/files/publications/ICCT_EV_Charging_Cost_20190813.pdf

source("./R/setup_logging.R")

## Setup the logging destination
lg <-
  lgr::get_logger("test")$set_propagate(FALSE)$set_appenders(lgr::AppenderJson$new(layout = LayoutLogstash$new(), file = here::here(
    paste0(
      "logs/run_",
      as.character(Sys.time(), format = "%y-%m-%d-%H-%M-%S"),
      ".log"
    )
  )))


dcfc_cost <- function(type = "new",
                      new_plug_count = 1,
                      evse_id = '') {
  total_plug_count <- 0
  # Database settings -------------------------------------------------------

  if (!DBI::dbCanConnect(
    RPostgres::Postgres(),
    host = Sys.getenv("MAIN_HOST"),
    dbname = Sys.getenv("MAIN_DB"),
    user = Sys.getenv("MAIN_USER"),
    password = Sys.getenv("MAIN_PWD"),
    port = Sys.getenv("MAIN_PORT")
  )) {
    lg$log(level = "fatal",
           msg = "Cannot connect to database",
           "ip" = ipify::get_ip())
    # Exit if DB cannot connect
    stop("Cannot connect to database")
  }

  main_con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host = Sys.getenv("MAIN_HOST"),
    dbname = Sys.getenv("MAIN_DB"),
    user = Sys.getenv("MAIN_USER"),
    password = Sys.getenv("MAIN_PWD"),
    port = Sys.getenv("MAIN_PORT")
  )
  if (new_plug_count >= 1) {
    if (type == "new") {
      lg$log(level = "info",
             msg = "New station build cost",
             "ip" = ipify::get_ip())
      total_plug_count <- new_plug_count
    } else if (type == "upgrade") {
      lg$log(level = "info",
             msg = "Upgrade station build cost",
             "ip" = ipify::get_ip())
      # gas prices in WA
      cs_details <-
        DBI::dbGetQuery(main_con,
                        glue::glue('select * from built_evse where bevse_id = {evse_id}'))
      total_plug_count <- new_plug_count + cs_details$dcfc_count
    } else {
      lg$log(level = "fatal",
             msg = "Wrong input for station type",
             "ip" = ipify::get_ip())
      stop("Wrong input for station type")
    }

  }



  if (total_plug_count == 1) {
    total_cost <- 45506
  } else if (total_plug_count == 2) {
    total_cost  <- 36235 * new_plug_count
  } else if ((3 <= total_plug_count) & (total_plug_count <= 5)) {
    total_cost <- 26964 * new_plug_count
  } else if ((6 <= total_plug_count) & (total_plug_count <= 50)) {
    total_cost <- 22470 * new_plug_count
  } else if (total_plug_count > 50) {
    lg$log(level = "fatal",
           msg = "Total plug count higher than 50 is not supported currently.",
           "ip" = ipify::get_ip())
    stop("Total plug count higher than 50 is not supported currently.")

  } else {
    total_cost <- 0
  }

  return (total_cost)
}
