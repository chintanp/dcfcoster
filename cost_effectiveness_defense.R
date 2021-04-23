# This file does calculations for cost-effectiveness
library(magrittr)
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


## Locations of with max CS
basecase_aid <-  484
sql_be_cs = glue::glue(
  "select count(cs_id) as count, analysis_id, evse_id, be.latitude, be.longitude
from evse_charging_session ecs
         left join built_evse be on
    'b' || cast(be.bevse_id as text) = ecs.evse_id
where ecs.analysis_id = {basecase_aid}
group by ecs.analysis_id, ecs.evse_id, be.latitude, be.longitude
order by count desc
limit 5;
"
)
be_cs  <-  DBI::dbGetQuery(main_con, sql_be_cs)

## Stats for the sets with new and upgrades
set_id <- c(102, 110, 111, 112, 113, 114, 115)
sql_count_stats = glue::glue(
  "select count(ets.veh_id) as sim_count,
       ets.analysis_id,
       coalesce(ne.count, 0) as nevse_count,
       ef.fin_count,
       ef.evmt,
       es.str_count,
       ecs.cs_count,
       ew.wait_count,
       ar.set_id
from evtrip_scenarios ets
         left join (select count(nevse_id), analysis_id from new_evses group by analysis_id) as ne
              on ets.analysis_id = ne.analysis_id
         join (select count(veh_id) as fin_count, sum(distance_travelled) as evmt, analysis_id
               from ev_finished
               group by analysis_id) as ef
              on ets.analysis_id = ef.analysis_id
         join (select count(veh_id) as str_count, analysis_id from ev_stranded group by analysis_id) as es
              on ets.analysis_id = es.analysis_id
         join (select count(cs_id) as cs_count, analysis_id from evse_charging_session group by analysis_id) as ecs
              on ets.analysis_id = ecs.analysis_id
join (select count(wait_id) as wait_count, analysis_id from evse_evs_waiting group by analysis_id) as ew
              on ets.analysis_id = ew.analysis_id
join (select analysis_id, set_id from analysis_record where set_id IN ({glue::glue_collapse(set_id, ', ')})) as ar ON ets.analysis_id = ar.analysis_id
group by ets.analysis_id, nevse_count, ef.fin_count, es.str_count, ef.evmt, ecs.cs_count, ew.wait_count, ar.set_id
order by ets.analysis_id desc
;"
)

count_stats <- DBI::dbGetQuery(main_con, sql_count_stats)

count_stats_set_avg <-
  count_stats %>% dplyr::group_by(set_id) %>% dplyr::summarise(mean_evmt = mean(evmt))

count_stats_set_avg$delta_evmt <-
  count_stats_set_avg$mean_evmt - count_stats_set_avg$mean_evmt[count_stats_set_avg$set_id == 102]

count_stats_set_avg$type <-
  c('basecase', 'upgrade', 'upgrade', 'new', 'new', 'new', 'new')
count_stats_set_avg$dcfc_count <- c(0, 5, 10, 5, 10, 5, 10)
count_stats_set_avg$total_cost <- 0

for (i in rownames(count_stats_set_avg)) {
  type <- count_stats_set_avg[i, "type"]
  dcfc_count <- count_stats_set_avg[i, "dcfc_count"]
  total_cost <- 0
  print(type)
  print(dcfc_count)
  for (evse_id in be_cs$evse_id)
  {
    evse_id <- substr(evse_id, start = 2, stop = nchar(evse_id))
    print(evse_id)
    cost <-
      dcfcoster::dcfc_cost(type = type,
                           new_plug_count = dcfc_count,
                           evse_id = evse_id)
    print(cost)
    total_cost <- total_cost + cost
  }
  print(total_cost)
  count_stats_set_avg[i, "total_cost"] <- total_cost
}


count_stats_set_avg$cost_eff <-
  count_stats_set_avg$delta_evmt / count_stats_set_avg$total_cost

write.csv(count_stats_set_avg, "results_cs.csv")

## Locations of with max WS
basecase_aid <-  484
sql_be_ws = glue::glue(
  "select count(wait_id) as count, analysis_id, evse_id, be.latitude, be.longitude
from evse_evs_waiting ews
         left join built_evse be on
    'b' || cast(be.bevse_id as text) = ews.evse_id
where ews.analysis_id = {basecase_aid}
group by ews.analysis_id, ews.evse_id, be.latitude, be.longitude
order by count desc
limit 5;
"
)
be_ws  <-  DBI::dbGetQuery(main_con, sql_be_ws)

## Stats for the sets with new and upgrades
set_id <- c(102, 104, 105, 106, 107, 108, 109)
sql_count_stats_ws = glue::glue(
  "select count(ets.veh_id) as sim_count,
       ets.analysis_id,
       coalesce(ne.count, 0) as nevse_count,
       ef.fin_count,
       ef.evmt,
       es.str_count,
       ecs.cs_count,
       ew.wait_count,
       ar.set_id
from evtrip_scenarios ets
         left join (select count(nevse_id), analysis_id from new_evses group by analysis_id) as ne
              on ets.analysis_id = ne.analysis_id
         join (select count(veh_id) as fin_count, sum(distance_travelled) as evmt, analysis_id
               from ev_finished
               group by analysis_id) as ef
              on ets.analysis_id = ef.analysis_id
         join (select count(veh_id) as str_count, analysis_id from ev_stranded group by analysis_id) as es
              on ets.analysis_id = es.analysis_id
         join (select count(cs_id) as cs_count, analysis_id from evse_charging_session group by analysis_id) as ecs
              on ets.analysis_id = ecs.analysis_id
join (select count(wait_id) as wait_count, analysis_id from evse_evs_waiting group by analysis_id) as ew
              on ets.analysis_id = ew.analysis_id
join (select analysis_id, set_id from analysis_record where set_id IN ({glue::glue_collapse(set_id, ', ')})) as ar ON ets.analysis_id = ar.analysis_id
group by ets.analysis_id, nevse_count, ef.fin_count, es.str_count, ef.evmt, ecs.cs_count, ew.wait_count, ar.set_id
order by ets.analysis_id desc
;"
)

count_stats_ws <- DBI::dbGetQuery(main_con, sql_count_stats_ws)

count_stats_ws_set_avg <-
  count_stats_ws %>% dplyr::group_by(set_id) %>% dplyr::summarise(mean_evmt = mean(evmt))

count_stats_ws_set_avg$delta_evmt <-
  count_stats_ws_set_avg$mean_evmt - count_stats_ws_set_avg$mean_evmt[count_stats_ws_set_avg$set_id == 102]

count_stats_ws_set_avg$type <-
  c('basecase', 'upgrade', 'upgrade', 'new', 'new', 'new', 'new')
count_stats_ws_set_avg$dcfc_count <- c(0, 5, 10, 5, 10, 5, 10)
count_stats_ws_set_avg$total_cost <- 0

for (i in rownames(count_stats_ws_set_avg)) {
  type <- count_stats_ws_set_avg[i, "type"]
  dcfc_count <- count_stats_ws_set_avg[i, "dcfc_count"]
  total_cost <- 0
  print(type)
  print(dcfc_count)
  for (evse_id in be_ws$evse_id)
  {
    evse_id <- substr(evse_id, start = 2, stop = nchar(evse_id))
    print(evse_id)
    cost <-
      dcfcoster::dcfc_cost(type = type,
                           new_plug_count = dcfc_count,
                           evse_id = evse_id)
    print(cost)
    total_cost <- total_cost + cost
  }
  print(total_cost)
  count_stats_ws_set_avg[i, "total_cost"] <- total_cost
}


count_stats_ws_set_avg$cost_eff <-
  count_stats_ws_set_avg$delta_evmt / count_stats_ws_set_avg$total_cost


write.csv(count_stats_ws_set_avg, "results_ws.csv")

# Plotting
x_labels <-
  c(
    "Upgrade with 5 chargers",
    "Upgrade with 10 chargers",
    "New - 1 with 5 chargers",
    "New - 1 with 10 chargers",
    "New - 2 with 5 chargers",
    "New - 2 with 10 chargers"
  )

xform <- list(categoryorder = "array",
              categoryarray = df$V1)

fig <-
  plotly::plot_ly(
    x = x_labels,
    y = count_stats_set_avg$cost_eff[2:nrow(count_stats_set_avg)]*1000,
    type = 'scatter',
    mode = 'lines+markers',
    name = 'Near max. charging sessions'
  )
fig <-
  fig %>% plotly::add_trace(
    x = x_labels,
    y = count_stats_ws_set_avg$cost_eff[2:nrow(count_stats_ws_set_avg)]*1000,
    type = 'scatter',
    mode = 'lines+markers',
    name = 'Near max. waiting sessions'
  )
fig
fig <-
  fig %>% plotly::layout(
    title = "Cost effectiveness of new/upgrade for various cases",
    xaxis = list(title = "Description", categoryorder = "array", type = 'category', categoryarray = x_labels),
    yaxis = list(title = "Cost Effectiveness (in evmt/k$)")
  )
fig
