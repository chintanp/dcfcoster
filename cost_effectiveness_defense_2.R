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
set_id <- c(125, 126, 127, 128)
sql_count_stats = glue::glue(
  "select count(ets.veh_id) as sim_count,
       ets.analysis_id,
       coalesce(ne.count, 0) as nevse_count,
       ef.fin_count,
       ef.evmt,
       es.str_count,
       ecs.cs_count,
       ew.wait_count,
       ar.set_id,
       ew.wait_duration
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
join (select count(wait_id) as wait_count, analysis_id, coalesce(sum(extract(minutes from (wait_end_time::timestamp - wait_start_time::timestamp))),
                         0) as wait_duration from evse_evs_waiting group by analysis_id) as ew
              on ets.analysis_id = ew.analysis_id
join (select analysis_id, set_id from analysis_record where set_id IN ({glue::glue_collapse(set_id, ', ')})) as ar ON ets.analysis_id = ar.analysis_id
group by ets.analysis_id, nevse_count, ef.fin_count, es.str_count, ef.evmt, ecs.cs_count, ew.wait_count, ar.set_id, ew.wait_duration
order by ets.analysis_id desc
;"
)

count_stats <- DBI::dbGetQuery(main_con, sql_count_stats)

count_stats$new_up_set_id <- NA

count_stats <- count_stats %>% dplyr::mutate(new_up_set_id =
  dplyr::case_when(
    671 <= analysis_id & analysis_id <= 675 ~ 0,
    676 <= analysis_id & analysis_id <= 680 ~ 1,
    681 <= analysis_id & analysis_id <= 685 ~ 2,
    686 <= analysis_id & analysis_id <= 690 ~ 3,
    691 <= analysis_id & analysis_id <= 695 ~ 4,
    696 <= analysis_id & analysis_id <= 700 ~ 5,
    701 <= analysis_id & analysis_id <= 705 ~ 6,
    706 <= analysis_id & analysis_id <= 710 ~ 7,
    711 <= analysis_id & analysis_id <= 715 ~ 8,
    716 <= analysis_id & analysis_id <= 720 ~ 9,
    721 <= analysis_id & analysis_id <= 725 ~ 10,
    726 <= analysis_id & analysis_id <= 730 ~ 11,
    731 <= analysis_id & analysis_id <= 735 ~ 12,
    736 <= analysis_id & analysis_id <= 740 ~ 13,
    741 <= analysis_id & analysis_id <= 745 ~ 14,
    746 <= analysis_id & analysis_id <= 750 ~ 15,
    751 <= analysis_id & analysis_id <= 755 ~ 16,
    756 <= analysis_id & analysis_id <= 760 ~ 17,
    761 <= analysis_id & analysis_id <= 765 ~ 18,
    766 <= analysis_id & analysis_id <= 770 ~ 19,
    771 <= analysis_id & analysis_id <= 775 ~ 20,
    776 <= analysis_id & analysis_id <= 780 ~ 21,
    781 <= analysis_id & analysis_id <= 785 ~ 22,
    786 <= analysis_id & analysis_id <= 790 ~ 23,
    791 <= analysis_id & analysis_id <= 795 ~ 24,
    796 <= analysis_id & analysis_id <= 800 ~ 25,
    801 <= analysis_id & analysis_id <= 805 ~ 26,
    806 <= analysis_id & analysis_id <= 810 ~ 27,
    811 <= analysis_id & analysis_id <= 815 ~ 28,
    816 <= analysis_id & analysis_id <= 820 ~ 29,
    821 <= analysis_id & analysis_id <= 825 ~ 30
  ))

count_stats_set_avg <-
  count_stats %>% dplyr::group_by(new_up_set_id) %>% dplyr::summarise(mean_evmt = mean(evmt), mean_wait_duration = mean(wait_duration))


count_stats_set_avg$delta_evmt <-
  count_stats_set_avg$mean_evmt - count_stats_set_avg$mean_evmt[count_stats_set_avg$new_up_set_id == 0]


count_stats_set_avg$type <-
  c(
    'basecase',
    'upgrade',
    'upgrade',
    'upgrade',
    'upgrade',
    'upgrade',
    'upgrade',
    'upgrade',
    'upgrade',
    'upgrade',
    'upgrade',
    'new',
    'new',
    'new',
    'new',
    'new',
    'new',
    'new',
    'new',
    'new',
    'new',
    'new',
    'new',
    'new',
    'new',
    'new',
    'new',
    'new',
    'new',
    'new',
    'new'
  )
count_stats_set_avg$dcfc_count <-
  c(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
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

write.csv(count_stats_set_avg, "results_ws2.csv")

# ## Locations of with max WS
# basecase_aid <-  484
# sql_be_ws = glue::glue(
#   "select count(wait_id) as count, analysis_id, evse_id, be.latitude, be.longitude
# from evse_evs_waiting ews
#          left join built_evse be on
#     'b' || cast(be.bevse_id as text) = ews.evse_id
# where ews.analysis_id = {basecase_aid}
# group by ews.analysis_id, ews.evse_id, be.latitude, be.longitude
# order by count desc
# limit 5;
# "
# )
# be_ws  <-  DBI::dbGetQuery(main_con, sql_be_ws)
#
# ## Stats for the sets with new and upgrades
# set_id <- c(102, 104, 105, 106, 107, 108, 109)
# sql_count_stats_ws = glue::glue(
#   "select count(ets.veh_id) as sim_count,
#        ets.analysis_id,
#        coalesce(ne.count, 0) as nevse_count,
#        ef.fin_count,
#        ef.evmt,
#        es.str_count,
#        ecs.cs_count,
#        ew.wait_count,
#        ar.set_id
# from evtrip_scenarios ets
#          left join (select count(nevse_id), analysis_id from new_evses group by analysis_id) as ne
#               on ets.analysis_id = ne.analysis_id
#          join (select count(veh_id) as fin_count, sum(distance_travelled) as evmt, analysis_id
#                from ev_finished
#                group by analysis_id) as ef
#               on ets.analysis_id = ef.analysis_id
#          join (select count(veh_id) as str_count, analysis_id from ev_stranded group by analysis_id) as es
#               on ets.analysis_id = es.analysis_id
#          join (select count(cs_id) as cs_count, analysis_id from evse_charging_session group by analysis_id) as ecs
#               on ets.analysis_id = ecs.analysis_id
# join (select count(wait_id) as wait_count, analysis_id from evse_evs_waiting group by analysis_id) as ew
#               on ets.analysis_id = ew.analysis_id
# join (select analysis_id, set_id from analysis_record where set_id IN ({glue::glue_collapse(set_id, ', ')})) as ar ON ets.analysis_id = ar.analysis_id
# group by ets.analysis_id, nevse_count, ef.fin_count, es.str_count, ef.evmt, ecs.cs_count, ew.wait_count, ar.set_id
# order by ets.analysis_id desc
# ;"
# )
#
# count_stats_ws <- DBI::dbGetQuery(main_con, sql_count_stats_ws)
#
# count_stats_ws_set_avg <-
#   count_stats_ws %>% dplyr::group_by(set_id) %>% dplyr::summarise(mean_evmt = mean(evmt))
#
# count_stats_ws_set_avg$delta_evmt <-
#   count_stats_ws_set_avg$mean_evmt - count_stats_ws_set_avg$mean_evmt[count_stats_ws_set_avg$set_id == 102]
#
# count_stats_ws_set_avg$type <-
#   c('basecase', 'upgrade', 'upgrade', 'new', 'new', 'new', 'new')
# count_stats_ws_set_avg$dcfc_count <- c(0, 5, 10, 5, 10, 5, 10)
# count_stats_ws_set_avg$total_cost <- 0
#
# for (i in rownames(count_stats_ws_set_avg)) {
#   type <- count_stats_ws_set_avg[i, "type"]
#   dcfc_count <- count_stats_ws_set_avg[i, "dcfc_count"]
#   total_cost <- 0
#   print(type)
#   print(dcfc_count)
#   for (evse_id in be_ws$evse_id)
#   {
#     evse_id <- substr(evse_id, start = 2, stop = nchar(evse_id))
#     print(evse_id)
#     cost <-
#       dcfcoster::dcfc_cost(type = type,
#                            new_plug_count = dcfc_count,
#                            evse_id = evse_id)
#     print(cost)
#     total_cost <- total_cost + cost
#   }
#   print(total_cost)
#   count_stats_ws_set_avg[i, "total_cost"] <- total_cost
# }
#
#
# count_stats_ws_set_avg$cost_eff <-
#   count_stats_ws_set_avg$delta_evmt / count_stats_ws_set_avg$total_cost
#
#
# write.csv(count_stats_ws_set_avg, "results_ws.csv")

# Plotting
x_labels <-
  c(
    "New / Upgrade with 1 chargers",
    "New / Upgrade with 2 chargers",
    "New / Upgrade with 3 chargers",
    "New / Upgrade with 4 chargers",
    "New / Upgrade with 5 chargers",
    "New / Upgrade with 6 chargers",
    "New / Upgrade with 7 chargers",
    "New / Upgrade with 8 chargers",
    "New / Upgrade with 9 chargers",
    "New / Upgrade with 10 chargers"
  )

ay <- list(
  tickfont = list(color = "red"),
  overlaying = "y",
  side = "right",
  title = "Cumulative Wait Duration (min)"
)



fig <-
  plotly::plot_ly(
    x = x_labels,
    y = count_stats_set_avg$cost_eff[2:11] * 1000,
    type = 'scatter',
    mode = 'lines+markers',
    name = 'Upgrade cost effectiveness'
  )
fig <-
  fig %>% plotly::add_trace(
    x = x_labels,
    y = count_stats_set_avg$cost_eff[12:21]*1000,
    type = 'scatter',
    mode = 'lines+markers',
    name = 'New - 1 cost effectiveness'
  )
fig <-
  fig %>% plotly::add_trace(
    x = x_labels,
    y = count_stats_set_avg$cost_eff[22:31]*1000,
    type = 'scatter',
    mode = 'lines+markers',
    name = 'New - 2 cost effectiveness'
  )

fig <-
  fig %>% plotly::layout(
    title = "Cost effectiveness of new/upgrade for various cases",
    xaxis = list(
      title = "Scenario Description",
      categoryorder = "array",
      type = 'category',
      categoryarray = x_labels
    ),
    yaxis = list(title = "Cost Effectiveness (in evmt/k$)")
  )
fig

fig2 <-
  plotly::plot_ly(
    x = x_labels,
    y = count_stats_set_avg$mean_wait_duration[2:11],
    type = 'scatter',
    mode = 'lines+markers',
    name = 'Upgrade wait durations'

  )
fig2 <-
  fig2 %>% plotly::add_trace(
    x = x_labels,
    y = count_stats_set_avg$mean_wait_duration[12:21],
    type = 'scatter',
    mode = 'lines+markers',
    name = 'New - 1 wait duration'

  )
fig2 <-
  fig2 %>% plotly::add_trace(
    x = x_labels,
    y = count_stats_set_avg$mean_wait_duration[22:31],
    type = 'scatter',
    mode = 'lines+markers',
    name = 'New - 2 wait duration'

  )
fig2 <-
  fig2 %>% plotly::add_trace(
    x = x_labels,
    y = count_stats_set_avg$mean_wait_duration[1:1],
    type = 'scatter',
    mode = 'lines+markers',
    name = 'Basecase'

  )

fig2 <-
  fig2 %>% plotly::layout(
    title = "Cumulative wait duration of new/upgrade for various cases",
    xaxis = list(
      title = "Scenario Description",
      categoryorder = "array",
      type = 'category',
      categoryarray = x_labels
    ),
    yaxis = list(title = "Cumulative Wait Duration (min)")
  )
fig2
