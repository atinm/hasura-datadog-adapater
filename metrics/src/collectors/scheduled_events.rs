use super::sql::*;
use crate::{Configuration};
use datadog_statsd::Client;
use log::{warn, info, debug};

fn create_scheduled_event_request() -> SQLRequest {
    SQLRequest {
            request_type: "bulk".to_string(),
            args: vec![
                RunSQLQuery{
                    request_type: "run_sql".to_string(),
                    args: RunSQLArgs {
                        source: "default".to_string(),
                        cascade: false,
                        read_only: true,
                        sql: "SELECT COUNT(*) FROM hdb_catalog.hdb_scheduled_events WHERE status = 'error';".to_string()
                    }
                },
                RunSQLQuery{
                    request_type: "run_sql".to_string(),
                    args: RunSQLArgs {
                        source: "default".to_string(),
                        cascade: false,
                        read_only: true,
                        sql: "SELECT COUNT(*) FROM hdb_catalog.hdb_scheduled_events WHERE status = 'delivered';".to_string()
                    }
                },
                RunSQLQuery{
                    request_type: "run_sql".to_string(),
                    args: RunSQLArgs {
                        source: "default".to_string(),
                        cascade: false,
                        read_only: true,
                        sql: "SELECT COUNT(*) FROM hdb_catalog.hdb_scheduled_events WHERE status = 'scheduled';".to_string()
                    }
                },
                RunSQLQuery{
                    request_type: "run_sql".to_string(),
                    args: RunSQLArgs {
                        source: "default".to_string(),
                        cascade: false,
                        read_only: true,
                        sql: "SELECT COUNT(*) FROM hdb_catalog.hdb_scheduled_events WHERE status = 'error' or status = 'delivered';".to_string()
                    }
                },
            ],
        }
}

pub(crate) async fn check_scheduled_events(cfg: &Configuration, dd: &Client) {
    if cfg.disabled_collectors.contains(&crate::Collectors::ScheduledEvents) {
        info!("Not collecting scheduled event.");
        return;
    }
    debug!("Running SQL query for scheduled events");
    let sql_result = make_sql_request(&create_scheduled_event_request(), cfg).await;
    match sql_result {
        Ok(v) => {

            if v.status() == reqwest::StatusCode::OK {
                let response = v.json::<Vec<SQLResult>>().await;
                match response {
                    Ok(v) => {
                        v.iter().enumerate().for_each(|(index, query)| {
                            let obj = match index as i32 {
                                // Index values must match create_scheduled_event_request() for coherence
                                0 => Ok("failed_one_off_events"),
                                1 => Ok("successful_one_off_events"),
                                2 => Ok("pending_one_off_events"),
                                3 => Ok("processed_one_off_events"),
                                _ => {
                                    warn!("Unexpected entry {:?}",query);
                                    Err(format!("Unexpected entry {:?}",query))
                                }
                            };

                            process_sql_result(query, dd, obj, None);
                        });
                    }
                    Err(e) => {
                        warn!( "Failed to collect scheduled event check invalid response format: {}", e );
                        dd.incr("errors_total", &Some(vec!("type:scheduled")));
                    }
                }
            } else {
                warn!(
                    "Failed to collect scheduled event check invalid status code: {}",
                    v.status()
                );
                dd.incr("errors_total", &Some(vec!("type:scheduled")));
            }
        }
        Err(e) => {
            dd.incr("errors_total", &Some(vec!("type:scheduled")));
            warn!("Failed to collect scheduled event check {}", e);
        }
    };
}
