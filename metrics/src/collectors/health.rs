use crate::{Configuration};
use datadog_statsd::Client;
use log::{debug, warn};

pub(crate) async fn check_health(cfg: &Configuration, dd: &Client) {
    let health_check = reqwest::get(format!("{}/healthz", cfg.hasura_addr)).await;
    match health_check {
        Ok(v) => {
            if v.status() == reqwest::StatusCode::OK {
                debug!("Healthcheck OK");
                dd.service_check(format!("{}.{}", cfg.prefix, "health").as_str(), datadog_statsd::client::ServiceCheckStatus::Ok, &None);
            } else {
                debug!("Healthcheck NOK");
                dd.service_check(format!("{}.{}", cfg.prefix, "health").as_str(), datadog_statsd::client::ServiceCheckStatus::Critical, &None);
            }
        },
        Err(e) => {
            dd.service_check(format!("{}.{}", cfg.prefix, "health").as_str(), datadog_statsd::client::ServiceCheckStatus::Unknown, &None);
            dd.incr("errors_total", &Some(vec!("type:health")));

            warn!("Failed to collect health check {}", e);
        }
    };
}
