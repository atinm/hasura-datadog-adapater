use std::sync::mpsc;
use std::sync::mpsc::RecvTimeoutError;
use datadog_statsd::Client;
use log::debug;
use crate::{Configuration};

mod sql;
mod health;
mod metadata;
mod scheduled_events;
mod cron_triggers;
mod event_triggers;

pub(crate) async fn run_metadata_collector(cfg: &Configuration, client: &Client, termination_rx: &mpsc::Receiver<()>) -> std::io::Result<()> {
    loop {
        debug!("Running metadata collector");

        tokio::join!(
            health::check_health(cfg, client),
            scheduled_events::check_scheduled_events(&cfg, client),
            cron_triggers::check_cron_triggers(&cfg, client),
            async {
                let metadata = metadata::check_metadata(cfg, client).await;
                event_triggers::check_event_triggers(&cfg, client, &metadata).await;
            }
        );

        match termination_rx.recv_timeout(std::time::Duration::from_millis(cfg.collect_interval)) {
            Ok(_) | Err(RecvTimeoutError::Disconnected) => return Ok(()),
            Err(RecvTimeoutError::Timeout) => () //continue
        }
    }
}
