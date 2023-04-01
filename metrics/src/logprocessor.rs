use datadog_statsd::Client;
use log::warn;

use serde::Deserialize;
use serde_json::{from_str, from_value};


#[derive(Deserialize)]
pub struct BaseLog {
    #[serde(rename = "timestamp")]
    pub timestamp: String,
    #[serde(rename = "level")]
    pub level: String,
    #[serde(rename = "type")]
    pub logtype: String,
    #[serde(rename = "detail")]
    pub detail: serde_json::Value,
}

#[derive(Deserialize)]
pub struct HttpLogDetailHttpInfo {
    #[serde(rename = "status")]
    pub status: i32,
    #[serde(rename = "http_version")]
    pub http_version: String,
    #[serde(rename = "url")]
    pub url: String,
    #[serde(rename = "method")]
    pub method: String,
    #[serde(rename = "ip")]
    pub ip: String,
}

#[derive(Deserialize)]
pub struct HttpLogDetailOperationError {
    #[serde(rename = "path")]
    pub path: String,
    #[serde(rename = "error")]
    pub error: String,
    #[serde(rename = "code")]
    pub code: String,
}

#[derive(Deserialize)]
pub struct HttpLogDetailOperationQuery {
    #[serde(rename = "operationName")]
    pub operation_name: Option<String>,
    #[serde(rename = "query")]
    pub query: Option<String>,
}

#[derive(Deserialize)]
pub struct HttpLogDetailOperation {
    #[serde(rename = "query_execution_time")]
    pub query_execution_time: Option<f64>,
    #[serde(rename = "request_id")]
    pub request_id: String,
    #[serde(rename = "parameterized_query_hash")]
    pub parameterized_query_hash: Option<String>,
    #[serde(rename = "response_size")]
    pub response_size: i32,
    #[serde(rename = "error")]
    pub error: Option<HttpLogDetailOperationError>,
    #[serde(rename = "query")]
    pub query: Option<HttpLogDetailOperationQuery>,
}

#[derive(Deserialize)]
pub struct HttpLogDetails {
    #[serde(rename = "request_id")]
    pub request_id: String,
    #[serde(rename = "operation")]
    pub operation: HttpLogDetailOperation,
    pub http_info: HttpLogDetailHttpInfo,
}

async fn handle_http_log(log: &BaseLog, dd: &Client) {
    let detail_result = from_value::<HttpLogDetails>(log.detail.clone());
    match detail_result {
        Ok(http) => {
            dd.incr("request_counter", &Some(vec!(format!("url:{}", http.http_info.url).as_str(),
             format!("status:{}", http.http_info.status).as_str(), format!("logtype:{}", log.logtype).as_str())));

            if let Some(query) = http.operation.query {
                let error = http.operation.error.map_or("".to_string(), |v| v.code);

                let operation = query.operation_name.unwrap_or("".to_string());
                dd.incr("request_query_counter", &Some(vec!(operation.as_str(), error.as_str())));

                if let Some(exec_time) = http.operation.query_execution_time {
                    dd.timer("query_execution_seconds", exec_time, &Some(vec!(operation.as_str(), error.as_str())));
                }
            }
        }
        Err(e) => {
            eprintln!("Invalid HTTP log detail: {}", e);
        }
    };
}

#[derive(Deserialize)]
pub struct WebSocketDetailEvent {
    #[serde(rename = "type")]
    pub event_type: String,

    #[serde(rename = "detail")]
    pub detail: Option<WebSocketDetailEventDetail>,
}

#[derive(Deserialize)]
pub struct WebSocketDetailEventDetailOperationType {
    #[serde(rename = "type")]
    pub operation_type: String,
    #[serde(rename = "detail")]
    pub detail: Option<HttpLogDetailOperationError>,
}

#[derive(Deserialize)]
pub struct WebSocketDetailEventDetail {
    #[serde(rename = "operation_name")]
    pub operation_name: Option<String>,
    #[serde(rename = "request_id")]
    pub request_id: Option<String>,
    #[serde(rename = "operation_type")]
    pub operation_type: WebSocketDetailEventDetailOperationType,
}

#[derive(Deserialize)]
pub struct WebSocketDetailConnInfo {}

#[derive(Deserialize)]
pub struct WebSocketDetail {
    #[serde(rename = "event")]
    pub event: WebSocketDetailEvent,
    #[serde(rename = "connection_info")]
    pub connection_info: WebSocketDetailConnInfo,
}

async fn handle_websocket_log(log: &BaseLog, dd: &Client) {
    let detail_result = from_value::<WebSocketDetail>(log.detail.clone());
    match detail_result {
        Ok(http) => {
            match &http.event.event_type as &str {
                "accepted" => dd.incr("websockets_active", &None),
                "closed" => dd.decr("websockets_active", &None),
                "operation" => {
                    if let Some(detail) = http.event.detail {
                        let op_name = detail.operation_name.unwrap_or("".to_string());
                        match &detail.operation_type.operation_type as &str {
                            "started" => dd.incr("active_websocket_operations", &None),
                            "stopped" => {
                                dd.incr("websockets_operations", &Some(vec!(format!("opname:{}", op_name).as_str())));
                                dd.decr("active_websocket_operations", &None);
                            }
                            "query_err" => {
                                let err = detail
                                    .operation_type
                                    .detail
                                    .map_or("".to_string(), |v| v.code);
                                dd.incr("websockets_operations", &Some(vec!(format!("opname:{}", op_name).as_str(), format!("error:{}", err).as_str())));
                            }
                            _ => (),
                        };
                    }
                }
                _ => (),
            };
        }
        Err(e) => {
            warn!("Invalid Websocket log detail: {}", e);
        }
    };
}

pub async fn log_processor(logline: &String, dd: &Client) {
    //println!("{}", logline);
    dd.incr("log_lines_counter_total", &None);
    let log_result = from_str::<BaseLog>(logline);
    match log_result {
        Ok(log) => {
            dd.incr("log_lines_counter", &Some(vec!(format!("logtype:{}", log.logtype).as_str())));
            match &log.logtype as &str {
                "http-log" => {
                    handle_http_log(&log, dd).await;
                }
                "websocket-log" => {
                    handle_websocket_log(&log, dd).await;
                }
                _ => {}
            };
        }
        Err(e) => {
            warn!("Failed to parse log line: {}", e);
        }
    };
}
