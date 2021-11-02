mod get_profile;
mod get_readiness;
use crate::models::in_memory_database::InMemoryDatabase;
use crate::HttpHealthEndpoint;
use std::{convert::Infallible, sync::Arc};
use warp::{hyper::StatusCode, Filter, Rejection, Reply};

pub fn handle_all_routes(
    memory_database: Arc<InMemoryDatabase>,
    health: Arc<HttpHealthEndpoint>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let get_profile = get_profile::get_profile(memory_database);
    let health_filter = get_readiness::get_readiness(health);
    let cors = warp::cors()
        .allow_any_origin()
        .allow_methods(vec!["GET", "POST", "DELETE", "OPTIONS", "PUT", "PATCH"])
        .allow_headers(vec!["Origin", "Content-Type", "X-Auth-Token", "X-AppId"]);
    let api_routes = warp::path!("api" / "v1" / ..).and(get_profile);
    let health_routes = warp::path!("health" / ..).and(health_filter);
    api_routes
        .or(health_routes)
        .recover(handle_rejection)
        .with(cors)
}
// We turn Rejection into Reply to workaround warp not setting CORS headers on rejections.
async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    Ok(warp::reply::with_status(
        format!("{:?}", err),
        StatusCode::INTERNAL_SERVER_ERROR,
    ))
}
#[cfg(test)]
async fn response_body(response: warp::hyper::Response<warp::hyper::Body>) -> Vec<u8> {
    let mut body = response.into_body();
    let mut result = Vec::new();
    while let Some(bytes) = futures::StreamExt::next(&mut body).await {
        result.extend_from_slice(bytes.unwrap().as_ref());
    }
    result
}
