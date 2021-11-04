use crate::HttpHealthEndpoint;
use anyhow::Result;
use std::{convert::Infallible, sync::Arc};
use warp::{hyper::StatusCode, Filter, Rejection, Reply};

pub fn get_readiness(
    health: Arc<HttpHealthEndpoint>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    get_readiness_request().and_then(move || {
        let health = health.clone();
        async move { Result::<_, Infallible>::Ok(get_readiness_response(health.is_ready())) }
    })
}

pub fn get_readiness_request() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::path("readiness")
}
pub fn get_readiness_response(is_ready: bool) -> impl Reply {
    match is_ready {
        true => StatusCode::NO_CONTENT,
        false => StatusCode::SERVICE_UNAVAILABLE,
    }
}
