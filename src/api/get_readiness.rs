use crate::HttpHealthEndpoint;
use anyhow::Result;
use std::{convert::Infallible, sync::Arc};
use warp::{
    hyper::StatusCode,
    reply::{self, Json, WithStatus},
    Filter, Rejection, Reply,
};

pub fn get_readiness(
    health: Arc<HttpHealthEndpoint>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    get_readiness_request().and_then(move || {
        let health = health.clone();
        tracing::debug!("is ready response is{:?}", health.is_ready());
        async move { Result::<_, Infallible>::Ok(get_readiness_response(health.is_ready())) }
    })
}

pub fn get_readiness_request() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    warp::path("readiness")
}
pub fn get_readiness_response(is_ready: bool) -> WithStatus<Json> {
    match is_ready {
        true => reply::with_status(reply::json(&is_ready), StatusCode::NO_CONTENT),
        false => reply::with_status(reply::json(&is_ready), StatusCode::SERVICE_UNAVAILABLE),
    }
}
