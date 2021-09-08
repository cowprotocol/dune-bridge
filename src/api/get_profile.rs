use crate::models::in_memory_database::{InMemoryDatabase, Profile};
use anyhow::Result;
use hex::{FromHex, FromHexError};
use primitive_types::H160;
use serde::Deserialize;
use serde::Serialize;
use std::str::FromStr;
use std::{convert::Infallible, sync::Arc};
use warp::{
    hyper::StatusCode,
    reply::{self, json, with_status, Json, WithStatus},
    Filter, Rejection, Reply,
};

/// Wraps H160 with FromStr and Deserialize that can handle a `0x` prefix.
#[derive(Deserialize)]
#[serde(transparent)]
pub struct H160Wrapper(pub H160);

impl FromStr for H160Wrapper {
    type Err = FromHexError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.strip_prefix("0x").unwrap_or(s);
        Ok(H160Wrapper(H160(FromHex::from_hex(s)?)))
    }
}

pub fn get_profile_request() -> impl Filter<Extract = (H160,), Error = Rejection> + Clone {
    warp::path!("profile" / H160Wrapper)
        .and(warp::get())
        .map(|token: H160Wrapper| token.0)
}

pub fn get_profile_response(result: Result<Profile>) -> WithStatus<Json> {
    match result {
        Ok(profile) => reply::with_status(reply::json(&profile), StatusCode::OK),
        Err(err) => convert_get_profile_error_to_reply(err),
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Error<'a> {
    error_type: &'a str,
    description: &'a str,
}

pub fn internal_error(err: anyhow::Error) -> Json {
    json(&Error {
        error_type: "InternalServerError",
        description: &format!("{:?}", err),
    })
}

pub fn convert_get_profile_error_to_reply(err: anyhow::Error) -> WithStatus<Json> {
    tracing::error!(?err, "get_profile error");
    with_status(internal_error(err), StatusCode::INTERNAL_SERVER_ERROR)
}

pub fn get_profile(
    db: Arc<InMemoryDatabase>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    get_profile_request().and_then(move |address| {
        let db = db.clone();
        async move {
            let result = db.get_profile_from_raw_data(address);
            Result::<_, Infallible>::Ok(get_profile_response(result))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::response_body;
    use hex_literal::hex;
    use primitive_types::H160;
    use warp::test::{request, RequestBuilder};

    #[tokio::test]
    async fn get_profile_request_ok() {
        let profile_filter = |request: RequestBuilder| async move {
            let filter = get_profile_request();
            request.method("GET").filter(&filter).await
        };

        let owner = H160::from_slice(&hex!("5ab290183b21559d8a7e55fc11920b4caadda137"));
        let path = format!("/profile/0x{:x}", owner);
        let request = request().path(path.as_str());
        let result = profile_filter(request).await.unwrap();
        assert_eq!(result, owner);
    }

    #[tokio::test]
    async fn get_orders_response_ok() {
        let profile = Profile::default();
        let response = get_profile_response(Ok(profile.clone())).into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_body(response).await;
        let response_profile: Profile = serde_json::from_slice(body.as_slice()).unwrap();
        assert_eq!(response_profile, profile);
    }
}
