extern crate serde_derive;
use super::dune_json_formats::Data;
use chrono::prelude::*;
use primitive_types::H160;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct InMemoryDatabase(pub HashMap<H160, Vec<Data>>, pub DateTime<Utc>);
