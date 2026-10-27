//! Schema types shared across Pulseblade.
//!
//! Every type derives [`schemars::JsonSchema`] so agents can discover the exact
//! shape of what the MCP surface returns.

mod health;
mod labels;
mod model;
mod remedy;
mod time;

pub use health::{
    default_time_format, default_timezone, valid_time_format, valid_timezone, HealthThresholds,
};
pub use labels::{glob_match, LabelRule};
pub use model::*;
pub use remedy::{bind_remedy_params, generalize_params, resource_fields, resource_kind_of_id};
pub use time::{parse_duration, Since, SinceParseError};
