//! Schema types shared across Pulseblade.
//!
//! Every type derives [`schemars::JsonSchema`] so agents can discover the exact
//! shape of what the MCP surface returns.

mod labels;
mod model;
mod time;

pub use labels::{glob_match, LabelRule};
pub use model::*;
pub use time::{parse_duration, Since, SinceParseError};
