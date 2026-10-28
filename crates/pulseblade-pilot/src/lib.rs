//! Event-driven investigator. It polls MCP for findings and changes, and calls
//! an OpenAI-compatible model only when something is worth looking at.

mod chat;
mod config;
mod mcp;
mod run;
mod spend;

pub use chat::{parse_completion, Chat, HttpChat};
pub use config::{is_loopback, BudgetConfig, EndpointConfig, PilotConfig, TierConfig};
pub use mcp::{McpClient, Tools};
pub use run::{PilotRun, Session};
pub use spend::SpendRecord;
