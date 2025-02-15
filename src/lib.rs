pub mod query_parser; // Ensure this line exists
pub mod cli_engine;
pub mod graph_evolution;
pub mod network_interfaces;
pub mod plugin_system;
pub mod query_exec_engine;
pub mod rest_api;
pub mod storage_engine;
pub mod transact_indexing;
pub mod indexing_caching; // Import the module
pub use crate::indexing_caching::{index_node, cache_node_state, get_cached_node_state};
