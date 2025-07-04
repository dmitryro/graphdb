// server/src/cli/mod.rs

// This file declares the modules within the 'cli' directory and re-exports
// common types and functions for easier access from other parts of the crate.

pub mod cli; // Declare cli.rs as a submodule
pub mod commands;
pub mod handlers;
pub mod interactive;
pub mod config;
pub mod daemon_management;
pub mod help_display; // Declare the new help_display module

// Re-export necessary types and functions from sub-modules
// These re-exports allow other modules (including cli.rs) to access items
// without needing verbose paths (e.g., `use crate::cli::commands::CliArgs;`
// can become `use crate::cli::CliArgs;` if re-exported here).

pub use cli::{start_cli}; // Re-export the main CLI entry point from cli.rs
pub use commands::{
    CliArgs, GraphDbCommands, DaemonCliCommand, RestCliCommand, StorageAction,
    StatusAction, StopAction, StatusArgs, StopArgs, // Removed HelpArgs from here
    DaemonData, KVPair, PidStore,
};
pub use handlers::{
    display_rest_api_status, display_daemon_status, display_storage_daemon_status,
    display_full_status_summary, stop_process_by_port, check_process_status_by_port,
    print_welcome_screen,
};
pub use interactive::{
    parse_command, print_interactive_help, print_interactive_filtered_help, CommandType,
    run_cli_interactive, handle_interactive_command,
};
pub use config::{
    load_cli_config, load_storage_config, StorageConfig, StorageEngineType,
    CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
};
pub use daemon_management::{
    find_running_storage_daemon_port, start_daemon_process, stop_daemon_api_call,
    handle_internal_daemon_run,
};
pub use help_display::{print_help_clap_generated, print_filtered_help_clap_generated, HelpArgs}; // Corrected: Re-export HelpArgs from help_display

// Lazy static for shared memory keys (if still needed globally across daemons)
// Keeping it here for now as it's a global state that might be accessed by various parts.
lazy_static::lazy_static! {
    pub static ref SHARED_MEMORY_KEYS: tokio::sync::Mutex<std::collections::HashSet<i32>> = tokio::sync::Mutex::new(std::collections::HashSet::new());
}

