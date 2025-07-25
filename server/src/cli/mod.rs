// server/src/cli/mod.rs
// This module contains the command-line interface (CLI) logic for the GraphDB server.
// It includes argument parsing, command handling, and interactive mode.

pub mod cli;
pub mod commands;
pub mod config;
pub mod daemon_management;
pub mod handlers;
pub mod help_display;
pub mod interactive;
pub mod network_utilities;

// Re-export the main CLI entry point from cli.rs
pub use cli::{start_cli, CliArgs, Commands};

// Re-export specific types/functions from other modules if they are part of the public CLI API
pub use commands::{
    DaemonCliCommand,
    RestCliCommand,
    StorageAction,
    StatusArgs,
    StopArgs,
    ReloadArgs,
    RestartArgs,
    StopAction,
    ReloadAction,
    RestartAction,
    StatusAction,
};
pub use handlers::{
    handle_daemon_command,
    handle_rest_command,
    handle_storage_command,
    handle_status_command,
    handle_stop_command,
    handle_start_command,
    handle_restart_command,
    handle_reload_command,
    handle_restart_command_interactive,
};
pub use interactive::{
    run_cli_interactive,
};
pub use help_display::{
    print_interactive_help,
    print_interactive_filtered_help,
    print_help_clap_generated,
    print_filtered_help_clap_generated,
    collect_all_cli_elements_for_suggestions,
};
pub use config::{
    load_cli_config,
    load_storage_config,
    CliConfig,
    ServerConfig,
    StorageConfig,
    StorageEngineType,
    CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
    get_default_rest_port_from_config,
};
pub use daemon_management::{
    start_daemon_process,
    stop_daemon_api_call,
    find_running_storage_daemon_port,
    clear_all_daemon_processes,
};
