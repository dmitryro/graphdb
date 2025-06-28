use graphdb_daemon::daemonize::{Daemonize, DaemonizeBuilder};
use clap::{Parser, Subcommand};
use crossterm::{
    cursor,
    style::{self, Color},
    terminal::{Clear, ClearType},
    ExecutableCommand,
};
use std::fs::{File};
use std::io::{self, Write, Read};
use std::process::{Command, Child, exit, id};
use std::sync::Mutex;
use std::vec::Vec;
use graphdb_lib::query_parser::{parse_query_from_string, QueryType};
use config::{Config, File as ConfigFile}; // Assuming the config crate is used
use std::path::Path;
use std::mem::size_of;
use std::ptr::null_mut;
use serde_json::to_vec;
use serde::{Serialize, Deserialize};
use libc::{IPC_PRIVATE, IPC_CREAT, IPC_RMID}; // Import necessary constants
use shared_memory::Shmem;
use shared_memory::ShmemConf;
use shm::{shmctl, shmget};
use libc::{SHM_RDONLY, SHM_RND};
use std::ptr;
use std::ffi::CString;
use once_cell::sync::Lazy;
use std::collections::HashSet;
use lazy_static::lazy_static;
use nix::unistd::getpid;
use nix::unistd::Pid; // Assuming you're using the nix crate for Pid type
use std::process; // Importing the process module

// Ensure SHARED_MEMORY_KEYS is globally mutable
lazy_static! {
    static ref SHARED_MEMORY_KEYS: Mutex<HashSet<i32>> = Mutex::new(HashSet::new());
}

#[derive(Parser, Debug)]
#[command(name = "graphdb-cli")]
#[command(version = "0.1.0")]
#[command(about = "Experimental Graph Database CLI")]
struct CliArgs {
    #[command(subcommand)]
    command: Option<GraphDbCommands>,
}

#[derive(Serialize, Deserialize, Debug)]
struct DaemonData {
    port: u16,
    host: String,
    pid: u32, // Change this to i32
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct KVPair {
    key: String,
    value: Vec<u8>,
}


#[derive(Subcommand, Debug)]
enum GraphDbCommands {
    ViewGraph {
        #[arg(value_name = "GRAPH_ID")]
        graph_id: Option<u32>,
    },
    ViewGraphHistory {
        #[arg(value_name = "GRAPH_ID")]
        graph_id: Option<u32>,
        #[arg(value_name = "START_DATE")]
        start_date: Option<String>,
        #[arg(value_name = "END_DATE")]
        end_date: Option<String>,
    },
    IndexNode {
        #[arg(value_name = "NODE_ID")]
        node_id: Option<u32>,
    },
    CacheNodeState {
        #[arg(value_name = "NODE_ID")]
        node_id: Option<u32>,
    },
    Start {
        #[arg(short = 'p', long = "port", value_name = "PORT")]
        port: Option<u16>,
    },
    Stop,
}

#[derive(Debug, Serialize, Deserialize)] // Add Serialize and Deserialize
struct SharedData {
    port: u16,
    running: bool,
    host: String,
    pid: u32,
    shm_id: i32,
}

#[derive(Serialize, Deserialize, Debug)]
struct PidStore {
    pid: u32,
}

pub fn start_cli() {
    let args = CliArgs::parse();

    match args.command {
        Some(command) => match command {
            GraphDbCommands::ViewGraph { graph_id } => {
                if let Some(id) = graph_id {
                    println!("Executing view-graph for graph ID: {}", id);
                } else {
                    println!("Executing view-graph without specifying a graph ID");
                }
            }
            GraphDbCommands::ViewGraphHistory { graph_id, start_date, end_date } => {
                if let Some(graph_id) = graph_id {
                    println!("Executing view-graph-history for graph ID: {}", graph_id);
                } else {
                    println!("Executing view-graph-history with no graph ID specified");
                }

                if let Some(start) = start_date {
                    println!("Start Date: {}", start);
                } else {
                    println!("Start Date: not specified");
                }

                if let Some(end) = end_date {
                    println!("End Date: {}", end);
                } else {
                    println!("End Date: not specified");
                }
            }
            GraphDbCommands::IndexNode { node_id } => {
                if let Some(id) = node_id {
                    println!("Executing index-node for node ID: {}", id);
                } else {
                    println!("Executing index-node with no node ID specified");
                }
            }
            GraphDbCommands::CacheNodeState { node_id } => {
                if let Some(id) = node_id {
                    println!("Executing cache-node-state for node ID: {}", id);
                } else {
                    println!("Executing cache-node-state with no node ID specified");
                }
            }
            GraphDbCommands::Start { port } => {
                start_daemon(port);
            }
            GraphDbCommands::Stop => {
                stop_daemon();
            }
        },
        None => {
            interactive_cli();
        }
    }
}

fn start_daemon(port: Option<u16>) {
    let config_path = "server/src/cli/config.toml";

    let mut host_to_use = "127.0.0.1".to_string();
    let mut port_to_use = port.unwrap_or(8080); // CLI flag takes priority
    let mut process_name_to_use = "graphdb-cli".to_string();

    // Use config only if port is not provided via CLI flag
    if port.is_none() && Path::new(config_path).exists() {
        let config = Config::builder()
            .add_source(config::File::with_name(config_path))
            .build()
            .unwrap();

        if let Ok(host) = config.get_string("server.host") {
            host_to_use = host;
        }
        if let Ok(process_name) = config.get_string("daemon.process_name") {
            process_name_to_use = process_name;
        }
        if let Ok(port) = config.get_int("server.port") {
            port_to_use = port as u16;
        }
    }

    // Output HashSet before daemon starts
    {
        let shared_memory_keys = SHARED_MEMORY_KEYS.lock().unwrap();
        println!("Before daemon start, HashSet contents: {:?}", *shared_memory_keys);
    }

    let stdout = File::create("/tmp/daemon.out").unwrap();
    let stderr = File::create("/tmp/daemon.err").unwrap();

    let mut daemonize = DaemonizeBuilder::new()
        .working_directory("/tmp")
        .umask(0o027)
        .stdout(stdout)
        .stderr(stderr)
        .process_name(&process_name_to_use)
        .host(&host_to_use)
        .port(port_to_use)
        .build()
        .expect("Failed to build Daemonize object");

    // Start the daemon and capture the child PID
    match daemonize.start() {
        Ok(child_pid) => {
            {
                let mut shared_memory_keys = SHARED_MEMORY_KEYS.lock().unwrap();
                shared_memory_keys.insert(child_pid as i32);
                println!("After inserting child PID, HashSet contents: {:?}", *shared_memory_keys);
            }

            println!("Daemon started with PID: {}", child_pid);
            println!("Daemon is listening on {}:{} PID:{}", host_to_use, port_to_use, child_pid);
            let mut shared_memory_keys = SHARED_MEMORY_KEYS.lock().unwrap();
            shared_memory_keys.insert(child_pid as i32);
            println!("After inserting child PID, HashSet contents: {:?}", *shared_memory_keys);
        }
        Err(e) => {
            eprintln!("Daemonization failed: {}", e);
        }
    }
    let mut shared_memory_keys = SHARED_MEMORY_KEYS.lock().unwrap();
     println!("On exit, after  inserting child PID, HashSet contents: {:?}", *shared_memory_keys);   
}

fn stop_daemon() {
    // Output HashSet before reading PID
    let mut shared_memory_keys = SHARED_MEMORY_KEYS.lock().unwrap();
    println!("Before existing, HashSet contents: {:?}", *shared_memory_keys);
    // Retrieve the stored PID from the shared memory keys
    let pid: i32 = unsafe {
        let shared_memory_keys = SHARED_MEMORY_KEYS.lock().unwrap();
        if let Some(&stored_pid) = shared_memory_keys.iter().next() { // Assuming one PID is stored
            println!("Found stored PID {} in HashSet", stored_pid);
            stored_pid
        } else {
            eprintln!("No PID found in HashSet, daemon may not be running.");
            return;
        }
    };

    // Attempt to stop the daemon (daemonizer stop)
    let daemonizer = DaemonizeBuilder::new();
    match daemonizer.stop() {
        Ok(_) => {
            println!("Successfully stopped daemon with PID: {}", pid);
        }
        Err(e) => {
            eprintln!("Failed to stop daemon: {}", e);
        }
    }

    // Remove the shared memory key from HashSet
    unsafe {
        let mut shared_memory_keys = SHARED_MEMORY_KEYS.lock().unwrap();
        if shared_memory_keys.remove(&pid) { // Remove the actual PID
            println!("Removed shared memory key {} from HashSet", pid);
        } else {
            eprintln!("Failed to remove shared memory key {} from HashSet", pid);
        }

        // Output HashSet after removing key
        println!("After daemon stop, HashSet contents: {:?}", *shared_memory_keys);
    }
}

fn store_kv_pair(shmem: &Shmem, offset: usize, key: String, value: Vec<u8>) {
    let kv_store_ptr = unsafe { shmem.as_ptr().add(offset) as *mut KVPair };
    const NUM_ENTRIES: usize = 10;

    // Convert the key (String) to i32 before working with the keys set
    let key_as_i32 = match key.parse::<i32>() {
        Ok(parsed_key) => parsed_key,
        Err(_) => {
            eprintln!("Failed to parse key: {}", key);
            return; // If key can't be parsed as i32, return early
        }
    };

    // Check if the shared memory segment is already in use
    let mut keys = SHARED_MEMORY_KEYS.lock().unwrap();
    if keys.contains(&key_as_i32) {
        eprintln!("Shared memory key {} is already in use!", key);
        return; // Avoid inserting the value if key exists
    }

    // Look for an empty spot in the key-value store
    for i in 0..NUM_ENTRIES {
        let entry_ptr = unsafe { kv_store_ptr.add(i) };
        let entry = unsafe { entry_ptr.as_mut().unwrap() }; // Get a mutable reference

        if entry.key.is_empty() {
            entry.key = key.clone(); // Clone key to avoid moving it
            entry.value = value;

            // Track this key in the HashSet to avoid further inserts
            keys.insert(key_as_i32); // Insert the key (as i32)
    
            return; // Exit after storing
        }
    }
    eprintln!("Key-value store is full!");
}

fn retrieve_kv_pair(shmem: &Shmem, offset: usize, key: &String) -> Option<Vec<u8>> {
    let kv_store_ptr = unsafe { shmem.as_ptr().add(offset) as *const KVPair };
    const NUM_ENTRIES: usize = 10; // Or however many entries you have

    // Check if the key is already in the shared memory keys set
    let keys = SHARED_MEMORY_KEYS.lock().unwrap();

    // Convert key (String) to i32 before calling contains
    let key_as_i32 = key.parse::<i32>();

    // Ensure the conversion is successful before proceeding
    if let Ok(key_i32) = key_as_i32 {
        if !keys.contains(&key_i32) {
            eprintln!("Key {} not found in shared memory!", key);
            return None; // If key is not found, return None
        }
    } else {
        eprintln!("Failed to parse key: {}", key);
        return None; // If the key couldn't be parsed as i32, return None
    }

    // Look for the key in the key-value store
    for i in 0..NUM_ENTRIES {
        let entry_ptr = unsafe { kv_store_ptr.add(i) };
        let entry = unsafe { entry_ptr.as_ref().unwrap() }; // Dereference and get a reference
    
        if entry.key == *key { // Compare the keys
            return Some(entry.value.clone()); // Return a clone of the value
        }
    }
    None // Return None if the key is not found
}

fn interactive_cli() {
    let valid_commands: HashSet<&str> = [
        "help", "status", "list", "connect", "clear", "view-graph", "view-graph-history",
        "index-node", "cache-node-state", "exit", "quit", "q", "start", "stop", "--port", "--host",
    ]
    .iter()
    .cloned()
    .collect();

    let mut stdout = io::stdout();
    stdout.execute(Clear(ClearType::All)).expect("Failed to clear screen");
    stdout.execute(cursor::MoveTo(0, 0)).expect("Failed to move cursor");

    stdout
        .execute(style::SetForegroundColor(Color::Cyan))
        .expect("Failed to set color");
    writeln!(
        stdout,
        "\nWelcome to GraphDB CLI\nType a command and press Enter. Type 'exit', 'quit', or 'q' to quit.\n"
    )
    .expect("Failed to write greeting");
    stdout.execute(style::ResetColor).expect("Failed to reset color");

    stdout.flush().expect("Failed to flush stdout");

    loop {
        stdout
            .execute(style::SetForegroundColor(Color::Cyan))
            .expect("Failed to set color");
        print!("=> ");
        stdout.execute(style::ResetColor).expect("Failed to reset color");
        io::stdout().flush().expect("Failed to flush stdout");

        let mut input = String::new();
        if let Err(e) = io::stdin().read_line(&mut input) {
            println!("Error reading input: {}", e);
            continue;
        }

        let command = input.trim();

        if valid_commands.contains(command) {
            if command == "exit" || command == "quit" || command == "q" {
                println!("\nExiting GraphDB CLI... Goodbye!\n");
                break;
            }
            println!("Executing command: {}", command);

            match command {
                "view-graph" => {
                    println!("Executing view-graph command");
                    // Add your logic here
                }
                "view-graph-history" => {
                    println!("Executing view-graph-history command");
                    // Add your logic here
                }
                "index-node" => {
                    println!("Executing index-node command");
                    // Add your logic here
                }
                "cache-node-state" => {
                    println!("Executing cache-node-state command");
                    // Add your logic here
                }
                "start" => {
                    start_daemon(None);
                }
                "stop" => {
                    stop_daemon();
                }
                _ => {
                    println!("Unknown command: {}", command);
                }
            }
        } else if !command.is_empty() {
            match parse_query_from_string(command) {
                Ok(parsed_query) => match parsed_query {
                    QueryType::Cypher => {
                        println!("Cypher query detected: {}", command);
                    }
                    QueryType::SQL => {
                        println!("SQL query detected: {}", command);
                    }
                    QueryType::GraphQL => {
                        println!("GraphQL query detected: {}", command);
                    }
                },
                Err(_) => {
                    stdout
                        .execute(style::SetForegroundColor(Color::Yellow))
                        .expect("Failed to set color");
                    println!("Unknown command: {}", command);
                    stdout.execute(style::ResetColor).expect("Failed to reset color");
                }
            }
        }
    }
}
