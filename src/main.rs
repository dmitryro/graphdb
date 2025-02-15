use clap::{Arg, Command, Subcommand};
use graph_database::cli::cli::start_cli;
use graph_database::indexing_caching::{index_node, cache_node_state};

#[derive(Subcommand)]
enum CliCommands {
    #[command(about = "View the current state of the graph")]
    ViewGraph {
        #[arg(help = "Specify the node ID to view")]
        node: String,
    },
    #[command(about = "View historical states of the graph")]
    ViewGraphHistory {
        #[arg(help = "Specify the node ID")]
        node: String,
        #[arg(long, help = "Specify start timestamp for history")]
        start: Option<String>,
        #[arg(long, help = "Specify end timestamp for history")]
        end: Option<String>,
    },
    #[command(about = "Index a new node in the graph")]
    IndexNode {
        #[arg(help = "Specify the node ID to index")]
        node: String,
        #[arg(help = "Specify the node data")]
        data: String,
    },
    #[command(about = "Cache a node state")]
    CacheNodeState {
        #[arg(help = "Specify the node ID to cache")]
        node: String,
        #[arg(help = "Specify the node state to cache")]
        state: String,
    },
    #[command(about = "Default command")]
    Default,
}

fn main() {
    let matches = Command::new("graphdb-cli")
        .version("1.0")
        .author("Your Name <youremail@example.com>")
        .about("Command line interface for graph database")
        .subcommand_required(false)
        .subcommand(
            Command::new("view-graph")
                .about("View the current state of the graph")
                .arg(Arg::new("node").help("Specify the node ID to view").required(true)),
        )
        .subcommand(
            Command::new("view-graph-history")
                .about("View historical states of the graph")
                .arg(Arg::new("node").help("Specify the node ID").required(true))
                .arg(Arg::new("start").long("start").num_args(1).help("Specify start timestamp for history"))
                .arg(Arg::new("end").long("end").num_args(1).help("Specify end timestamp for history")),
        )
        .subcommand(
            Command::new("index-node")
                .about("Index a new node in the graph")
                .arg(Arg::new("node").help("Specify the node ID to index").required(true))
                .arg(Arg::new("data").help("Specify the node data").required(true)),
        )
        .subcommand(
            Command::new("cache-node-state")
                .about("Cache a node state")
                .arg(Arg::new("node").help("Specify the node ID to cache").required(true))
                .arg(Arg::new("state").help("Specify the node state to cache").required(true)),
        )
        .get_matches();

    if let Some(("view-graph", sub_m)) = matches.subcommand() {
        let node = sub_m.get_one::<String>("node").unwrap();
        println!("Viewing graph for node: {}", node);
        // Call the function to view the current state of the graph
    } else if let Some(("view-graph-history", sub_m)) = matches.subcommand() {
        let node = sub_m.get_one::<String>("node").unwrap();
        
        // Bind default values to variables before using them in unwrap_or
        let default_start = "2023-01-01".to_string();
        let default_end = "2023-12-31".to_string();
        
        let start = sub_m.get_one::<String>("start").unwrap_or(&default_start);
        let end = sub_m.get_one::<String>("end").unwrap_or(&default_end);

        println!(
            "Viewing graph history for node: {} from {} to {}",
            node, start, end
        );
        // Call the function to view the graph history
    } else if let Some(("index-node", sub_m)) = matches.subcommand() {
        let node = sub_m.get_one::<String>("node").unwrap();
        let data = sub_m.get_one::<String>("data").unwrap();
        println!("Indexing node: {} with data: {}", node, data);

        // Call the indexing function
        index_node(node, data);
    } else if let Some(("cache-node-state", sub_m)) = matches.subcommand() {
        let node = sub_m.get_one::<String>("node").unwrap();
        let state = sub_m.get_one::<String>("state").unwrap();
        println!("Caching state for node: {} with state: {}", node, state);

        // Call the caching function
        cache_node_state(node, state);
    } else {
        // No subcommand provided, call the default start_cli function
        start_cli();
    }
}

