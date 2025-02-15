use clap::{Arg, Command, Subcommand};
use graph_database::cli_engine::cli_engine::start_cli;

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
    } else {
        // No subcommand provided, call the default start_cli function
        start_cli();
    }
}

