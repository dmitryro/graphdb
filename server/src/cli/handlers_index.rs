// server/src/cli/handlers_index.rs
use anyhow::Result;
use lib::commands::IndexAction;
use std::sync::Arc;
use indexing_service::IndexingService;
use crate::cli::handlers_utils::get_current_storage_port;
use tokio::sync::OnceCell;
use log::{info, error, warn, debug};

static INDEXING_ENGINE: OnceCell<Arc<IndexingService>> = OnceCell::const_new();

pub async fn get_indexing_engine() -> Arc<IndexingService> {
    INDEXING_ENGINE.get_or_init(|| async {
        let port = get_current_storage_port().await;
        info!("Initializing IndexingService with daemon on port {}", port);
        Arc::new(IndexingService::new(port))
    }).await.clone()
}

pub async fn handle_index_command(action: IndexAction) -> Result<()> {
    let engine = get_indexing_engine().await;

    match action {
        IndexAction::Create { label, property } => {
            let res = engine.create_index(&label, &property).await?;
            println!("Index created on {label}.{property}\n{res:#}");
        }
        IndexAction::Drop { label, property } => {
            let res = engine.drop_index(&label, &property).await?;
            println!("Index dropped {label}.{property}\n{res:#}");
        }
        IndexAction::List => {
            let res = engine.list_indexes().await?;
            println!("All indexes:\n{res:#}");
        }
        IndexAction::Search { term, top } => {
            let limit = top.unwrap_or(10);
            let res = engine.fulltext_search(&term, limit).await?;
            println!("Search results for '{term}' (top {limit}):\n{res:#}");
        }
        IndexAction::Rebuild => {
            let res = engine.rebuild_indexes().await?;
            println!("Full-text indexes refreshed\n{res:#}");
        }
        IndexAction::Stats => {
            let res = engine.list_indexes().await?;
            println!("Index statistics:\n{res:#}");
        }
    }
    Ok(())
}