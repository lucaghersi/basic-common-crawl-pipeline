//! The batcher only operates on index files that contain metadata about the URLs that are part of the crawl.
//! It does not have to download the actual content of the URLs and therefore it does not have to deal with WARC files.
//!
//! For a given crawl, there are hundreds of index files, each containing roughly a gigabyte of URL metadata.
//! Every line in the index file contains the following information. Notice that I have split the line into multiple lines for readability:
//!
//! ```json
//! 0,100,22,165)/
//! 20240722120756
//! {
//!     "url": "http://165.22.100.0/",
//!     "mime": "text/html",
//!     "mime-detected": "text/html",
//!     "status": "301",
//!     "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R",
//!     "length": "689",
//!     "offset": "3499",
//!     "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz",
//!     "redirect": "https://157.245.55.71/"
//! }
//! ```
//!
//! The first lines contains the URL in SURT (Sort-friendly URI Reordering Transform) format, the second lines contains the crawl timestamp, and the remaining lines contain JSON metadata.
//!
//! The URLs in the index files are sorted alpha-numerically.
//!
//! Once the batcher has downloaded (parts of) an index file, it will filter out URLs that are not in English or that did not return a 200 HTTP status code, batch them into groups whose size has a constant upper limit and push the messages containing these URls into a RabbitMQ queue.

use anyhow::{Context, Result};
use clap::Parser;
use pipeline::commoncrawl::{download_and_store, CdxEntry, ClusterIdxEntry};
use pipeline::{
    commoncrawl::{download_and_unzip, parse_cdx_line, parse_cluster_idx},
    rabbitmq::{
        publish_batch, rabbitmq_channel_with_queue, rabbitmq_connection, BATCH_SIZE, CC_QUEUE_NAME_BATCHES,
    },
    tracing_and_metrics::{run_metrics_server, setup_tracing},
};
use std::fs;
use autometrics::autometrics;
use metrics::{counter, increment_counter};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// For an explanation for why this file needs to be provided, please
    /// see Readme.md, section "Why do we download the cluster.idx file up front?".
    #[arg(short('i'), long("index"), default_value = "cluster.idx")]
    cluster_idx_filename: String,

    /// The dataset to use; the index file should point to the same dataset or it will not work
    #[arg(short('d'), long("dataset"), default_value = "CC-MAIN-2024-30")]
    dataset: String,

    /// This command line argument can be used to limit the number of chunks that should be processed.
    /// If set, the batcher only processes so many lines from the provided cluster.idx file.
    /// Otherwise, it processes all entries in the file.
    #[arg(short('c'), long("chunks"), default_value_t = 1000, 
    value_parser = clap::value_parser!(u64).range(1..=1000))]
    num_cdx_chunks_to_process: u64,
}

#[tokio::main]
async fn main() {

    setup_tracing();
    tokio::task::spawn(run_metrics_server(9000));
    
    let run_result = run(Args::parse()).await;
    if let Err(e) = run_result {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<()> {
    
    let rabbit_conn = rabbitmq_connection()
        .await
        .with_context(|| "Looks like rabbit is not available.")?;
    let (channel, _queue) = rabbitmq_channel_with_queue(&rabbit_conn, CC_QUEUE_NAME_BATCHES).await?;

    // build index structure for further processing
    let idx = obtain_index(&args.cluster_idx_filename, &args.dataset).await?;

    // process index
    process_index(
        &idx,
        &args.dataset,
        args.num_cdx_chunks_to_process as usize,
        &channel,
    )
    .await?;

    Ok(())
}

async fn obtain_index(index_file_name: &str, dataset_name: &str) -> Result<Vec<ClusterIdxEntry>> {
    let index_file_path = format!("./data/{}", index_file_name);

    // download file if not exists
    if !fs::exists(&index_file_path).unwrap_or(false) {
        tracing::info!("Index file missing in ./data folder. Downloading...");
        let index_file_url = format!(
            "https://data.commoncrawl.org/cc-index/collections/{}/indexes/{}",
            dataset_name, index_file_name
        );

        download_and_store(&index_file_url, &index_file_path).await?;
    }

    let idx = fs::read_to_string(&index_file_path)
        .with_context(|| format!("Failed to read idx file from {}", index_file_path))?
        .lines()
        .filter_map(parse_cluster_idx)
        .collect::<Vec<_>>();

    tracing::info!("{} index lines prepared for processing", idx.len());

    Ok(idx)
}

#[autometrics]
async fn process_index(
    idx: &Vec<ClusterIdxEntry>,
    dataset: &str,
    max_chunks_to_process: usize,
    channel: &lapin::Channel,
) -> Result<()> {
    let mut num_cdx_chunks_processed = 0usize;
    for cdx_chunk in idx {

        let url = &format!(
            "https://data.commoncrawl.org/cc-index/collections/{}/indexes/{}",
            dataset, cdx_chunk.cdx_filename
        );
        
        let content = download_and_unzip(url, cdx_chunk.cdx_offset, cdx_chunk.cdx_length, ).await?;
        
        let english_cdx_entries = String::from_utf8(content)?
        .lines()
        .map(parse_cdx_line)
        .filter(select_only_english_cdx_entries)
        .collect::<Vec<_>>();

        for batch in english_cdx_entries.as_slice().chunks(BATCH_SIZE) {
            counter!("index_chunks_processed", batch.len() as u64);
            publish_batch(channel, CC_QUEUE_NAME_BATCHES, batch).await;
        }
        
        num_cdx_chunks_processed += 1;

        if max_chunks_to_process == num_cdx_chunks_processed {
            break;
        }
    }

    Ok(())
}

fn select_only_english_cdx_entries(e: &CdxEntry) -> bool {
    if let Some(languages) = e.metadata.languages.as_ref() {
        increment_counter!("batcher_cdx_entry_selected");
        languages.contains("eng") && e.metadata.status == 200
    } else {
        false
    }
}
