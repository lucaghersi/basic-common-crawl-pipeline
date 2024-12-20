//! The worker(s) pull(s) messages from the RabbitMQ queue and downloads the WARC files that contain the actual content of the URLs.
//! Once the content has been downloaded, the worker extracts the text from the HTML file using the trafilatura Python package.
//!
//! After having downloaded and extracted the text from the HTML file, the worker could apply some filters to the extracted text.
//! We would also want to tokenize (for LLM training) the text and output it to a file.
//!
//! In its current implementation it does not refine or filter the extracted text in any way nor does it output the extracted text to a file.

use anyhow::Result;
use autometrics::autometrics;
use futures_util::StreamExt;
use lapin::options::BasicAckOptions;
use metrics::{counter, increment_counter};
use tokenizers::Tokenizer;
use pipeline::commoncrawl::CdxFileContext;
use pipeline::rabbitmq::{publish, CC_QUEUE_NAME_STORE};
use pipeline::{
    commoncrawl::{download_and_unzip, CdxEntry},
    rabbitmq::{
        rabbitmq_channel_with_queue, rabbitmq_connection, rabbitmq_consumer, CC_QUEUE_NAME_BATCHES,
    },
    tracing_and_metrics::{run_metrics_server, setup_tracing},
    trafilatura,
};
use warc::WarcHeader;

#[tokio::main]
async fn main() {
    setup_tracing();
    tokio::task::spawn(run_metrics_server(9000));

    let run_result = run("worker").await;
    if let Err(e) = run_result {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

async fn run(worker_name: &str) -> Result<()> {
    let rabbit_conn = rabbitmq_connection().await?;
    let (channel, _queue) =
        rabbitmq_channel_with_queue(&rabbit_conn, CC_QUEUE_NAME_BATCHES).await?;
    let (files_channel, _queue) =
        rabbitmq_channel_with_queue(&rabbit_conn, CC_QUEUE_NAME_STORE).await?;
    let mut consumer = rabbitmq_consumer(&channel, CC_QUEUE_NAME_BATCHES, worker_name).await?;
    let tokenizer = Tokenizer::from_pretrained("bert-base-cased", None).unwrap();

    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(delivery) => {
                let batch = serde_json::from_slice::<Vec<CdxEntry>>(&delivery.data)?;
                let batch_len =  batch.len();
                
                tracing::info!(
                    "{} - Received a batch of {} entries",
                    worker_name,
                    batch_len
                );
                
                counter!("worker_received_batch_total", batch_len as u64);
                increment_counter!("worker_received_batch_count");

                for entry in batch {
                    process_index_entry(entry, &files_channel, &tokenizer).await?
                }

                delivery.ack(BasicAckOptions::default()).await?;
            }
            Err(e) => {
                tracing::warn!(err.msg = %e, err.details = ?e, "Worker failed to receive message from RabbitMQ. Reconnecting.");
                continue;
            }
        }
    }

    Ok(())
}

#[autometrics]
async fn process_index_entry(entry: CdxEntry, channel: &lapin::Channel, tokenizer: &Tokenizer) -> Result<()> {
    let url = &format!("https://data.commoncrawl.org/{}", entry.metadata.filename);
    let data = download_and_unzip(url, entry.metadata.offset, entry.metadata.length).await?;
    counter!("worker_downloaded_data", data.len() as u64);

    for warc_entry in warc::WarcReader::new(data.as_slice()).iter_records() {
        let warc_entry = warc_entry?;

        if warc_entry.header(WarcHeader::WarcType).unwrap() != "response" {
            continue;
        }

        let target_uri = warc_entry.header(WarcHeader::TargetURI).unwrap();
        tracing::info!("Successfully read WARC entry with URL {}", target_uri);

        let raw_content = String::from_utf8_lossy(warc_entry.body());
        extract_and_process_content(&entry, &raw_content, channel, &target_uri, tokenizer).await?
    }

    Ok(())
}

async fn extract_and_process_content(
    entry: &CdxEntry,
    raw_content: &str,
    channel: &lapin::Channel,
    target_uri: &str,
    tokenizer: &Tokenizer
) -> Result<()> {
    let html_begin_index = raw_content.find("\n\n");
    let Some(html_begin_index) = html_begin_index else {
        // we ignore content that is not valid HTML
        tracing::debug!("Failed to find HTML content in WARC entry");
        return Ok(());
    };

    tracing::debug!(
        "First 1000 characters of raw content: {}",
        &raw_content[..1000]
    );
    increment_counter!("worker_doc_processed");

    let content = trafilatura::extract(&raw_content[html_begin_index..])?;

    if let Some(content) = content {
        let len = content.len();

        tracing::debug!("Extracted content: {}", &content);
        
        if !(500..=1000000).contains(&len) {
            tracing::debug!("Extracted content of length {}, which is outside the allowed range", len);
            return Ok(());
        }
        else {
            tracing::info!("Content length is {}; content will be transmitted for further processing", len);
        }

        // tokenize
        let tokens = tokenize(&content, tokenizer).unwrap_or(Vec::new());
        let file_content_to_save = CdxFileContext {
            content: content,
            filename: entry.metadata.filename.clone(),
            target_uri: target_uri.to_string(),
            tokens: tokens
        };
        publish(channel, CC_QUEUE_NAME_STORE, &file_content_to_save).await?;
    } else {
        tracing::warn!("Failed to extract content from WARC entry");
    }

    Ok(())
}

fn tokenize(content: &str, tokenizer: &Tokenizer) -> Result<Vec<String>> {
    let encoding = tokenizer.encode(content, false).unwrap();
    let result = encoding.get_tokens();
    Ok(result.to_vec())
}