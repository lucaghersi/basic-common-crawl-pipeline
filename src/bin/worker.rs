//! The worker(s) pull(s) messages from the RabbitMQ queue and downloads the WARC files that contain the actual content of the URLs.
//! Once the content has been downloaded, the worker extracts the text from the HTML file using the trafilatura Python package.
//!
//! After having downloaded and extracted the text from the HTML file, the worker could apply some filters to the extracted text.
//! We would also want to tokenize (for LLM training) the text and output it to a file.
//!
//! In its current implementation it does not refine or filter the extracted text in any way nor does it output the extracted text to a file.

use autometrics::autometrics;
use futures_util::StreamExt;
use lapin::options::BasicAckOptions;
use pipeline::{
    commoncrawl::{download_and_unzip, CdxEntry},
    rabbitmq::{
        rabbitmq_channel_with_queue, rabbitmq_connection, rabbitmq_consumer, CC_QUEUE_NAME_BATCHES,
    },
    tracing_and_metrics::{run_metrics_server, setup_tracing},
    trafilatura,
};
use warc::WarcHeader;
use anyhow::Result;
use metrics::increment_counter;
use pipeline::commoncrawl::CdxFileContext;
use pipeline::rabbitmq::{publish_content, CC_QUEUE_NAME_STORE};

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
    let (channel, _queue) = rabbitmq_channel_with_queue(&rabbit_conn, CC_QUEUE_NAME_BATCHES).await?;
    let (files_channel, _queue) = rabbitmq_channel_with_queue(&rabbit_conn, CC_QUEUE_NAME_STORE).await?;
    let mut consumer = rabbitmq_consumer(&channel, CC_QUEUE_NAME_BATCHES, worker_name).await?;

    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(delivery) => {
                let batch = serde_json::from_slice::<Vec<CdxEntry>>(&delivery.data);
                tracing::info!("{} - Received a batch of {} entries", worker_name,
                    batch.as_ref().unwrap().len());

                for entry in batch? {
                    process_index_entry(entry, &files_channel).await?
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
async fn process_index_entry(entry: CdxEntry, channel: &lapin::Channel) -> Result<()> {

    let url = &format!("https://data.commoncrawl.org/{}", entry.metadata.filename);
    let data = download_and_unzip(url, entry.metadata.offset, entry.metadata.length, ).await?;

    for warc_entry in warc::WarcReader::new(data.as_slice()).iter_records() {
        let warc_entry = warc_entry?;

        if warc_entry.header(WarcHeader::WarcType).unwrap() != "response" {
            continue;
        }

        let target_uri =warc_entry.header(WarcHeader::TargetURI).unwrap();
        tracing::info!("Successfully read WARC entry with URL {}", target_uri);

        let raw_content = String::from_utf8_lossy(warc_entry.body());
        extract_and_process_content(&entry, &raw_content, channel, &target_uri).await?
    }

    Ok(())
}

async fn extract_and_process_content(entry: &CdxEntry, raw_content: &str, channel: &lapin::Channel, target_uri: &str) -> Result<()> {

    let html_begin_index = raw_content.find("\n\n");
    let Some(html_begin_index) = html_begin_index else {
        // we ignore content that is not valid HTML
        tracing::debug!("Failed to find HTML content in WARC entry");
        return Ok(())
    };

    tracing::debug!("First 1000 characters of raw content: {}", &raw_content[..1000]);
    increment_counter!("worker_doc_processed");

    let content = trafilatura::extract(&raw_content[html_begin_index..])?;

    if let Some(content) = content {
        tracing::info!("Extracted content of length {}", content.len());
        tracing::debug!("Extracted content: {}", &content);

        let file_content_to_save = CdxFileContext {
            content: content,
            filename: entry.metadata.filename.clone(),
            target_uri: target_uri.to_string()
        };
        let batch = [file_content_to_save];

        publish_content(channel, CC_QUEUE_NAME_STORE, &batch).await;
    } else {
        tracing::warn!("Failed to extract content from WARC entry");
    }

    Ok(())
}