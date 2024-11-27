//! The worker(s) pull(s) messages from the RabbitMQ queue and downloads the WARC files that contain the actual content of the URLs.
//! Once the content has been downloaded, the worker extracts the text from the HTML file using the trafilatura Python package.
//!
//! After having downloaded and extracted the text from the HTML file, the worker could apply some filters to the extracted text.
//! We would also want to tokenize (for LLM training) the text and output it to a file.
//!
//! In its current implementation it does not refine or filter the extracted text in any way nor does it output the extracted text to a file.

use futures_util::StreamExt;
use lapin::options::BasicAckOptions;
use pipeline::{
    rabbitmq::{
        rabbitmq_channel_with_queue, rabbitmq_connection, rabbitmq_consumer,
    },
    tracing_and_metrics::{run_metrics_server, setup_tracing}
};
use anyhow::{Context, Result};
use clap::Parser;
use metrics::increment_counter;
use minio::s3::args::{BucketExistsArgs, MakeBucketArgs, PutObjectArgs};
use minio::s3::client::ClientBuilder;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use pipeline::commoncrawl::CdxFileContext;
use pipeline::rabbitmq::CC_QUEUE_NAME_STORE;
use pipeline::utility::calculate_hash;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The address of a compatible s3 server to use (minio)
    #[arg(short('s'), long("store-server"))]
    s3_server: String,
    /// Add a s3-compatible bucket address to store files after processing
    #[arg(short('b'), long("bucket"))]
    s3_bucket: String,
    /// The s3 bucket user
    #[arg(short('u'), long("user"))]
    s3_bucket_user: String,
    /// The s3 bucket password
    #[arg(short('p'), long("password"))]
    s3_bucket_password: String,
}

#[tokio::main]
async fn main() {
    setup_tracing();
    tokio::task::spawn(run_metrics_server(9002));

    let run_result = run("fs", Args::parse()).await;
    if let Err(e) = run_result {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
async fn run(file_processor_name: &str, args: Args) -> Result<()> {
    
    let rabbit_conn = rabbitmq_connection().await?;
    let (channel, _queue) = rabbitmq_channel_with_queue(&rabbit_conn, CC_QUEUE_NAME_STORE).await?;
    let mut consumer = rabbitmq_consumer(&channel, CC_QUEUE_NAME_STORE, file_processor_name).await?;
    
    let base_url = args.s3_server.parse::<BaseUrl>()?;
    tracing::info!("Trying to connect to MinIO at: `{:?}`", base_url);
    
    let static_provider = StaticProvider::new(&args.s3_bucket_user, &args.s3_bucket_password, None);

    let client = ClientBuilder::new(base_url.clone())
        .provider(Some(Box::new(static_provider)))
        .build()
        .with_context(|| format!("Connection to MinIO at url {} failed", args.s3_server))?;
    tracing::info!("Connection to MinIO at url {} successful", args.s3_server);

    // Check 's3_bucket' bucket exist or not.
    let exists: bool = client
        .bucket_exists(&BucketExistsArgs::new(&args.s3_bucket)?)
        .await?;

    // Make 's3_bucket' bucket if not exist.
    if !exists {
        client.make_bucket(&MakeBucketArgs::new(&args.s3_bucket)?).await?;
    }
    
    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(delivery) => {
                
                let batch = serde_json::from_slice::<Vec<CdxFileContext>>(&delivery.data)?;
                
                // here we expect a single entry
                for entry in batch {
                    let file_name_hash = calculate_hash(&entry.filename);
                    let file_name = format!("{}/{}", &entry.filename, file_name_hash);
                    
                    tracing::info!("File content for uri {} received and ready for storage", file_name);

                    let mut bytes = entry.content.as_bytes();
                    let read: &mut dyn std::io::Read = &mut bytes;
                    let object_size = Some(entry.content.as_bytes().len());
                    
                    client.put_object(&mut PutObjectArgs::new(&args.s3_bucket,
                                                              &file_name, read, object_size, None).unwrap()).await?;

                    tracing::info!("File `{}` uploaded successfully as object to bucket `{}`.", file_name, &args.s3_bucket);
                    increment_counter!("saver_file_uploaded");
                }
                
                delivery.ack(BasicAckOptions::default()).await?;
            }
            Err(e) => {
                tracing::warn!(err.msg = %e, err.details = ?e, "File processor failed to receive message from RabbitMQ. Reconnecting.");
                continue;
            }
        }
    }

    Ok(())
}