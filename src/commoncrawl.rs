//! This module contains helper functions and structs for de-serializing CommonCrawl-specific data structures.
use std::io::{Read, Write};
use std::fs::{File};
use anyhow::Context;
use autometrics::autometrics;
use serde::{Deserialize, Serialize};
use serde_aux::prelude::deserialize_number_from_string;
use tracing::info;

/// Metadata for a crawled URL.
/// We use this metadata in the batcher to filter URLs before passing them on to the worker(s).
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CdxMetadata {
    pub url: String,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub status: usize,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub length: usize,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub offset: usize,
    pub filename: String,
    pub languages: Option<String>,
}

pub async fn download_and_store(url: &str, path: &str) -> anyhow::Result<()> {
    let response = reqwest::get(url).await?;
    
    if response.status().is_success() {
        info!("File {} downloaded successfully", url);

        _ = create_path(path);
        let mut file = File::create(path)?;
        let content = response.bytes().await?;
        file.write_all(&content).with_context(|| "Something went wrong storing file")?;
        Ok(())
    } else {
        Err(anyhow::anyhow!("Failed to download and store file from {}", url))
    }
}

fn create_path(path: &str) -> anyhow::Result<()>{
    let path = std::path::Path::new(path);
    let prefix = path.parent().unwrap();
    std::fs::create_dir_all(prefix)?;
    Ok(())
}

/// Downloads a given byte range from a URL and unzips the resulting data into a byte Vec.
/// Does not interpret the output as UTF-8 because the `warc` crate wants plain bytes.
#[autometrics]
pub async fn download_and_unzip(
    url: &str,
    offset: usize,
    length: usize,
) -> Result<Vec<u8>, anyhow::Error> {
    let client = reqwest::Client::new();
    let res = client
        .get(url)
        .header("Range", format!("bytes={}-{}", offset, offset + length - 1))
        .send()
        .await?;
    match res.status() {
        reqwest::StatusCode::PARTIAL_CONTENT => {
            let body = res.bytes().await?;
            tracing::trace!(
                "Successfully fetched the URL {} from {} to {}",
                url,
                offset,
                offset + length - 1
            );
            let mut decoder = flate2::read::GzDecoder::new(&body[..]);
            let mut buffer = Vec::new();
            decoder.read_to_end(&mut buffer)?;
            Ok(buffer)
        }
        _ => Err(anyhow::anyhow!(
            "Failed to fetch index file {}: {}",
            url,
            res.status()
        )),
    }
}

/// Represents a line in a cdx index file.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CdxFileContext {
    pub filename: String,
    pub content: String,
    pub target_uri: String,
    pub tokens: Vec<String>
}

/// Represents a line in a cdx index file.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CdxEntry {
    pub surt_url: String,
    pub timestamp: String,
    pub metadata: CdxMetadata,
}

/// Deserialize an index file fow into a [CdxEntry].
/// Panics if parsing fails.
pub fn parse_cdx_line(line: &str) -> CdxEntry {
    let mut parts = line.splitn(3, ' ');
    CdxEntry {
        surt_url: parts.next().unwrap().to_string(),
        timestamp: parts.next().unwrap().to_string(),
        metadata: serde_json::from_str(parts.next().unwrap()).unwrap(),
    }
}

/// Represents a line in a cluster.idx file.
/// We only care about the cdx filename and offset/length pair into that file.
pub struct ClusterIdxEntry {
    _surt_url: String,
    _timestamp: String,
    pub cdx_filename: String,
    pub cdx_offset: usize,
    pub cdx_length: usize,
    _cluster_id: String,
}

/// De-serializes a cluster.idx file line into a [ClusterIdxEntry].
/// Returns none if there are missing elements in the line.
/// Panics if parsing fails.
pub fn parse_cluster_idx(line: &str) -> Option<ClusterIdxEntry> {
    let mut idx = line.split_whitespace();
    Some(ClusterIdxEntry {
        _surt_url: idx.next()?.to_string(),
        _timestamp: idx.next()?.to_string(),
        cdx_filename: idx.next()?.to_string(),
        cdx_offset: idx.next()?.parse().unwrap(),
        cdx_length: idx.next()?.parse().unwrap(),
        _cluster_id: idx.next()?.to_string(),
    })
}
