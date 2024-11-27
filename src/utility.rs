use anyhow::Context;
use metrics::increment_counter;
use minio::s3::args::PutObjectArgs;
use minio::s3::client::Client;
use minio::s3::utils::Multimap;
use sha2::{Sha256, Digest};
use crate::commoncrawl::CdxFileContext;

pub fn calculate_hash(to_be_hashed: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(to_be_hashed);
    format!("{:X}", hasher.finalize())
}

pub async fn upload_file_to_minio(client: &Client, entry: &CdxFileContext, s3_bucket: &str) -> anyhow::Result<()> {
    let file_name_hash = calculate_hash(&entry.filename);
    let file_name = format!("{}/{}.json", &entry.filename, file_name_hash);

    tracing::info!(
        "File content for uri {} received and ready for storage",
        file_name
    );
    
    let bytes = &serde_json::to_vec(&entry)?;
    let read: &mut dyn std::io::Read = &mut bytes.as_slice();
    let object_size = Some(bytes.len());

    // prepare file loading
    let put_args = &mut PutObjectArgs::new(&s3_bucket, &file_name, read, object_size, None)?;
    // adding original url as metadata
    let mut map = Multimap::new();
    map.insert("x-original-url".to_string(), entry.target_uri.to_string());
    put_args.user_metadata = Some(&map);

    client.put_object(put_args).await.with_context(|| {
        format!(
            "Something went wrong uploading file {} to MinIO",
            entry.target_uri
        )
    })?;

    tracing::info!(
        "File `{}` uploaded successfully as object to bucket `{}`.",
        file_name,
        &s3_bucket
    );
    increment_counter!("saver_file_uploaded");

    Ok(())
}
