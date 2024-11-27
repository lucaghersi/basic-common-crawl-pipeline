use sha2::{Sha256, Digest};

pub fn calculate_hash(to_be_hashed: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(to_be_hashed);
    format!("{:X}", hasher.finalize())
}

