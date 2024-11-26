#[cfg(test)]
mod commoncrawl_tests {
    use std::{fs};
    use mockito::{Server};
    use tempfile::tempdir;
    use pipeline::commoncrawl::download_and_store;

    #[tokio::test]
    async fn test_download_and_store_success() {
        
        let dir = tempdir().unwrap();
        let path = dir.path().join("file.txt");
        let path_str = path.to_str().unwrap();
        
        let expected_content = "index content";

        let mut server = Server::new_async().await;
        let mock = server.mock("GET", "/index")
            .with_status(200)
            .with_header("content-type", "text/plain")
            .with_body(expected_content)
            .create();
        
        let server_url = server.url();
        let url = format!("{server_url}/index");
        
        let result = download_and_store(&url, path_str).await;
        
        assert!(result.is_ok());
        mock.assert_async().await;
        
        let content = fs::read_to_string(path_str).unwrap();
        assert_eq!(content, expected_content);
    }

     #[tokio::test]
     async fn test_download_and_store_url_failure() {
         let mut server = Server::new_async().await;
         let mock = server.mock("GET", "/index")
             .with_status(200)
             .with_header("content-type", "text/plain")
             .create();

         let server_url = server.url();
         let url = format!("{server_url}/wrong-index");

         let result = download_and_store(&url, "").await;

         assert!(result.is_err());
         mock.expect(0).assert_async().await;
     }
}