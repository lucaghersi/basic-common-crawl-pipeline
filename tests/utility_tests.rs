#[cfg(test)]
mod tests {
    use pipeline::utility::remove_scheme_trailing_slash_and_ports;
    use super::*;

    #[test]
    fn test_remove_scheme() {
        assert_eq!(remove_scheme_trailing_slash_and_ports("http://example.com"), "example.com");
        assert_eq!(remove_scheme_trailing_slash_and_ports("https://example.com"), "example.com");
    }

    #[test]
    fn test_remove_trailing_slash() {
        assert_eq!(remove_scheme_trailing_slash_and_ports("example.com/"), "example.com");
        assert_eq!(remove_scheme_trailing_slash_and_ports("http://example.com/"), "example.com");
        assert_eq!(remove_scheme_trailing_slash_and_ports("https://example.com/"), "example.com");
    }

    #[test]
    fn test_replace_ports() {
        assert_eq!(remove_scheme_trailing_slash_and_ports("example.com:8080"), "example.com_8080");
        assert_eq!(remove_scheme_trailing_slash_and_ports("http://example.com:8080"), "example.com_8080");
    }

    #[test]
    fn test_no_scheme_or_slash_or_ports() {
        assert_eq!(remove_scheme_trailing_slash_and_ports("example.com"), "example.com");
    }

    #[test]
    fn test_empty_string() {
        assert_eq!(remove_scheme_trailing_slash_and_ports(""), "");
    }

    #[test]
    fn test_scheme_only() {
        assert_eq!(remove_scheme_trailing_slash_and_ports("http://"), "");
        assert_eq!(remove_scheme_trailing_slash_and_ports("https://"), "");
    }

    #[test]
    fn test_slash_only() {
        assert_eq!(remove_scheme_trailing_slash_and_ports("/"), "");
    }

    #[test]
    fn test_scheme_slash_and_ports() {
        assert_eq!(remove_scheme_trailing_slash_and_ports("http://example.com:8080/"), "example.com_8080");
    }

    #[test]
    fn test_multiple_colons() {
        assert_eq!(remove_scheme_trailing_slash_and_ports("example.com:8080:9090"), "example.com_8080_9090");
    }
}