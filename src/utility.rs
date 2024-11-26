pub fn remove_scheme_trailing_slash_and_ports(url: &str) -> String {
    let url = if let Some(rest) = url.strip_prefix("http://") {
        rest
    } else if let Some(rest) = url.strip_prefix("https://") {
        rest
    } else {
        url
    };

    let url = if let Some(rest) = url.strip_suffix("/") {
        rest
    } else {
        url
    };

    str::replace(url, ":", "_")
}