#[cfg(test)]
mod utility_tests {
    use pipeline::utility::{calculate_hash};

    #[test]
    fn test_calculate_hash_empty_string() {
        let result = calculate_hash("");
        let expected = "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855";
        assert_eq!(result, expected, "Hash of an empty string should be the SHA256 hash for an empty input.");
    }

    #[test]
    fn test_calculate_hash_hello_world() {
        let result = calculate_hash("Hello, world!");
        let expected = "315F5BDB76D078C43B8AC0064E4A0164612B1FCE77C869345BFC94C75894EDD3";
        assert_eq!(result, expected, "The hash for 'Hello, world!' is incorrect.");
    }

    #[test]
    fn test_calculate_hash_special_characters() {
        let result = calculate_hash("!@#$%^&*()_+");
        let expected = "36D3E1BC65F8B67935AE60F542ABEF3E55C5BBBD547854966400CC4F022566CB";
        assert_eq!(result, expected, "The hash for '!@#$%^&*()_+' is incorrect.");
    }

    #[test]
    fn test_calculate_hash_numerics() {
        let result = calculate_hash("1234567890");
        let expected = "C775E7B757EDE630CD0AA1113BD102661AB38829CA52A6422AB782862F268646";
        assert_eq!(result, expected, "The hash for '1234567890' is incorrect.");
    }

    #[test]
    fn test_calculate_hash_unicode() {
        let result = calculate_hash("你好，世界！");
        let expected = "FA65D94B3532D83FD24ADA92DADECFC7AE5370E6DBF762133027A89C2E7202F1";
        assert_eq!(result, expected, "The hash for '你好，世界！' is incorrect.");
    }
}