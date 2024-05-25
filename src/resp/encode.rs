use super::{
    RespArray, RespArrayNull, RespBulkString, RespBulkStringNull, RespDouble, RespEncoder, RespMap,
    RespNull, RespSet, RespSimpleError, RespSimpleString,
};

const CAPACITY: usize = 4096;

// Simple string format "+<str>\r\n"
impl RespEncoder for RespSimpleString {
    fn encode(self) -> Vec<u8> {
        format!("+{}\r\n", self.0).into_bytes()
    }
}

// Simple error format "-<str>\r\n"
impl RespEncoder for RespSimpleError {
    fn encode(self) -> Vec<u8> {
        format!("-{}\r\n", self.0).into_bytes()
    }
}

// integer format ":[<+|->]<value>\r\n"
impl RespEncoder for i64 {
    fn encode(self) -> Vec<u8> {
        format!(":{}\r\n", self).into_bytes()
    }
}

// Bulk string format "$<length>\r\n<data>\r\n"
impl RespEncoder for RespBulkString {
    fn encode(self) -> Vec<u8> {
        let length = self.len();
        let mut buf: Vec<u8> = Vec::with_capacity(length + 10);
        buf.extend(format!("${}\r\n", length).into_bytes());
        buf.extend(self.0);
        buf.extend(b"\r\n");
        buf
    }
}

// Null bulk string format "$-1\r\n"
impl RespEncoder for RespBulkStringNull {
    fn encode(self) -> Vec<u8> {
        b"$-1\r\n".to_vec()
    }
}

// Arrays format "*<number-of-elements>\r\n<element-1>...<element-n>"
impl RespEncoder for RespArray {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(CAPACITY);
        buf.extend(format!("*{}\r\n", self.len()).into_bytes());
        for frame in self.0 {
            buf.extend(frame.encode());
        }
        buf
    }
}

// Null array format "*-1\r\n"
impl RespEncoder for RespArrayNull {
    fn encode(self) -> Vec<u8> {
        b"*-1\r\n".to_vec()
    }
}

// Null format "_\r\n"
impl RespEncoder for RespNull {
    fn encode(self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

// Boolean format "#<t|f>\r\n"
impl RespEncoder for bool {
    fn encode(self) -> Vec<u8> {
        format!("#{}\r\n", if self { "t" } else { "f" }).into_bytes()
    }
}

// Double format ",[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n"
impl RespEncoder for RespDouble {
    fn encode(self) -> Vec<u8> {
        if self.is_nan() {
            return b",nan\r\n".to_vec();
        }
        if self.is_infinite() {
            return if self.is_sign_negative() {
                b",-inf\r\n".to_vec()
            } else {
                b",inf\r\n".to_vec()
            };
        }
        format!(",{}\r\n", self).into_bytes()
    }
}

// Map format "%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>"
impl RespEncoder for RespMap {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(CAPACITY);
        buf.extend(format!("%{}\r\n", self.len()).into_bytes());
        for (key, value) in self.0 {
            buf.extend(key.encode());
            buf.extend(value.encode());
        }
        buf
    }
}

// Set format "~<number-of-elements>\r\n<element-1>...<element-n>"
impl RespEncoder for RespSet {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(CAPACITY);
        buf.extend(format!("~{}\r\n", self.len()).into_bytes());
        for frame in self.0 {
            buf.extend(frame.encode());
        }
        buf
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use crate::RespFrame;

    use super::*;

    #[test]
    fn test_simple_string_encode() {
        let simple_string: RespFrame = RespSimpleString::new("OK").into();
        assert_eq!(simple_string.encode(), b"+OK\r\n");
    }

    #[test]
    fn test_simple_error_encode() {
        let simple_error: RespFrame = RespSimpleError::new("ERR unknown command 'asdf'").into();
        assert_eq!(simple_error.encode(), b"-ERR unknown command 'asdf'\r\n");
    }

    #[test]
    fn test_integer_encode() {
        let integer: RespFrame = 123.into();
        assert_eq!(integer.encode(), b":123\r\n");

        let integer_negative: RespFrame = (-123).into();
        assert_eq!(integer_negative.encode(), b":-123\r\n");
    }

    #[test]
    fn test_bulk_string_encode() {
        let bulk_string: RespFrame = RespBulkString::new("foobar").into();
        assert_eq!(bulk_string.encode(), b"$6\r\nfoobar\r\n");

        let bulk_string_empty: RespFrame = RespBulkString::new("").into();
        assert_eq!(bulk_string_empty.encode(), b"$0\r\n\r\n");
    }

    #[test]
    fn test_bulk_string_null_encode() {
        let bulk_string_null: RespFrame = RespBulkStringNull.into();
        assert_eq!(bulk_string_null.encode(), b"$-1\r\n");
    }

    #[test]
    fn test_array_encode() {
        let array: RespFrame = RespArray::new(vec![
            RespSimpleString::new("foo").into(),
            RespSimpleString::new("bar").into(),
            RespBulkString::new("foobar").into(),
            RespArray::new(vec![64.into()]).into(),
        ])
        .into();
        assert_eq!(
            array.encode(),
            b"*4\r\n+foo\r\n+bar\r\n$6\r\nfoobar\r\n*1\r\n:64\r\n"
        );
    }

    #[test]
    fn test_array_null_encode() {
        let array_null: RespFrame = RespArrayNull.into();
        assert_eq!(array_null.encode(), b"*-1\r\n");
    }

    #[test]
    fn test_null_encode() {
        let null: RespFrame = RespNull.into();
        assert_eq!(null.encode(), b"_\r\n");
    }

    #[test]
    fn test_boolean_encode() {
        let boolean_true: RespFrame = true.into();
        assert_eq!(boolean_true.encode(), b"#t\r\n");

        let boolean_false: RespFrame = false.into();
        assert_eq!(boolean_false.encode(), b"#f\r\n");
    }

    #[test]
    fn test_double_encode() {
        let double: RespFrame = RespDouble::new(5.14).into();
        assert_eq!(double.encode(), b",5.14\r\n");

        let double_negative: RespFrame = RespDouble::new(-5.14).into();
        assert_eq!(double_negative.encode(), b",-5.14\r\n");

        let double_nan: RespFrame = RespDouble::new(f64::NAN).into();
        assert_eq!(double_nan.encode(), b",nan\r\n");

        let double_inf: RespFrame = RespDouble::new(f64::INFINITY).into();
        assert_eq!(double_inf.encode(), b",inf\r\n");

        let double_inf_negative: RespFrame = RespDouble::new(f64::NEG_INFINITY).into();
        assert_eq!(double_inf_negative.encode(), b",-inf\r\n");
    }

    #[test]
    fn test_map_encode() {
        let mut hash_map = HashMap::new();
        hash_map.insert(RespSimpleString::new("foo").into(), 64.into());
        hash_map.insert(RespSimpleString::new("foo").into(), 128.into());
        let map: RespFrame = RespMap::new(hash_map).into();
        assert_eq!(map.encode(), b"%1\r\n+foo\r\n:128\r\n");
    }

    #[test]
    fn test_set_encode() {
        let mut hash_set = HashSet::new();
        hash_set.insert(RespDouble::new(2024.0925).into());
        hash_set.insert(RespDouble::new(2024.0925).into());
        let set: RespFrame = RespSet::new(hash_set).into();
        assert_eq!(set.encode(), b"~1\r\n,2024.0925\r\n");
    }
}
