//! This module contains extra methods for [std::collections::HashMap]
//! including a new macro [hash_map] similar to [std::vec].

#[macro_export]
macro_rules! hash_map {
    ($($key:expr => $val:expr),* ,) => (
        $crate::hash_map!($($key => $val),*)
    );
    ($($key:expr => $val:expr),*) => {
        {
            let mut dict = ::std::collections::HashMap::new();
            $( dict.insert($key, $val); )*
            dict
        }
    };
}
