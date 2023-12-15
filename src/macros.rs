#[macro_export]
#[doc(hidden)]
macro_rules! read_ints {
    ($reader:expr => $($type:ty),*) => {{
        let mut main_buf = [0u8; 0 $(+core::mem::size_of::<$type>())*];
        $reader.read_exact(&mut main_buf)?;
        let mut _pos = 0;
        ($({
            let mut buf = [0u8; core::mem::size_of::<$type>()];
            buf.copy_from_slice(&main_buf[_pos..(_pos + core::mem::size_of::<$type>())]);
            _pos += core::mem::size_of::<$type>();
            <$type>::from_le_bytes(buf)
        }),*)
    }}
}
