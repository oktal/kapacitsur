use std::io;

use bytes::{BufMut, BytesMut};
use integer_encoding::{VarIntAsyncReader, VarInt};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::udf;
use crate::Result;

pub(crate) struct Connection {
    stream: UnixStream,

    encode_buf: BytesMut,
    rx_buf: BytesMut,
}

impl Connection {
    pub(crate) fn new(stream: UnixStream) -> Connection {
        Connection{
            stream,
            encode_buf: BytesMut::with_capacity(128),
            rx_buf: BytesMut::with_capacity(128),
        }
    }

    pub(crate) async fn read_message(&mut self) -> Result<udf::Request> {
        self.stream.readable().await?;

        let size = self.stream.read_varint_async::<u64>().await? as usize;

        self.rx_buf.resize(size, 0u8);
        self.stream.read_exact(&mut self.rx_buf[..size]).await?;
        udf::Request::decode(&mut io::Cursor::new(&self.rx_buf)).map_err(|e| e.into())
    }

    pub(crate) async fn send_response(&mut self, response: &udf::response::Message) -> Result<()> {
        self.encode_buf.clear();

        let mut buf = [0u8; 8];
        let encoded_len = response.encoded_len();
        let size_len = (encoded_len as u64).encode_var(&mut buf);
        self.encode_buf.put(&buf[..size_len]);
        response.encode(&mut self.encode_buf);

        let len = encoded_len + size_len;

        self.stream.write_all(&self.encode_buf[..len]).await?;
        Ok(())
    }
}

