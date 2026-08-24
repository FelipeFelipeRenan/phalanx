use mio::net::TcpStream;

pub struct Connection {
    pub stream: TcpStream,
    pub write_buffer: Vec<u8>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            write_buffer: Vec::new(),
        }
    }
}
