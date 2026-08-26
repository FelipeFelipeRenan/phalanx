use mio::net::TcpStream;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Active,
    Closing,
}

pub struct Connection {
    pub stream: TcpStream,
    pub write_buffer: Vec<u8>,
    pub state: ConnectionState,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            write_buffer: Vec::new(),
            state: ConnectionState::Active,
        }
    }
}
