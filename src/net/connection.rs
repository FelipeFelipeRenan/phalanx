use std::io::{ErrorKind, Read, Write};

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

    pub fn read(&mut self) -> std::io::Result<Option<usize>> {
        let mut buffer = [0u8; 4096];

        loop {
            match self.stream.read(&mut buffer) {
                Ok(0) => {
                    self.state = ConnectionState::Closing;
                    return Ok(None);
                }

                Ok(bytes_read) => {
                    self.write_buffer.extend_from_slice(&buffer[..bytes_read]);

                    return Ok(Some(bytes_read));
                }

                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    return Ok(Some(0));
                }

                Err(error) => {
                    return Err(error);
                }
            }
        }
    }

    pub fn write(&mut self) -> std::io::Result<usize> {
        let mut total_written = 0;

        while !self.write_buffer.is_empty() {
            match self.stream.write(&self.write_buffer) {
                Ok(0) => {
                    return Ok(total_written);
                }

                Ok(bytes_written) => {
                    self.write_buffer.drain(..bytes_written);
                    total_written += bytes_written;
                }

                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    break;
                }

                Err(error) => {
                    return Err(error);
                }
            }
        }

        Ok(total_written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_stores_data_in_write_buffer() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();

        let address = listener.local_addr().unwrap();

        let mut client = std::net::TcpStream::connect(address).unwrap();

        let (server_stream, _) = listener.accept().unwrap();

        client.write_all(b"hello phalanx").unwrap();

        let mut connection = Connection {
            stream: TcpStream::from_std(server_stream),
            write_buffer: Vec::new(),
            state: ConnectionState::Active,
        };

        let result = connection.read().unwrap();

        assert_eq!(result, Some(13));
        assert_eq!(connection.write_buffer, b"hello phalanx");
    }
}
