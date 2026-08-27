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

    #[test]
    fn read_returns_zero_when_no_data_is_available() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();

        let address = listener.local_addr().unwrap();

        let _client = std::net::TcpStream::connect(address).unwrap();

        let (server_stream, _) = listener.accept().unwrap();

        server_stream.set_nonblocking(true).unwrap();

        let mut connection = Connection {
            stream: TcpStream::from_std(server_stream),
            write_buffer: Vec::new(),
            state: ConnectionState::Active,
        };

        let result = connection.read().unwrap();

        assert_eq!(result, Some(0));
        assert!(connection.write_buffer.is_empty());
    }

    #[test]
    fn write_sends_buffered_data_to_peer() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();

        let address = listener.local_addr().unwrap();

        let mut client = std::net::TcpStream::connect(address).unwrap();

        let (server_stream, _) = listener.accept().unwrap();

        let mut connection = Connection {
            stream: TcpStream::from_std(server_stream),
            write_buffer: b"hello phalanx".to_vec(),
            state: ConnectionState::Active,
        };

        let bytes_written = connection.write().unwrap();

        assert_eq!(bytes_written, 13);
        assert!(connection.write_buffer.is_empty());

        let mut buffer = [0u8; 13];

        client.read_exact(&mut buffer).unwrap();

        assert_eq!(&buffer, b"hello phalanx");
    }

    #[test]
    fn write_stops_when_socket_would_block() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();

        let address = listener.local_addr().unwrap();

        let client = std::net::TcpStream::connect(address).unwrap();

        let (server_stream, _) = listener.accept().unwrap();

        server_stream.set_nonblocking(true).unwrap();

        let data = vec![b'x'; 16 * 1024 * 1024];

        let mut connection = Connection {
            stream: TcpStream::from_std(server_stream),
            write_buffer: data.clone(),
            state: ConnectionState::Active,
        };

        let bytes_written = connection.write().unwrap();

        assert!(bytes_written > 0);
        assert!(bytes_written < data.len());
        assert_eq!(connection.write_buffer.len(), data.len() - bytes_written);

        drop(client);
    }

    #[test]
    fn read_marks_connection_as_closing_on_eof() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();

        let address = listener.local_addr().unwrap();

        let client = std::net::TcpStream::connect(address).unwrap();

        let (server_stream, _) = listener.accept().unwrap();

        client.shutdown(std::net::Shutdown::Write).unwrap();

        let mut connection = Connection {
            stream: TcpStream::from_std(server_stream),
            write_buffer: Vec::new(),
            state: ConnectionState::Active,
        };

        let result = connection.read().unwrap();

        assert_eq!(result, None);
        assert_eq!(connection.state, ConnectionState::Closing);
    }

    #[test]
    fn closing_connection_can_still_write_buffered_data() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();

        let address = listener.local_addr().unwrap();

        let mut client = std::net::TcpStream::connect(address).unwrap();

        let (server_stream, _) = listener.accept().unwrap();

        let mut connection = Connection {
            stream: TcpStream::from_std(server_stream),
            write_buffer: b"goodbye phalanx".to_vec(),
            state: ConnectionState::Closing,
        };

        let bytes_written = connection.write().unwrap();

        assert_eq!(bytes_written, 15);
        assert!(connection.write_buffer.is_empty());

        let mut buffer = [0u8; 15];

        client.read_exact(&mut buffer).unwrap();

        assert_eq!(&buffer, b"goodbye phalanx");
    }
}
