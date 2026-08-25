use std::{
    collections::HashMap,
    io::{self, ErrorKind, Read, Write},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use mio::{Events, Interest, Poll, Token};

use crate::{
    error::Result,
    net::{connection::Connection, listener::Listener},
};

const SERVER: Token = Token(0);
const WAKER: Token = Token(1);

pub struct Reactor {
    poll: Poll,
    events: Events,
    listener: Listener,
    connections: HashMap<Token, Connection>,
    next_token: usize,
    running: Arc<AtomicBool>,
    waker: Arc<mio::Waker>,
}

#[derive(Clone)]
pub struct ShutdownHandle {
    running: Arc<AtomicBool>,
    waker: Arc<mio::Waker>,
}

impl ShutdownHandle {
    pub fn shutdown(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        self.waker.wake()?;

        Ok(())
    }
}

impl Reactor {
    pub fn bind(addr: SocketAddr) -> Result<Self> {
        let mut listener = Listener::bind(addr)?;

        let poll = Poll::new()?;

        poll.registry()
            .register(listener.inner_mut(), SERVER, Interest::READABLE)?;

        let running = Arc::new(AtomicBool::new(true));

        let waker = Arc::new(mio::Waker::new(poll.registry(), WAKER)?);

        Ok(Self {
            poll,
            events: Events::with_capacity(1024),
            listener,
            connections: HashMap::new(),
            next_token: 2,
            running,
            waker,
        })
    }

    pub fn local_addr(&mut self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn run(&mut self) -> Result<()> {
        while self.running.load(Ordering::Relaxed) {
            self.poll.poll(&mut self.events, None)?;

            let events: Vec<(Token, bool, bool)> = self
                .events
                .iter()
                .map(|event| (event.token(), event.is_readable(), event.is_writable()))
                .collect();

            for (token, readable, writable) in events {
                println!("Event: token={token:?}, readable={readable}, writable={writable}");

                match token {
                    SERVER => {
                        self.accept_connection()?;
                    }

                    WAKER => {
                        println!("Waker event");
                    }

                    _ => {
                        self.handle_connection(token, readable, writable)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn shutdown(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        self.waker.wake()?;

        Ok(())
    }

    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            running: Arc::clone(&self.running),
            waker: Arc::clone(&self.waker),
        }
    }

    fn accept_connection(&mut self) -> Result<()> {
        match self.listener.accept() {
            Ok((mut stream, address)) => {
                let token = Token(self.next_token);
                self.next_token += 1;

                println!("Connection from {address} with token {token:?}");

                self.poll
                    .registry()
                    .register(&mut stream, token, Interest::READABLE)?;

                self.connections.insert(token, Connection::new(stream));
            }

            Err(error) => {
                eprintln!("Accept error: {error}");
            }
        }

        Ok(())
    }

    fn handle_connection(&mut self, token: Token, readable: bool, writable: bool) -> Result<()> {
        let mut should_remove = false;

        if let Some(connection) = self.connections.get_mut(&token) {
            if readable {
                let mut buffer = [0u8; 4096];

                loop {
                    match connection.stream.read(&mut buffer) {
                        Ok(0) => {
                            println!("Connection {token:?} closed");
                            should_remove = true;
                            break;
                        }

                        Ok(bytes_read) => {
                            println!("Read {bytes_read} bytes");

                            connection
                                .write_buffer
                                .extend_from_slice(&buffer[..bytes_read]);
                        }

                        Err(error) if error.kind() == ErrorKind::WouldBlock => {
                            break;
                        }

                        Err(error) => {
                            eprintln!("Read error on {token:?}: {error}");
                            should_remove = true;
                            break;
                        }
                    }
                }
            }

            if writable && !connection.write_buffer.is_empty() {
                loop {
                    match connection.stream.write(&connection.write_buffer) {
                        Ok(0) => {
                            eprintln!("Write returned 0 on {token:?}");
                            should_remove = true;
                            break;
                        }

                        Ok(bytes_written) => {
                            println!("Wrote {bytes_written} bytes");

                            connection.write_buffer.drain(..bytes_written);

                            if connection.write_buffer.is_empty() {
                                break;
                            }
                        }

                        Err(error) if error.kind() == ErrorKind::WouldBlock => {
                            break;
                        }

                        Err(error) => {
                            eprintln!("Write error on {token:?}: {error}");
                            should_remove = true;
                            break;
                        }
                    }
                }
            }

            if !should_remove {
                let interest = if connection.write_buffer.is_empty() {
                    Interest::READABLE
                } else {
                    Interest::READABLE | Interest::WRITABLE
                };

                self.poll
                    .registry()
                    .reregister(&mut connection.stream, token, interest)?;
            }
        }

        if should_remove {
            self.connections.remove(&token);
        }

        Ok(())
    }
}
