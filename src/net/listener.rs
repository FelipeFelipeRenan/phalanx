use std::{io, net::SocketAddr};

use mio::net::{TcpListener, TcpStream};

pub struct Listener {
    inner: TcpListener,
}

impl Listener {
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;

        Ok(Self { inner: listener })
    }

    pub fn accept(&mut self) -> io::Result<(TcpStream, SocketAddr)> {
        self.inner.accept()
    }

    pub fn inner_mut(&mut self) -> &mut TcpListener {
        &mut self.inner
    }
}
