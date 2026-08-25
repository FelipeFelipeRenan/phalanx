mod error;
mod net;
mod reactor;

use std::net::SocketAddr;

use error::Result;
use reactor::Reactor;

fn main() -> Result<()> {
    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

    let mut reactor = Reactor::bind(addr)?;

    reactor.run()
}
