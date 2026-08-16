use std::{collections::HashMap, net::SocketAddr};

use mio::{Events, Interest, Poll, Token, net::TcpListener};

const SERVER: Token = Token(0);

fn main() -> std::io::Result<()> {
    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

    let mut listener = TcpListener::bind(addr)?;

    let mut poll = Poll::new()?;

    poll.registry()
        .register(&mut listener, SERVER, Interest::READABLE)?;

    let mut events = Events::with_capacity(1024);
    let mut connections = HashMap::new();

    let mut next_token = 1;

    loop {
        poll.poll(&mut events, None)?;

        for event in events.iter() {
            if event.token() == SERVER {
                match listener.accept() {
                    Ok((mut stream, address)) => {
                        let token = Token(next_token);
                        next_token += 1;

                        println!("Connection from {address} with token {token:?}");

                        poll.registry()
                            .register(&mut stream, token, Interest::READABLE)?;
                        connections.insert(token, stream);
                    }
                    Err(error) => {
                        eprintln!("Accept error: {error}");
                    }
                }
            }
        }
    }
}
