use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    thread,
    time::Duration,
};

use phalanx::reactor::Reactor;

#[test]
fn tcp_echo() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let mut reactor = Reactor::bind(addr).unwrap();

    let server_addr = reactor.local_addr().unwrap();

    let shutdown = reactor.shutdown_handle();

    let server_thread = thread::spawn(move || {
        reactor.run().unwrap();
    });

    thread::sleep(Duration::from_millis(50));

    let mut client = TcpStream::connect(server_addr).unwrap();

    client.write_all(b"hello phalanx").unwrap();

    let mut buffer = [0u8; 13];

    client.read_exact(&mut buffer).unwrap();

    assert_eq!(&buffer, b"hello phalanx");

    shutdown.shutdown().unwrap();

    server_thread.join().unwrap();
}
