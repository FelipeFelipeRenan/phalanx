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

    println!("Creating reactor");

    let mut reactor = Reactor::bind(addr).unwrap();

    let server_addr = reactor.local_addr().unwrap();

    println!("Server listening on {server_addr}");

    let shutdown = reactor.shutdown_handle();

    let server_thread = thread::spawn(move || {
        println!("Starting reactor");

        reactor.run().unwrap();

        println!("Reactor stopped");
    });

    thread::sleep(Duration::from_millis(50));

    println!("Connecting client");

    let mut client = TcpStream::connect(server_addr).unwrap();

    println!("Client connected");

    client.write_all(b"hello phalanx").unwrap();

    println!("Request sent");

    let mut buffer = [0u8; 13];

    println!("Waiting for response");

    client.read_exact(&mut buffer).unwrap();

    println!("Response received");

    assert_eq!(&buffer, b"hello phalanx");

    println!("Shutting down reactor");

    shutdown.shutdown().unwrap();

    println!("Waiting for reactor thread");

    server_thread.join().unwrap();

    println!("Test finished");
}
