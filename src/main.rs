use phalanx::reactor::Reactor;

fn main() -> phalanx::error::Result<()> {
    let addr = "127.0.0.1:8080".parse().unwrap();

    let mut reactor = Reactor::bind(addr)?;

    reactor.run()
}
