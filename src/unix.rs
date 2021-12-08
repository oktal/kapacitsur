use std::future::Future;
use tokio::sync::broadcast;

use crate::Acceptor;
use crate::Result;
use crate::Shutdown;

struct Listener {
    acceptor:  Box<dyn Acceptor>,
    listener: tokio::net::UnixListener,

    shutdown_tx: broadcast::Sender<()>,
}

impl Listener {
    pub async fn run(&mut self) -> Result<()> {
        loop {
            let (stream, addr) = self.listener.accept().await?;
            println!("Accepted connection from {:?}", addr);
            let shutdown_rx = self.shutdown_tx.subscribe();
            let mut agent = self.acceptor.accept(stream, Shutdown::new(shutdown_rx))?;
            tokio::spawn(async move {
                if let Err(e) = agent.run().await { 
                    println!("Agent stopped with error {:?}", e);
                }
            });
        }
    }
}

pub async fn run(listener: tokio::net::UnixListener, acceptor: Box<dyn Acceptor>, shutdown: impl Future) {
    let (shutdown_tx, _) = broadcast::channel(1);

    let mut listener = Listener {
        acceptor,
        listener,

        shutdown_tx
    };

    tokio::select! {
        res = listener.run() => {
            if let Err(e) = res {
                println!("Error: {:?}", e);
            }
        }
        _ = shutdown => {
            println!("Shutting down ...");
        }
    }
}
