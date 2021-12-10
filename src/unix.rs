use std::future::Future;
use tokio::sync::{broadcast, mpsc};

use crate::Acceptor;
use crate::Result;
use crate::Shutdown;

struct Listener {
    acceptor: Box<dyn Acceptor>,
    listener: tokio::net::UnixListener,

    shutdown_tx: broadcast::Sender<()>,

    shutdown_tx_complete: mpsc::Sender<()>,
    shutdown_rx_complete: mpsc::Receiver<()>,
}

impl Listener {
    pub async fn run(&mut self) -> Result<()> {
        loop {
            let (stream, addr) = self.listener.accept().await?;
            println!("Accepted connection from {:?}", addr);
            let shutdown_rx = self.shutdown_tx.subscribe();
            let mut agent = self.acceptor.accept(
                stream,
                Shutdown::new(shutdown_rx, self.shutdown_tx_complete.clone()),
            )?;

            tokio::spawn(async move {
                if let Err(e) = agent.run().await {
                    println!("Agent stopped with error {:?}", e);
                }
            });
        }
    }
}

pub async fn run(
    listener: tokio::net::UnixListener,
    acceptor: Box<dyn Acceptor>,
    shutdown: impl Future,
) {
    let (shutdown_tx, _) = broadcast::channel(1);
    let (shutdown_tx_complete, shutdown_rx_complete) = mpsc::channel(1);

    let mut listener = Listener {
        acceptor,
        listener,

        shutdown_tx,
        shutdown_tx_complete,
        shutdown_rx_complete,
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

    let Listener {
        shutdown_tx,
        shutdown_tx_complete,
        mut shutdown_rx_complete,
        ..
    } = listener;

    drop(shutdown_tx);
    drop(shutdown_tx_complete);

    let _ = shutdown_rx_complete.recv().await;
}
