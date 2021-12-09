use tokio::sync::{broadcast, mpsc};

/// A struct to listen to shutdown signals through tokio's `brodcast::Receiver`.
/// This struct also provided a way for the user to know whether the shutdown
/// signal has been sent.
pub struct Shutdown {
    shutdown_rx: broadcast::Receiver<()>,

    is_shutdown: bool,

    _complete: mpsc::Sender<()>,
}

impl Shutdown {
    pub fn new(shutdown_rx: broadcast::Receiver<()>, _complete: mpsc::Sender<()>) -> Shutdown {
        Shutdown {
            shutdown_rx,
            is_shutdown: false,
            _complete
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    pub async fn recv(&mut self) {
        if self.is_shutdown {
            return;
        }

        let _ = self.shutdown_rx.recv().await;

        self.is_shutdown = true;
    }
}
