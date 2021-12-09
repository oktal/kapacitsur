use tokio::net::UnixStream;
use tokio::sync::mpsc;

use crate::connection::Connection;
use crate::udf::{self, request, response};
use crate::Handler;
use crate::Shutdown;
use crate::Result;

pub struct Agent {
    connection: Connection,

    handler: Box<dyn Handler>,

    shutdown: Shutdown,

    points_tx: mpsc::Sender<udf::Point>,
    points_rx: mpsc::Receiver<udf::Point>,
}

impl Agent {
    pub fn new(stream: UnixStream, handler: Box<dyn Handler>, shutdown: Shutdown) -> Agent {
        let connection = Connection::new(stream);

        let (points_tx, points_rx) = mpsc::channel(128);

        Agent {
            connection,
            handler,

            shutdown,

            points_tx,
            points_rx
        }
    }

    fn handle_request(&mut self, request: &udf::Request) -> Result<Option<response::Message>> {
        println!("Received {:?}", &request);

        if let Some(message) = &request.message {
            match message {
                request::Message::Info(_) => {
                    let info = self.handler.info()?;
                    Ok(Some(response::Message::Info(info)))
                },
                request::Message::Init(r) => {
                    let init = self.handler.init(r)?;
                    Ok(Some(response::Message::Init(init)))
                },
                request::Message::Keepalive(r) => {
                    Ok(Some(response::Message::Keepalive(
                        udf::KeepaliveResponse{
                            time: r.time
                        }))
                    )
                },
                request::Message::Snapshot(r) => {
                    let snapshot = self.handler.snapshot(r)?;
                    Ok(Some(response::Message::Snapshot(snapshot)))
                },
                request::Message::Restore(r) => {
                    let restore = self.handler.restore(r)?;
                    Ok(Some(response::Message::Restore(restore)))
                },
                request::Message::Begin(r) => {
                    self.handler.begin_batch(r)?;
                    Ok(None)
                },
                request::Message::Point(r) => {
                    self.handler.point(r, self.points_tx.clone())?;
                    Ok(None)
                },
                request::Message::End(r) => {
                    self.handler.end_batch(r)?;
                    Ok(None)
                },
            }
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn run(&mut self) -> Result<()> {
        while !self.shutdown.is_shutdown() {
            let response = tokio::select! {
                request = self.connection.read_message() => self.handle_request(&request?)?,
                point = self.points_rx.recv() => point.map(response::Message::Point),
                _ = self.shutdown.recv() =>  { None }

            };

            if let Some(response) = response {
                self.connection.send_response(&response).await?;
            }
        }

        // Drain the points channel and send any outstanding point before exiting
        while let Ok(response) = self.points_rx.try_recv().map(response::Message::Point) {
            self.connection.send_response(&response).await?;
        }

        Ok(())
    }
}

