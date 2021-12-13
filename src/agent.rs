use std::fmt;
use std::ops::RangeBounds;
use std::vec::{Drain, Vec};

use tokio::net::UnixStream;

use crate::connection::Connection;
use crate::udf::{self, request, response};
use crate::{Handler, PointSender, Result, Shutdown};

#[derive(Debug)]
struct SendError;

impl fmt::Display for SendError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self)
    }
}

impl std::error::Error for SendError {
    fn description(&self) -> &str {
        "channel is full"
    }
}

struct Points(Vec<udf::Point>);

impl Points {
    fn with_capacity(size: usize) -> Points {
        Points(Vec::with_capacity(size))
    }

    fn drain<R>(&mut self, range: R) -> Drain<'_, udf::Point>
    where
        R: RangeBounds<usize>,
    {
        self.0.drain(range)
    }
}

impl PointSender for Points {
    fn send(&mut self, point: udf::Point) -> Result<()> {
        if self.0.len() < self.0.capacity() {
            self.0.push(point);
            Ok(())
        } else {
            Err(SendError.into())
        }
    }
}

pub struct Agent {
    connection: Connection,

    handler: Box<dyn Handler>,

    shutdown: Shutdown,
    points: Points,
}

impl Agent {
    pub fn new(stream: UnixStream, handler: Box<dyn Handler>, shutdown: Shutdown) -> Agent {
        let connection = Connection::new(stream);
        Agent {
            connection,
            handler,

            shutdown,
            points: Points::with_capacity(128),
        }
    }

    fn handle_request(&mut self, request: udf::Request) -> Result<Option<response::Message>> {
        if let Some(message) = request.message {
            match message {
                request::Message::Info(_) => {
                    let info = self.handler.info()?;
                    Ok(Some(response::Message::Info(info)))
                }
                request::Message::Init(r) => {
                    let init = self.handler.init(r)?;
                    Ok(Some(response::Message::Init(init)))
                }
                request::Message::Keepalive(r) => {
                    Ok(Some(response::Message::Keepalive(udf::KeepaliveResponse {
                        time: r.time,
                    })))
                }
                request::Message::Snapshot(r) => {
                    let snapshot = self.handler.snapshot(r)?;
                    Ok(Some(response::Message::Snapshot(snapshot)))
                }
                request::Message::Restore(r) => {
                    let restore = self.handler.restore(r)?;
                    Ok(Some(response::Message::Restore(restore)))
                }
                request::Message::Begin(r) => {
                    self.handler.begin_batch(r)?;
                    Ok(None)
                }
                request::Message::Point(r) => {
                    self.handler.point(r, &mut self.points)?;
                    Ok(None)
                }
                request::Message::End(r) => {
                    self.handler.end_batch(r)?;
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn run(&mut self) -> Result<()> {
        while !self.shutdown.is_shutdown() {
            let response = tokio::select! {
                request = self.connection.read_message() => self.handle_request(request?)?,
                _ = self.shutdown.recv() =>  { None }

            };

            if let Some(response) = response {
                self.connection.send_response(response).await?;
            }

            for point in self.points.drain(..).map(response::Message::Point) {
                self.connection.send_response(point).await?;
            }
        }

        Ok(())
    }
}
