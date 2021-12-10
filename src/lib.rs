use tokio::net::UnixStream;

pub mod agent;
mod connection;
pub mod shutdown;
pub mod unix;

pub mod udf {
    include!(concat!(env!("OUT_DIR"), "/agent.rs"));
}

use agent::Agent;
pub use shutdown::Shutdown;

pub type Error = Box<dyn std::error::Error>;
pub type Result<T> = std::result::Result<T, Error>;

pub trait PointSender {
    fn send(&mut self, point: udf::Point) -> Result<()>;
}

pub trait Handler: Send {
    fn info(&self) -> Result<udf::InfoResponse>;

    fn init(&mut self, req: udf::InitRequest) -> Result<udf::InitResponse>;

    fn snapshot(&mut self, req: udf::SnapshotRequest) -> Result<udf::SnapshotResponse>;

    fn restore(&mut self, req: udf::RestoreRequest) -> Result<udf::RestoreResponse>;

    fn begin_batch(&mut self, req: udf::BeginBatch) -> Result<()>;

    fn point(&mut self, point: udf::Point, sender: &mut dyn PointSender) -> Result<()>;

    fn end_batch(&mut self, req: udf::EndBatch) -> Result<()>;
}

pub trait Acceptor {
    fn accept(&self, stream: UnixStream, shutdown: Shutdown) -> Result<Agent>;
}
