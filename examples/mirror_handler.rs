use std::error::Error;
use std::collections::HashMap;
use std::vec::Vec;

use kapacitsur::agent::Agent;
use kapacitsur::udf;
use kapacitsur::udf::option_value::Value;
use kapacitsur::unix;
use kapacitsur::{Acceptor, Handler, Result, Shutdown};

struct MirrorHandler {
    name: String,
    value: f64,
}

impl Handler for MirrorHandler {
    fn info(&self) -> Result<udf::InfoResponse> {
        Ok(udf::InfoResponse{
            wants: udf::EdgeType::Stream.into(),
            provides: udf::EdgeType::Stream.into(),
            options: HashMap::from([
                ("field".to_string(), udf::OptionInfo{
                    value_types: vec![udf::ValueType::String.into(), udf::ValueType::Double.into()]
                })
            ])
        })
    }

    fn init(&mut self, req: udf::InitRequest) -> Result<udf::InitResponse> {
        let field = req
            .options
            .iter()
            .find(|x| x.name == "field")
            .map(|x| (x.values.get(0), x.values.get(1)));

        Ok(match field {
            None => udf::InitResponse{
                success: false,
                error: "Missing `field`".to_string()
            },
            Some((Some(name), Some(value))) => {
                match (name.value.as_ref(), value.value.as_ref()) {
                    (Some(Value::StringValue(s)), Some(Value::DoubleValue(d))) => {
                        self.name = s.clone();
                        self.value = *d;

                        udf::InitResponse{
                            success: true,
                            error: String::new()
                        }
                    },
                    _ => udf::InitResponse{
                            success: false,
                            error: "Invalid parameter type for `field`".to_string()
                        }
                }
            }
            Some(_) => udf::InitResponse{
                success: false,
                error: "Missing parameter for `field`".to_string()
            }
        })
    }

    fn snapshot(&mut self, _req: udf::SnapshotRequest) -> Result<udf::SnapshotResponse> {
        Ok(udf::SnapshotResponse{snapshot: Vec::new()})
    }

    fn restore(&mut self, _req: udf::RestoreRequest) -> Result<udf::RestoreResponse> {
        Ok(udf::RestoreResponse{success:true, error: String::new()})
    }

    fn begin_batch(&mut self, _req: udf::BeginBatch) -> Result<()> {
        unimplemented!();
    }

    fn point(&mut self, mut point: udf::Point, sender: tokio::sync::mpsc::Sender<udf::Point>) -> Result<()> {
        if let Some(val) = point.fields_double.get_mut(&self.name) {
            *val = self.value;
        } else {
            point.fields_double.insert(self.name.clone(), self.value);
        }

        sender.try_send(point)?;
        Ok(())
    }

    fn end_batch(&mut self, _req: udf::EndBatch) -> Result<()> {
        unimplemented!();
    }
}

struct MirrorAcceptor;

impl Acceptor for MirrorAcceptor {
    fn accept(&self, stream: tokio::net::UnixStream, shutdown: Shutdown) -> Result<Agent> {
        Ok(Agent::new(stream, Box::new(MirrorHandler{ name: String::new(), value: 0.0 }),  shutdown))
    }
}


#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let path: &String = args.get(0).ok_or("Please provide a unix-socket path")?;

    let listener = tokio::net::UnixListener::bind(&path)?;

    println!("Listening on {}", path);
    unix::run(listener, Box::new(MirrorAcceptor), tokio::signal::ctrl_c()).await;

    Ok(())
}
